/*
 * Copyright 2014-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.repository.cdi;

import java.lang.annotation.Annotation;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.enterprise.event.Observes;
import javax.enterprise.inject.UnsatisfiedResolutionException;
import javax.enterprise.inject.spi.AfterBeanDiscovery;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.ProcessBean;

import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.repository.cdi.CdiRepositoryBean;
import org.springframework.data.repository.cdi.CdiRepositoryExtensionSupport;

/**
 * A portable CDI extension which registers beans for Spring Data Cassandra repositories.
 *
 * @author Mark Paluch
 */
public class CassandraRepositoryExtension extends CdiRepositoryExtensionSupport {

	private final Map<Set<Annotation>, Bean<CassandraOperations>> cassandraOperationsMap = new HashMap<>();

	/**
	 * Implementation of a an observer which checks for CassandraOperations beans and stores them in
	 * {@link #cassandraOperationsMap} for later association with corresponding repository beans.
	 *
	 * @param <T> The type.
	 * @param processBean The annotated type as defined by CDI.
	 */
	@SuppressWarnings("unchecked")
	<T> void processBean(@Observes ProcessBean<T> processBean) {

		Bean<T> bean = processBean.getBean();
		bean.getTypes().stream() //
				.filter(type -> type instanceof Class<?> && CassandraOperations.class.isAssignableFrom((Class<?>) type)) //
				.forEach(type -> cassandraOperationsMap.put(bean.getQualifiers(), ((Bean<CassandraOperations>) bean)));
	}

	/**
	 * Implementation of a an observer which registers beans to the CDI container for the detected Spring Data
	 * repositories.
	 * <p>
	 * The repository beans are associated to the EntityManagers using their qualifiers.
	 *
	 * @param beanManager The BeanManager instance.
	 */
	void afterBeanDiscovery(@Observes AfterBeanDiscovery afterBeanDiscovery, BeanManager beanManager) {

		for (Map.Entry<Class<?>, Set<Annotation>> entry : getRepositoryTypes()) {

			Class<?> repositoryType = entry.getKey();
			Set<Annotation> qualifiers = entry.getValue();

			CdiRepositoryBean<?> repositoryBean = createRepositoryBean(repositoryType, qualifiers, beanManager);
			afterBeanDiscovery.addBean(repositoryBean);
			registerBean(repositoryBean);
		}
	}

	/**
	 * Creates a {@link Bean}.
	 *
	 * @param <T> The type of the repository.
	 * @param repositoryType The class representing the repository.
	 * @param beanManager The BeanManager instance.
	 * @return The bean.
	 */
	private <T> CdiRepositoryBean<T> createRepositoryBean(Class<T> repositoryType, Set<Annotation> qualifiers,
			BeanManager beanManager) {

		Bean<CassandraOperations> cassandraOperationsBean = Optional.ofNullable(this.cassandraOperationsMap.get(qualifiers))
				.orElseThrow(() -> new UnsatisfiedResolutionException(String.format(
						"Unable to resolve a bean for '%s' with qualifiers %s.", CassandraOperations.class.getName(), qualifiers)));

		return new CassandraRepositoryBean<>(cassandraOperationsBean, qualifiers, repositoryType, beanManager,
				getCustomImplementationDetector());
	}
}
