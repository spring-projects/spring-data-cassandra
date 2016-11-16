/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.cassandra.repository.config;

import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.core.io.ResourceLoader;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.cassandra.mapping.Table;
import org.springframework.data.cassandra.repository.ReactiveCassandraRepository;
import org.springframework.data.cassandra.repository.support.ReactiveCassandraRepositoryFactoryBean;
import org.springframework.data.repository.config.AnnotationRepositoryConfigurationSource;
import org.springframework.data.repository.config.RepositoryConfiguration;
import org.springframework.data.repository.config.RepositoryConfigurationExtension;
import org.springframework.data.repository.config.RepositoryConfigurationExtensionSupport;
import org.springframework.data.repository.config.RepositoryConfigurationSource;
import org.springframework.data.repository.config.XmlRepositoryConfigurationSource;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

/**
 * {@link RepositoryConfigurationExtension} for Cassandra.
 *
 * @author Mark Paluch
 * @since 2.0
 */
public class ReactiveCassandraRepositoryConfigurationExtension extends RepositoryConfigurationExtensionSupport {

	/**
	 * @inheritDoc
	 */
	@Override
	public String getModuleName() {
		return "Reactive Cassandra";
	}

	/**
	 * @inheritDoc
	 */
	@Override
	protected String getModulePrefix() {
		return "cassandra";
	}

	/**
	 * @inheritDoc
	 */
	@Override
	public String getRepositoryFactoryClassName() {
		return ReactiveCassandraRepositoryFactoryBean.class.getName();
	}

	/**
	 * @inheritDoc
	 */
	@Override
	public void postProcess(BeanDefinitionBuilder builder, XmlRepositoryConfigurationSource config) {}

	/**
	 * @inheritDoc
	 */
	@Override
	public void postProcess(BeanDefinitionBuilder builder, AnnotationRepositoryConfigurationSource config) {

		String reactiveCassandraTemplateRef = config.getAttributes().getString("reactiveCassandraTemplateRef");

		if (StringUtils.hasText(reactiveCassandraTemplateRef)) {
			builder.addPropertyReference("reactiveCassandraOperations", reactiveCassandraTemplateRef);
		}
	}

	/**
	 * @inheritDoc
	 */
	@Override
	protected Collection<Class<? extends Annotation>> getIdentifyingAnnotations() {
		return Collections.singleton(Table.class);
	}

	/**
	 * @inheritDoc
	 */
	@Override
	protected Collection<Class<?>> getIdentifyingTypes() {
		return Collections.singleton(ReactiveCassandraRepository.class);
	}

	/**
	 * @inheritDoc
	 */
	@Override
	public <T extends RepositoryConfigurationSource> Collection<RepositoryConfiguration<T>> getRepositoryConfigurations(
			T configSource, ResourceLoader loader, boolean strictMatchesOnly) {

		Collection<RepositoryConfiguration<T>> repositoryConfigurations =
			super.getRepositoryConfigurations(configSource, loader, strictMatchesOnly);

		return repositoryConfigurations.stream()
			.filter(configuration -> RepositoryType.isReactiveRepository(loadRepositoryInterface(configuration, loader)))
			.collect(Collectors.toList());
	}

	/**
	 * TODO replace with {@link org.springframework.data.repository.config.RepositoryConfigurationExtensionSupport#loadRepositoryInterface(RepositoryConfiguration, ResourceLoader)}
	 * Loads the Repository interface specified in the given {@link RepositoryConfiguration} using
	 * the given {@link ResourceLoader}.
	 *
	 * @param configuration must not be {@literal null}.
	 * @param loader must not be {@literal null}.
	 * @return the Repository interface.
	 * @throws InvalidDataAccessApiUsageException if the Repository interface could not loaded.
	 */
	private Class<?> loadRepositoryInterface(RepositoryConfiguration<?> configuration, ResourceLoader loader) {
		try {
			return ClassUtils.forName(configuration.getRepositoryInterface(), loader.getClassLoader());
		}
		catch (ClassNotFoundException | LinkageError e) {
			throw new InvalidDataAccessApiUsageException(String.format(
				"Could not find Repository type [%s]", configuration.getRepositoryInterface()), e);
		}
	}
}
