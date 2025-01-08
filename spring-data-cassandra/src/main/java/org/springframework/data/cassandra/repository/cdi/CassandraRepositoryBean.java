/*
 * Copyright 2014-2025 the original author or authors.
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

import jakarta.enterprise.context.spi.CreationalContext;
import jakarta.enterprise.inject.spi.Bean;
import jakarta.enterprise.inject.spi.BeanManager;

import java.lang.annotation.Annotation;
import java.util.Optional;
import java.util.Set;

import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.repository.support.CassandraRepositoryFactory;
import org.springframework.data.repository.cdi.CdiRepositoryBean;
import org.springframework.data.repository.config.CustomRepositoryImplementationDetector;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * A bean which represents a Cassandra repository.
 *
 * @author Mark Paluch
 */
public class CassandraRepositoryBean<T> extends CdiRepositoryBean<T> {

	private final Bean<CassandraOperations> cassandraOperationsBean;

	/**
	 * Create a new {@link CassandraRepositoryBean}.
	 *
	 * @param operations must not be {@literal null}.
	 * @param qualifiers must not be {@literal null}.
	 * @param repositoryType must not be {@literal null}.
	 * @param beanManager must not be {@literal null}.
	 * @param detector optional detector for the custom {@link org.springframework.data.repository.Repository}
	 *          implementations {@link CustomRepositoryImplementationDetector}, can be {@literal null}.
	 */
	public CassandraRepositoryBean(Bean<CassandraOperations> operations, Set<Annotation> qualifiers,
			Class<T> repositoryType, BeanManager beanManager, @Nullable CustomRepositoryImplementationDetector detector) {
		super(qualifiers, repositoryType, beanManager, Optional.ofNullable(detector));

		Assert.notNull(operations, "Cannot create repository with 'null' for CassandraOperations");
		this.cassandraOperationsBean = operations;
	}

	@Override
	protected T create(CreationalContext<T> creationalContext, Class<T> repositoryType) {

		CassandraOperations cassandraOperations = getDependencyInstance(cassandraOperationsBean, CassandraOperations.class);

		return create(() -> new CassandraRepositoryFactory(cassandraOperations), repositoryType);
	}

	@Override
	public Class<? extends Annotation> getScope() {
		return cassandraOperationsBean.getScope();
	}
}
