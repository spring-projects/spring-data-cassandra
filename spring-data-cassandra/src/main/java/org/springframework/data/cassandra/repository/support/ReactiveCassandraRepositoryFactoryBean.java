/*
 * Copyright 2016-2021 the original author or authors.
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
package org.springframework.data.cassandra.repository.support;

import java.util.Optional;

import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.data.cassandra.core.ReactiveCassandraOperations;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.support.RepositoryFactoryBeanSupport;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.data.repository.query.ReactiveExtensionAwareQueryMethodEvaluationContextProvider;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * {@link org.springframework.beans.factory.FactoryBean} to create
 * {@link org.springframework.data.cassandra.repository.ReactiveCassandraRepository} instances.
 *
 * @author Mark Paluch
 * @since 2.0
 * @see org.springframework.data.repository.reactive.ReactiveSortingRepository
 * @see org.springframework.data.repository.reactive.RxJava2SortingRepository
 */
public class ReactiveCassandraRepositoryFactoryBean<T extends Repository<S, ID>, S, ID>
		extends RepositoryFactoryBeanSupport<T, S, ID> {

	private boolean mappingContextConfigured = false;

	private @Nullable ReactiveCassandraOperations operations;

	/**
	 * Create a new {@link ReactiveCassandraRepositoryFactoryBean} for the given repository interface.
	 *
	 * @param repositoryInterface must not be {@literal null}.
	 */
	public ReactiveCassandraRepositoryFactoryBean(Class<? extends T> repositoryInterface) {
		super(repositoryInterface);
	}

	/**
	 * Configures the {@link ReactiveCassandraOperations} used for Cassandra data access operations.
	 *
	 * @param operations {@link ReactiveCassandraOperations} used to perform CRUD, Query and general data access
	 *          operations on Apache Cassandra.
	 */
	public void setReactiveCassandraOperations(ReactiveCassandraOperations operations) {
		this.operations = operations;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.core.support.RepositoryFactoryBeanSupport#setMappingContext(org.springframework.data.mapping.context.MappingContext)
	 */
	@Override
	protected void setMappingContext(MappingContext<?, ?> mappingContext) {

		super.setMappingContext(mappingContext);

		this.mappingContextConfigured = true;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.core.support.RepositoryFactoryBeanSupport#createRepositoryFactory()
	 */
	@Override
	protected final RepositoryFactorySupport createRepositoryFactory() {

		Assert.state(operations != null, "ReactiveCassandraOperations must not be null");

		return getFactoryInstance(operations);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.core.support.RepositoryFactoryBeanSupport#createDefaultQueryMethodEvaluationContextProvider(ListableBeanFactory)
	 */
	@Override
	protected Optional<QueryMethodEvaluationContextProvider> createDefaultQueryMethodEvaluationContextProvider(
			ListableBeanFactory beanFactory) {
		return Optional.of(new ReactiveExtensionAwareQueryMethodEvaluationContextProvider(beanFactory));
	}

	/**
	 * Creates and initializes a {@link RepositoryFactorySupport} instance.
	 *
	 * @param operations
	 * @return
	 */
	protected RepositoryFactorySupport getFactoryInstance(ReactiveCassandraOperations operations) {
		return new ReactiveCassandraRepositoryFactory(operations);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.core.support.RepositoryFactoryBeanSupport#afterPropertiesSet()
	 */
	@Override
	public void afterPropertiesSet() {

		super.afterPropertiesSet();

		Assert.notNull(operations, "ReactiveCassandraOperations must not be null!");

		if (!mappingContextConfigured) {
			setMappingContext(operations.getConverter().getMappingContext());
		}
	}
}
