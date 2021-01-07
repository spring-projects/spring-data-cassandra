/*
 * Copyright 2013-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.repository.support;

import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.support.RepositoryFactoryBeanSupport;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * {@link org.springframework.beans.factory.FactoryBean} to create {@link CassandraRepository} instances.
 *
 * @author Alex Shvid
 * @author John Blum
 * @author Oliver Gierke
 * @author Mark Paluch
 */
public class CassandraRepositoryFactoryBean<T extends Repository<S, ID>, S, ID>
		extends RepositoryFactoryBeanSupport<T, S, ID> {

	private @Nullable CassandraOperations cassandraOperations;

	/**
	 * Create a new {@link CassandraRepositoryFactoryBean} for the given repository interface.
	 *
	 * @param repositoryInterface must not be {@literal null}.
	 */
	public CassandraRepositoryFactoryBean(Class<? extends T> repositoryInterface) {
		super(repositoryInterface);
	}

	@Override
	protected RepositoryFactorySupport createRepositoryFactory() {

		Assert.state(cassandraOperations != null, "CassandraOperations must not be null");

		return new CassandraRepositoryFactory(cassandraOperations);
	}

	/**
	 * Configures the {@link CassandraTemplate} used for Cassandra data access operations.
	 *
	 * @param cassandraTemplate {@link CassandraTemplate} used to perform CRUD, Query and general data access operations
	 *          on Apache Cassandra.
	 */
	public void setCassandraTemplate(CassandraTemplate cassandraTemplate) {

		this.cassandraOperations = cassandraTemplate;
		setMappingContext(cassandraTemplate.getConverter().getMappingContext());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.core.support.RepositoryFactoryBeanSupport#afterPropertiesSet()
	 */
	@Override
	public void afterPropertiesSet() {

		super.afterPropertiesSet();

		Assert.notNull(cassandraOperations, "CassandraOperations must not be null!");
	}
}
