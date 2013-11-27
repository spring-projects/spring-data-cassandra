/*
 * Copyright 2011 the original author or authors.
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
package org.springframework.data.cassandra.repository.support;

import java.io.Serializable;

import org.springframework.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.CassandraDataOperations;
import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.support.RepositoryFactoryBeanSupport;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.util.Assert;

/**
 * {@link org.springframework.beans.factory.FactoryBean} to create {@link CassandraRepository} instances.
 * 
 * @author Alex Shvid
 * 
 */
public class CassandraRepositoryFactoryBean<T extends Repository<S, ID>, S, ID extends Serializable> extends
		RepositoryFactoryBeanSupport<T, S, ID> {

	private CassandraOperations operations;
	private CassandraDataOperations dataOperations;

	@Override
	protected RepositoryFactorySupport createRepositoryFactory() {
		return new CassandraRepositoryFactory(operations, dataOperations);
	}

	/**
	 * Configures the {@link CassandraOperations} to be used.
	 * 
	 * @param operations the operations to set
	 */
	public void setCassandraOperations(CassandraOperations operations) {
		this.operations = operations;
	}

	/**
	 * Configures the {@link CassandraDataOperations} to be used.
	 * 
	 * @param operations the operations to set
	 */
	public void setCassandraDataOperations(CassandraDataOperations dataOperations) {
		this.dataOperations = dataOperations;
		setMappingContext(dataOperations.getConverter().getMappingContext());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.springframework.data.repository.support.RepositoryFactoryBeanSupport
	 * #afterPropertiesSet()
	 */
	@Override
	public void afterPropertiesSet() {
		super.afterPropertiesSet();
		Assert.notNull(dataOperations, "CassandraDataOperations must not be null!");
	}

}
