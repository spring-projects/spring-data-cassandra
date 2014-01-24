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

import org.springframework.data.cassandra.core.CassandraTemplate;
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

	private CassandraTemplate cassandraDataTemplate;

	@Override
	protected RepositoryFactorySupport createRepositoryFactory() {
		return new CassandraRepositoryFactory(cassandraDataTemplate);
	}

	/**
	 * Configures the {@link CassandraTemplate} to be used.
	 * 
	 * @param operations the operations to set
	 */
	public void setCassandraDataTemplate(CassandraTemplate cassandraDataTemplate) {
		this.cassandraDataTemplate = cassandraDataTemplate;
		setMappingContext(cassandraDataTemplate.getConverter().getMappingContext());
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
		Assert.notNull(cassandraDataTemplate, "cassandraDataTemplate must not be null!");
	}

}
