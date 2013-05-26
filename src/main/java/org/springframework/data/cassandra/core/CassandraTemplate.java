/*
 * Copyright 2013 the original author or authors.
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
package org.springframework.data.cassandra.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty;
import org.springframework.data.mapping.context.MappingContext;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.TokenRange;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.entitystore.DefaultEntityManager;
import com.netflix.astyanax.entitystore.EntityManager;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.serializers.StringSerializer;

/**
 * Primary implementation of {@link CassandraOperations}.
 * 
 * @author David Webb
 */
public class CassandraTemplate implements CassandraOperations {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(CassandraTemplate.class);
	
	private CassandraFactoryBean cassandraFactory;
	private CassandraConverter converter;
	private final MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> mappingContext;
	
//	private ApplicationEventPublisher eventPublisher;
//	private ResourceLoader resourceLoader;

	/**
	 * Constructor used for a basic template configuration
	 * 
	 * @param cassandraFactory
	 */
	public CassandraTemplate(CassandraFactoryBean cassandraFactory) {
		this(cassandraFactory, null);
	}

	/**
	 * Constructor used for a basic template configuration.
	 * 
	 * @param mongoDbFactory
	 * @param mongoConverter
	 */
	public CassandraTemplate(CassandraFactoryBean cassandraFactory,
			CassandraConverter converter) {
		this.cassandraFactory = cassandraFactory;
		this.converter = converter == null ? getDefaultCassandraConverter(cassandraFactory) : converter;
		this.mappingContext = new CassandraMappingContext();
	}

	/**
	 * Returns the default
	 * {@link org.springframework.data.cassandra.core.convert.CassandraConverter}
	 * .
	 * 
	 * @return
	 */
	public CassandraConverter getConverter() {
		return this.converter;
	}
	
	public Keyspace getClient() {
		return cassandraFactory.getClient();
	}

	/**
	 * Returns the default
	 * {@link org.springframework.data.cassandra.core.convert.CassandraConverter}
	 * .
	 * 
	 * @return
	 */
	public CassandraConverter getDefaultCassandraConverter(CassandraFactoryBean cassandraFactory) {
		return this.converter;
	}

	public Set<String> getKeyspaceNames() {
		return null;
	}

	public List<TokenRange> describeRing() {
		
		List<TokenRange> ring = null;
		try {
			ring = cassandraFactory.getClient().describeRing();
		} catch (ConnectionException e) {
			e.printStackTrace();
		} finally {}
		
		return ring;

	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#describeKeyspace()
	 */
	public String describeKeyspace() {
		String name = null;
		
		try {
			name = cassandraFactory.getClient().describeKeyspace().getName();
		} catch (ConnectionException e) {
			e.printStackTrace();
		}
		
		return name;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#findById(java.lang.Object, java.lang.Class)
	 */
	public <T> T findById(Object id, Class<T> entityClass, String columnFamilyName) {
		
		ColumnFamily<String, String> CF_JOBS =
				  new ColumnFamily<String, String>(
					columnFamilyName,              
				    StringSerializer.get(), 
				    StringSerializer.get());
		
		final EntityManager<T, String> entityManager = 
				new DefaultEntityManager.Builder<T, String>()
				.withEntityType(entityClass)
				.withKeyspace(cassandraFactory.getClient())
				.withColumnFamily(CF_JOBS)
				.build();

		T t = entityManager.get(id.toString());
			
		LOGGER.info("t -> " + t);
		
		return t;
	}
	
}
