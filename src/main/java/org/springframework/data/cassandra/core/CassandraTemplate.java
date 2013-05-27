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

import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.cassandra.core.entitystore.DefaultCassandraEntityManager;
import org.springframework.data.cassandra.core.entitystore.CassandraEntityManager;
import org.springframework.data.cassandra.core.exception.MappingException;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.TokenRange;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
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
	
//	private ApplicationEventPublisher eventPublisher;
//	private ResourceLoader resourceLoader;

	/**
	 * Constructor used for a basic template configuration
	 * 
	 * @param cassandraFactory
	 */
	public CassandraTemplate(CassandraFactoryBean cassandraFactory) {
		this.cassandraFactory = cassandraFactory;
	}

	/**
	 * Returne the keyspace client
	 * @return
	 */
	public Keyspace getClient() {
		return cassandraFactory.getClient();
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
		
		final CassandraEntityManager<T, String> entityManager = 
				new DefaultCassandraEntityManager.Builder<T, String>()
				.withEntityType(entityClass)
				.withKeyspace(cassandraFactory.getClient())
				.withColumnFamily(CF_JOBS)
				.build();

		T t = null;
		try {
			t = entityManager.get(id.toString());
		} catch (MappingException e) {
			LOGGER.error("Caught MappingException trying to lookup type [" + entityClass.getName() + "] in CassandraTemplate.findById.", e);
		}
			
		LOGGER.info("t -> " + t);
		
		return t;
	}
	
}
