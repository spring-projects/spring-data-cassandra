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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.extern.log4j.Log4j;

import org.springframework.data.cassandra.core.entitystore.CassandraEntityManager;
import org.springframework.data.cassandra.core.entitystore.DefaultCassandraEntityManager;
import org.springframework.data.cassandra.core.exception.MappingException;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.TokenRange;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.StringSerializer;

/**
 * Primary implementation of {@link CassandraOperations}.
 * 
 * @author David Webb
 */
@Log4j
public class CassandraTemplate implements CassandraOperations {

	private CassandraFactoryBean cassandraFactory;
	
	/**
	 * Constructor used for a basic template configuration
	 * 
	 * @param cassandraFactory
	 */
	public CassandraTemplate(CassandraFactoryBean cassandra) {
		this.cassandraFactory = cassandra;
	}

	/**
	 * Return the keyspace client
	 * @return
	 */
	public Keyspace getClient() {
		return cassandraFactory.getClient();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#getKeyspaceNames()
	 */
	public Set<String> getKeyspaceNames() {
		return null;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#describeRing()
	 */
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
			KeyspaceDefinition ksd = cassandraFactory.getClient().describeKeyspace();
			name = ksd.getName();
		} catch (ConnectionException e) {
			e.printStackTrace();
		}
		
		return name;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#findById(java.lang.Object, java.lang.Class)
	 */
	public <T> T findById(Object id, Class<T> entityClass, String columnFamilyName) {
		
		ColumnFamily<String, String> CF =
				  new ColumnFamily<String, String>(
					columnFamilyName,              
				    StringSerializer.get(), 
				    StringSerializer.get());
		
		final CassandraEntityManager<T, String> entityManager = 
				new DefaultCassandraEntityManager.Builder<T, String>()
				.withEntityType(entityClass)
				.withKeyspace(cassandraFactory.getClient())
				.withColumnFamily(CF)
				.build();

		T t = null;
		try {
			t = entityManager.get(id.toString());
		} catch (MappingException e) {
			log.error("Caught MappingException trying to lookup type [" + entityClass.getName() + "] in CassandraTemplate.findById().", e);
		}
			
		log.info("t -> " + t);
		
		return t;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#findAll(java.lang.Class)
	 */
	public <T> List<T> findAll(Class<T> entityClass, String columnFamilyName) {
		
		/*
		 * Return var
		 */
		List<T> results = new ArrayList<T>();
		
		ColumnFamily<String, String> CF =
				  new ColumnFamily<String, String>(
					columnFamilyName,              
				    StringSerializer.get(), 
				    StringSerializer.get());	
		
		final CassandraEntityManager<T, String> entityManager = 
				new DefaultCassandraEntityManager.Builder<T, String>()
				.withEntityType(entityClass)
				.withKeyspace(cassandraFactory.getClient())
				.withColumnFamily(CF)
				.build();
		
		try {
			results = entityManager.getAll();
		} catch (MappingException e) {
			log.error("Caught MappingException trying to lookup type [" + entityClass.getName() + "] in CassandraTemplate.findAll().", e);
		}
		
		/*
		 * Return
		 */
		return results;
	
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#insert(java.lang.Object, java.lang.Class, java.lang.String)
	 */
	public <T> void insert(T objectToSave, Class<T> entityClass, String columnFamilyName) {

		ColumnFamily<String, String> CF =
				  new ColumnFamily<String, String>(
					columnFamilyName,              
				    StringSerializer.get(), 
				    StringSerializer.get());	
		
		final CassandraEntityManager<T, String> entityManager = 
				new DefaultCassandraEntityManager.Builder<T, String>()
				.withEntityType(entityClass)
				.withKeyspace(cassandraFactory.getClient())
				.withColumnFamily(CF)
				.build();
		
		try {
			entityManager.put(objectToSave);
		} catch (MappingException e) {
			log.error("Caught MappingException trying to lookup type [" + entityClass.getName() + "] in CassandraTemplate.findAll().", e);
		}
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#insert(java.util.Collection, java.lang.Class, java.lang.String)
	 */
	public <T> void insert(Collection<T> batchToSave, Class<T> entityClass,
			String columnFamilyName) {

		ColumnFamily<String, String> CF =
				  new ColumnFamily<String, String>(
					columnFamilyName,              
				    StringSerializer.get(), 
				    StringSerializer.get());	
		
		final CassandraEntityManager<T, String> entityManager = 
				new DefaultCassandraEntityManager.Builder<T, String>()
				.withEntityType(entityClass)
				.withKeyspace(cassandraFactory.getClient())
				.withColumnFamily(CF)
				.build();
		
		try {
			entityManager.put(batchToSave);
		} catch (MappingException e) {
			log.error("Caught MappingException trying to lookup type [" + entityClass.getName() + "] in CassandraTemplate.insert().", e);
		}
		
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#save(java.lang.Object, java.lang.Class, java.lang.String)
	 */
	public <T> void save(T objectToSave, Class<T> entityClass,
			String columnFamilyName) {

		ColumnFamily<String, String> CF =
				  new ColumnFamily<String, String>(
					columnFamilyName,              
				    StringSerializer.get(), 
				    StringSerializer.get());	
		
		final CassandraEntityManager<T, String> entityManager = 
				new DefaultCassandraEntityManager.Builder<T, String>()
				.withEntityType(entityClass)
				.withKeyspace(cassandraFactory.getClient())
				.withColumnFamily(CF)
				.build();
		
		try {
			entityManager.put(objectToSave);
		} catch (MappingException e) {
			log.error("Caught MappingException trying to lookup type [" + entityClass.getName() + "] in CassandraTemplate.insert().", e);
		}
		
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#save(java.util.Collection, java.lang.Class, java.lang.String)
	 */
	public <T> void save(Collection<T> batchToSave, Class<T> entityClass,
			String columnFamilyName) {


		ColumnFamily<String, String> CF =
				  new ColumnFamily<String, String>(
					columnFamilyName,              
				    StringSerializer.get(), 
				    StringSerializer.get());	
		
		final CassandraEntityManager<T, String> entityManager = 
				new DefaultCassandraEntityManager.Builder<T, String>()
				.withEntityType(entityClass)
				.withKeyspace(cassandraFactory.getClient())
				.withColumnFamily(CF)
				.build();
		
		try {
			entityManager.put(batchToSave);
		} catch (MappingException e) {
			log.error("Caught MappingException trying to lookup type [" + entityClass.getName() + "] in CassandraTemplate.insert().", e);
		}
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#remove(java.lang.Object, java.lang.Class, java.lang.String)
	 */
	public <T> void remove(T objectToRemove, Class<T> entityClass,
			String columnFamilyName) {


		ColumnFamily<String, String> CF =
				  new ColumnFamily<String, String>(
					columnFamilyName,              
				    StringSerializer.get(), 
				    StringSerializer.get());	
		
		final CassandraEntityManager<T, String> entityManager = 
				new DefaultCassandraEntityManager.Builder<T, String>()
				.withEntityType(entityClass)
				.withKeyspace(cassandraFactory.getClient())
				.withColumnFamily(CF)
				.build();
		
		try {
			entityManager.remove(objectToRemove);
		} catch (MappingException e) {
			log.error("Caught MappingException trying to lookup type [" + entityClass.getName() + "] in CassandraTemplate.remove().", e);
		}

	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#remove(java.util.Collection, java.lang.Class, java.lang.String)
	 */
	public <T> void remove(Collection<T> batchToRemove, Class<T> entityClass,
			String columnFamilyName) {


		ColumnFamily<String, String> CF =
				  new ColumnFamily<String, String>(
					columnFamilyName,              
				    StringSerializer.get(), 
				    StringSerializer.get());	
		
		final CassandraEntityManager<T, String> entityManager = 
				new DefaultCassandraEntityManager.Builder<T, String>()
				.withEntityType(entityClass)
				.withKeyspace(cassandraFactory.getClient())
				.withColumnFamily(CF)
				.build();
		
		try {
			entityManager.remove(batchToRemove);
		} catch (MappingException e) {
			log.error("Caught MappingException trying to lookup type [" + entityClass.getName() + "] in CassandraTemplate.remove().", e);
		}

	}


	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#createColumnFamily(java.lang.String)
	 */
	public void createColumnFamily(String columnFamilyName) {
		
		ColumnFamily<String, String> CF =
				  new ColumnFamily<String, String>(
					columnFamilyName,              
				    StringSerializer.get(), 
				    StringSerializer.get());	
		
		try {

			/*
			 * Make this debug.  Definitely required if the create fails.
			 */
			Map<String, List<String>> schemas = cassandraFactory.getClient().describeSchemaVersions();
			for (String a: schemas.keySet()) {
				log.info("Schema:" + a);
				for (String b: schemas.get(a)) {
					log.info(b);
				}
			}
			
			cassandraFactory.getClient().createColumnFamily(CF, null);
		} catch (ConnectionException e) {
			log.error("Caught ConnectionException trying to create new column family.", e);
		}

	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#dropColumnFamily(java.lang.String)
	 */
	public void dropColumnFamily(String columnFamilyName) {

		try {
			cassandraFactory.getClient().dropColumnFamily(columnFamilyName);
		} catch (ConnectionException e) {
			log.error("Caught ConnectionException trying to create new column family.", e);
		}
		
	}

}
