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

import lombok.extern.log4j.Log4j;

import org.springframework.dao.DataAccessException;
import org.springframework.data.cassandra.core.bean.RingMember;
import org.springframework.data.cassandra.core.entitystore.CassandraEntityManager;
import org.springframework.data.cassandra.core.entitystore.DefaultCassandraEntityManager;
import org.springframework.data.cassandra.core.exception.MappingException;
import org.springframework.util.Assert;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.TokenRange;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.ddl.SchemaChangeResult;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.StringSerializer;

/**
 * Thrift Protocol implementation of {@link CassandraOperations} using the Astyanax Client.
 * 
 * @author David Webb
 */
@Log4j
public class CassandraThriftTemplate implements CassandraOperations {

	private ThriftExceptionTranslator exceptionTranslator = new ThriftExceptionTranslator();
	private Keyspace keyspace;
	
	/**
	 * Constructor used for a basic template configuration
	 * 
	 * @param cassandraFactory
	 */
	public CassandraThriftTemplate(Keyspace keyspace) {
		this.keyspace = keyspace;
	}

	/**
	 * Return the keyspace client
	 * @return
	 */
	public Keyspace getKeyspace() {
		return keyspace;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#describeRing()
	 */
	public List<RingMember> describeRing() {
	
		/*
		 * Return variable
		 */
		List<RingMember> ring = new ArrayList<RingMember>();

		/*
		 * Get list of token ranges from the Keyspace
		 */
		List<TokenRange> nodes = execute(new KeyspaceCallback<List<TokenRange>>() {

			/* (non-Javadoc)
			 * @see org.springframework.data.cassandra.core.KeyspaceCallback#doInKeyspace(com.netflix.astyanax.Keyspace)
			 */
			public List<TokenRange> doInKeyspace(
					Keyspace ks) throws DataAccessException {

				try {
					return keyspace.describeRing();
				} catch (Exception e) {
					throw potentiallyConvertRuntimeException(e);
				}
			
			}
		});
		
		/*
		 * Convert them to generic beans for future implementations.
		 */
		RingMember member = null;
		for (TokenRange token: nodes) {
			member = new RingMember();
			member.setStartToken(token.getStartToken());
			member.setEndToken(token.getEndToken());
			member.setEndpoints(token.getEndpoints());
			
			ring.add(member);
		}
		
		/*
		 * Return
		 */
		return ring;
		
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#describeKeyspace()
	 */
	public String describeKeyspace() {
		
		return execute(new KeyspaceCallback<String>() {

			public String doInKeyspace(Keyspace ks) throws DataAccessException {
				return ks.getKeyspaceName();
			}
		});

	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#findById(java.lang.Object, java.lang.String)
	 */
	public <T> T findById(final Object id, final Class<T> entityClass, String columnFamilyName) {
		
		T t = execute(columnFamilyName, new ColumnFamilyCallback<T>() {

			public T doInColumnFamily(ColumnFamily<?, ?> cf) throws Exception,
					DataAccessException {

				final CassandraEntityManager<T, String> entityManager = 
						new DefaultCassandraEntityManager.Builder<T, String>()
						.withEntityType(entityClass)
						.withKeyspace(keyspace)
						.withColumnFamily((ColumnFamily<String, String>) cf)
						.build();

				return entityManager.get(id.toString());

			}
		});
		
		log.info("t -> " + t);
		
		return t;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#findAll(java.lang.Class)
	 */
	public <T> List<T> findAll(final Class<T> entityClass, String columnFamilyName) {
		
		/*
		 * Return var
		 */
		List<T> results = execute(columnFamilyName, new ColumnFamilyCallback<List<T>>() {

			public List<T> doInColumnFamily(ColumnFamily<?, ?> cf)
					throws Exception, DataAccessException {

				final CassandraEntityManager<T, String> entityManager = 
						new DefaultCassandraEntityManager.Builder<T, String>()
						.withEntityType(entityClass)
						.withKeyspace(keyspace)
						.withColumnFamily((ColumnFamily<String, String>) cf)
						.build();
				
				return entityManager.getAll();
			
			}
			
		});
		
		/*
		 * Return
		 */
		return results;
	
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#insert(java.lang.Object, java.lang.Class, java.lang.String)
	 */
	public <T> void insert(T objectToSave, final Class<T> entityClass, String columnFamilyName) {

		insertDBObject(columnFamilyName, objectToSave, entityClass);
		
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#insert(java.util.Collection, java.lang.Class, java.lang.String)
	 */
	public <T> void insert(final Collection<T> batchToSave, final Class<T> entityClass, String columnFamilyName) {

		execute(columnFamilyName, new ColumnFamilyCallback<T>() {

			public T doInColumnFamily(ColumnFamily<?, ?> cf) throws Exception,
					DataAccessException {

				final CassandraEntityManager<T, String> entityManager = 
						new DefaultCassandraEntityManager.Builder<T, String>()
						.withEntityType(entityClass)
						.withKeyspace(keyspace)
						.withColumnFamily((ColumnFamily<String, String>) cf)
						.build();
				
				entityManager.put(batchToSave);
				
				return null;

			}
			
		});
		
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#save(java.lang.Object, java.lang.Class, java.lang.String)
	 */
	public <T> void save(final T objectToSave, final Class<T> entityClass, String columnFamilyName) {

		execute(columnFamilyName, new ColumnFamilyCallback<T>() {

			public T doInColumnFamily(ColumnFamily<?, ?> cf) throws Exception,
					DataAccessException {

				final CassandraEntityManager<T, String> entityManager = 
						new DefaultCassandraEntityManager.Builder<T, String>()
						.withEntityType(entityClass)
						.withKeyspace(keyspace)
						.withColumnFamily((ColumnFamily<String, String>) cf)
						.build();

				entityManager.put(objectToSave);
				
				return null;

			}
		});
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#save(java.util.Collection, java.lang.Class, java.lang.String)
	 */
	public <T> void save(final Collection<T> batchToSave, final Class<T> entityClass, String columnFamilyName) {

		execute (columnFamilyName, new ColumnFamilyCallback<T>() {

			public T doInColumnFamily(ColumnFamily<?, ?> cf) throws Exception,
					DataAccessException {

				final CassandraEntityManager<T, String> entityManager = 
						new DefaultCassandraEntityManager.Builder<T, String>()
						.withEntityType(entityClass)
						.withKeyspace(keyspace)
						.withColumnFamily((ColumnFamily<String, String>) cf)
						.build();

				entityManager.put(batchToSave);

				return null;
			}
			
		});
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#remove(java.lang.Object, java.lang.Class, java.lang.String)
	 */
	public <T> void remove(final T objectToRemove, final Class<T> entityClass,
			String columnFamilyName) {

		execute(columnFamilyName, new ColumnFamilyCallback<T>() {

			public T doInColumnFamily(ColumnFamily<?, ?> cf) throws Exception,
					DataAccessException {

				final CassandraEntityManager<T, String> entityManager = 
						new DefaultCassandraEntityManager.Builder<T, String>()
						.withEntityType(entityClass)
						.withKeyspace(keyspace)
						.withColumnFamily((ColumnFamily<String, String>) cf)
						.build();
				
				entityManager.remove(objectToRemove);
				
				return null;
			}
		});
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#remove(java.util.Collection, java.lang.Class, java.lang.String)
	 */
	public <T> void remove(final Collection<T> batchToRemove, final Class<T> entityClass,
			String columnFamilyName) {

		execute(columnFamilyName, new ColumnFamilyCallback<T>() {

			public T doInColumnFamily(ColumnFamily<?, ?> cf) throws Exception,
					DataAccessException {

				final CassandraEntityManager<T, String> entityManager = 
						new DefaultCassandraEntityManager.Builder<T, String>()
						.withEntityType(entityClass)
						.withKeyspace(keyspace)
						.withColumnFamily((ColumnFamily<String, String>) cf)
						.build();
				
				entityManager.remove(batchToRemove);
				
				return null;
			}
		});
		
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#createColumnFamily(java.lang.String)
	 */
	public void createColumnFamily(String columnFamilyName) {

		final ColumnFamily<String, String> CF =
				  new ColumnFamily<String, String>(
					columnFamilyName,              
				    StringSerializer.get(), 
				    StringSerializer.get());	

		String schemaId = execute(new KeyspaceCallback<String>() {

			public String doInKeyspace(Keyspace ks) throws Exception, DataAccessException {

				/*
				 * Make this debug.  Definitely required if the create fails.
				 */
				if (log.isDebugEnabled()) {
					Map<String, List<String>> schemas = keyspace.describeSchemaVersions();
					for (String a: schemas.keySet()) {
						log.debug("Schema:" + a);
						for (String b: schemas.get(a)) {
							log.debug(b);
						}
					}
				}
				
				OperationResult<SchemaChangeResult> result = ks.createColumnFamily(CF, null);
				
				return result.getResult().getSchemaId();
			}
		});
		
		log.info("Created new ColumnFamily -> " + schemaId);

	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#dropColumnFamily(java.lang.String)
	 */
	public void dropColumnFamily(final String columnFamilyName) {

		String schemaId = execute(new KeyspaceCallback<String>() {

			public String doInKeyspace(Keyspace ks) throws Exception, DataAccessException {

				/*
				 * Make this debug.  Definitely required if the create fails.
				 */
				if (log.isDebugEnabled()) {
					Map<String, List<String>> schemas = keyspace.describeSchemaVersions();
					for (String a: schemas.keySet()) {
						log.debug("Schema:" + a);
						for (String b: schemas.get(a)) {
							log.debug(b);
						}
					}
				}
				
				OperationResult<SchemaChangeResult> result = ks.dropColumnFamily(columnFamilyName);
				
				return result.getResult().getSchemaId();
			}
		});
		
		log.info("Dropped ColumnFamily -> " + schemaId);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#findByCQL(java.lang.String, java.lang.Class, java.lang.String)
	 */
	public <T> List<T> findByCQL(final String cql, final Class<T> entityClass,
			String columnFamilyName) {
		
		List<T> results = execute(columnFamilyName, new ColumnFamilyCallback<List<T>>() {

			public List<T> doInColumnFamily(ColumnFamily<?, ?> cf) throws Exception,
					DataAccessException {

				final CassandraEntityManager<T, String> entityManager = 
						new DefaultCassandraEntityManager.Builder<T, String>()
						.withEntityType(entityClass)
						.withKeyspace(keyspace)
						.withColumnFamily((ColumnFamily<String, String>) cf)
						.build();
				
				return entityManager.find(cql);
					
			}
			
		});
		
		log.info("FindByCQL Results Size => " + results.size());
		
		return results;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#remove(java.util.List, java.lang.Class, java.lang.String)
	 */
	public <T> void delete(final List<String> rowKeys, final Class<T> entityClass,
			String columnFamilyName) {
		
		execute(columnFamilyName, new ColumnFamilyCallback<T>() {

			public T doInColumnFamily(ColumnFamily<?, ?> cf) throws Exception,
					DataAccessException {

				final CassandraEntityManager<T, String> entityManager = 
						new DefaultCassandraEntityManager.Builder<T, String>()
						.withEntityType(entityClass)
						.withKeyspace(keyspace)
						.withColumnFamily((ColumnFamily<String, String>) cf)
						.build();
				
				entityManager.delete(rowKeys);
				
				return null;
			}
		});
	}
	
	/**
	 * Exceute a Cassandra command at the Keyspace level
	 * 
	 * @param action
	 * @return
	 */
	protected <T> T execute(KeyspaceCallback<T> action) {

		Assert.notNull(action);

		try {
			Keyspace ks = this.getKeyspace();
			return action.doInKeyspace(ks);
		} catch (Exception e) {
			throw potentiallyConvertRuntimeException(e);
		}
	}
	
	/**
	 * Generic dbInsert that wraps CF Definition, Callback and ExceptionTranslation
	 * 
	 * @param cfName
	 * @param objectToSave
	 * @param entityClass
	 * @return
	 */
	protected <T> Object insertDBObject(final String cfName, final T objectToSave, final Class<?> entityClass) 
	throws DataAccessException {
		return execute(cfName, new ColumnFamilyCallback<T>() {
			
			public T doInColumnFamily(ColumnFamily<?, ?> CF) throws Exception, DataAccessException {
			
				final CassandraEntityManager<T, String> entityManager = 
						new DefaultCassandraEntityManager.Builder<T, String>()
						.withEntityType((Class<T>) entityClass)
						.withKeyspace(keyspace)
						.withColumnFamily((ColumnFamily<String, String>) CF)
						.build();
				
				entityManager.put(objectToSave);

				return objectToSave;
			}
		});
	}

//	public <T> T execute(Class<?> entityClass, CollectionCallback<T> callback) {
//		return execute(determineCollectionName(entityClass), callback);
//	}

	/**
	 * Execute a command at the ColumnFamily Level
	 * 
	 * @param cfName
	 * @param callback
	 * @return
	 */
	protected <T> T execute(String cfName, ColumnFamilyCallback<T> callback) {

		Assert.notNull(cfName);
		Assert.notNull(callback);

		try {
			ColumnFamily<String, String> CF =
					  new ColumnFamily<String, String>(
							  cfName,              
							  StringSerializer.get(), 
							  StringSerializer.get());
			
			return callback.doInColumnFamily(CF);
			
		} catch (Exception e) {
			throw potentiallyConvertRuntimeException(e);
		}
	}
	
	/**
	 * Tries to convert the given {@link RuntimeException} into a {@link DataAccessException} but returns the original
	 * exception if the conversation failed. Thus allows safe rethrowing of the return value.
	 * 
	 * @param ex
	 * @return
	 */
	protected RuntimeException potentiallyConvertRuntimeException(Exception ex) {
		RuntimeException resolved = this.exceptionTranslator.translateExceptionIfPossible(new RuntimeException(ex));
		return resolved == null ? new RuntimeException(ex) : resolved;
	}
}
