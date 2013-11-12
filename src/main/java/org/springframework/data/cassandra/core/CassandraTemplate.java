/*
 * Copyright 2011-2013 the original author or authors.
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
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.mapping.CassandraPersistentProperty;
import org.springframework.data.cassandra.vo.RingMember;
import org.springframework.data.convert.EntityReader;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.util.Assert;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

/**
 * @author Alex Shvid
 */
public class CassandraTemplate implements CassandraOperations {

	private final Session session;
	private final CassandraConverter cassandraConverter;
	private final MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> mappingContext;

	private final PersistenceExceptionTranslator exceptionTranslator = new CassandraExceptionTranslator();

	/**
	 * Constructor used for a basic template configuration
	 * 
	 * @param keyspace must not be {@literal null}.
	 */
	public CassandraTemplate(Keyspace keyspace) {
		this.session = keyspace.getSession();
		this.cassandraConverter = keyspace.getCassandraConverter();
		this.mappingContext = this.cassandraConverter.getMappingContext();
	}
	
	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#describeRing()
	 */
	@Override
	public List<RingMember> describeRing() {

		/*
		 * Initialize the return variable
		 */
		List<RingMember> ring = new ArrayList<RingMember>();
		
		/*
		 * Get the cluster metadata for this session
		 */
		Metadata clusterMetadata = execute(new SessionCallback<Metadata>() {

			@Override
			public Metadata doInSession(Session s) throws DataAccessException {
				return s.getCluster().getMetadata();
			}
			
		});
		
		/*
		 * Get all hosts in the cluster
		 */
		Set<Host> hosts = clusterMetadata.getAllHosts();
		
		/*
		 * Loop variables
		 */
		RingMember member = null;
		
		/*
		 * Populate Ring with Host Metadata
		 */
		for (Host h: hosts) {
			
			member = new RingMember(h);
			ring.add(member);
		}
		
		/*
		 * Return
		 */
		return ring;
	
	}

	
	public String getTableName(Class<?> entityClass) {
		return determineTableName(entityClass);
	}
	
	public ResultSet executeQuery(String query) {
		try {
			return session.execute(query);
		} catch (NoHostAvailableException e) {
			throw new CassandraConnectionFailureException("no host available", e);
		} catch (RuntimeException e) {
			throw potentiallyConvertRuntimeException(e);
		}
	}

	public <T> List<T> select(String query, Class<T> selectClass) {
		return selectInternal(query, new ReadRowCallback<T>(cassandraConverter, selectClass));
	}
	
	public <T> T selectOne(String query, Class<T> selectClass) {
		return selectOneInternal(query, new ReadRowCallback<T>(cassandraConverter, selectClass));
	}
	
	
	public void insert(Object entity) {
		// TODO Auto-generated method stub
		
	}

	public void insert(Object entity, String tableName) {
		// TODO Auto-generated method stub
		
	}

	public void remove(Object object) {
		// TODO Auto-generated method stub
		
	}

	public void remove(Object object, String tableName) {
		// TODO Auto-generated method stub
		
	}

	public void createTable(Class<?> entityClass) {
		// TODO Auto-generated method stub
		
	}

	public void createTable(Class<?> entityClass, String tableName) {
		// TODO Auto-generated method stub
		
	}

	public void alterTable(Class<?> entityClass) {
		// TODO Auto-generated method stub
		
	}

	public void alterTable(Class<?> entityClass, String tableName) {
		// TODO Auto-generated method stub
		
	}

	public void dropTable(Class<?> entityClass) {
		// TODO Auto-generated method stub
		
	}

	public void dropTable(String tableName) {
		// TODO Auto-generated method stub
		
	}

	public CassandraConverter getConverter() {
		return cassandraConverter;
	}
	
	/**
	 * Simple internal callback to allow operations on a {@link Row}.
	 * 
	 * @author Alex Shvid
	 */

	private interface RowCallback<T> {

		T doWith(Row object);
	}
	
	/**
	 * Simple {@link RowCallback} that will transform {@link Row} into the given target type using the given
	 * {@link EntityReader}.
	 * 
	 * @author Alex Shvid
	 */
	private static class ReadRowCallback<T> implements RowCallback<T> {

		private final EntityReader<? super T, Row> reader;
		private final Class<T> type;

		public ReadRowCallback(EntityReader<? super T, Row> reader, Class<T> type) {
			Assert.notNull(reader);
			Assert.notNull(type);
			this.reader = reader;
			this.type = type;
		}

		public T doWith(Row object) {
			T source = reader.read(type, object);
			return source;
		}
	}	
	
	<T> List<T> selectInternal(String query, ReadRowCallback<T> readRowCallback) {
		try {
			ResultSet resultSet = session.execute(query);
			List<T> result = new ArrayList<T>();
			Iterator<Row> iterator = resultSet.iterator();
			while(iterator.hasNext()) {
				Row row = iterator.next();
				result.add(readRowCallback.doWith(row));
			}
			return result;
		} catch (NoHostAvailableException e) {
			throw new CassandraConnectionFailureException("no host available", e);
		} catch (RuntimeException e) {
			throw potentiallyConvertRuntimeException(e);
		}
	}
	
	<T> T selectOneInternal(String query, ReadRowCallback<T> readRowCallback) {
		try {
			ResultSet resultSet = session.execute(query);
			Iterator<Row> iterator = resultSet.iterator();
			if (iterator.hasNext()) {
				Row row = iterator.next();
				T result = readRowCallback.doWith(row);
				if (iterator.hasNext()) {
					throw new DuplicateKeyException("found two or more results in query " + query);
				}
				return result;
			}
			return null;
		} catch (NoHostAvailableException e) {
			throw new CassandraConnectionFailureException("no host available", e);
		} catch (RuntimeException e) {
			throw potentiallyConvertRuntimeException(e);
		}
	}
	
	String determineTableName(Class<?> entityClass) {

		if (entityClass == null) {
			throw new InvalidDataAccessApiUsageException(
					"No class parameter provided, entity table name can't be determined!");
		}

		CassandraPersistentEntity<?> entity = mappingContext.getPersistentEntity(entityClass);
		if (entity == null) {
			throw new InvalidDataAccessApiUsageException("No Persitent Entity information found for the class "
					+ entityClass.getName());
		}
		return entity.getTable();
	}
	
	private RuntimeException potentiallyConvertRuntimeException(
			RuntimeException ex) {
		RuntimeException resolved = this.exceptionTranslator.translateExceptionIfPossible(ex);
		return resolved == null ? ex : resolved;
	}

	
	/**
	 * Execute a command at the Session Level
	 * 
	 * @param callback
	 * @return
	 */
	protected <T> T execute(SessionCallback<T> callback) {
		
		Assert.notNull(callback);

		try {
			
			return callback.doInSession(session);
			
		} catch (DataAccessException e) {
			throw potentiallyConvertRuntimeException(e);
		} 
	}

}
