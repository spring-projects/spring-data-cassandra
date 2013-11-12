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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.mapping.CassandraPersistentProperty;
import org.springframework.data.cassandra.util.CQLUtils;
import org.springframework.data.cassandra.vo.RingMember;
import org.springframework.data.convert.EntityReader;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

/**
 * @author Alex Shvid
 */
public class CassandraTemplate implements CassandraOperations {
	
	private static Logger log = LoggerFactory.getLogger(CassandraTemplate.class);
	
    private static final Collection<String> ITERABLE_CLASSES;
    static {

            Set<String> iterableClasses = new HashSet<String>();
            iterableClasses.add(List.class.getName());
            iterableClasses.add(Collection.class.getName());
            iterableClasses.add(Iterator.class.getName());

            ITERABLE_CLASSES = Collections.unmodifiableCollection(iterableClasses);
    }

    private final Keyspace keyspace;
	private final Session session;
	private final CassandraConverter cassandraConverter;
	private final MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> mappingContext;

	private final PersistenceExceptionTranslator exceptionTranslator = new CassandraExceptionTranslator();
	
	private ClassLoader beanClassLoader;

	/**
	 * Constructor used for a basic template configuration
	 * 
	 * @param keyspace must not be {@literal null}.
	 */
	public CassandraTemplate(Keyspace keyspace) {
		this.keyspace = keyspace;
		this.session = keyspace.getSession();
		this.cassandraConverter = keyspace.getCassandraConverter();
		this.mappingContext = this.cassandraConverter.getMappingContext();
	}
	
    /**
     * @param classLoader
     */
    public void setBeanClassLoader(ClassLoader classLoader) {
    	this.beanClassLoader = classLoader;
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
	
    /**
     * @param obj
     * @return
     */
    private <T> String determineTableName(T obj) {
        if (null != obj) {
                return determineTableName(obj.getClass());
        }

        return null;
}
	
	/**
	 * @param entityClass
	 * @return
	 */
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
     * Insert a row into a Cassandra ColumnFamily
     * 
     * @param tableName
     * @param objectToSave
     * @throws LinkageError 
     * @throws ClassNotFoundException 
     */
    protected <T> T doInsert(final String tableName, final T objectToSave) {

    	try {
    		
			final String entityClassName = objectToSave.getClass().getName();
			final Class<?> entityClass = ClassUtils.forName(entityClassName, this.beanClassLoader);
			final CassandraPersistentEntity<?> entity = mappingContext.getPersistentEntity(entityClass);
			final String useTableName = tableName != null ? tableName : entity.getTable();
	
	    	return execute(new SessionCallback<T>() {
	
	    		public T doInSession(Session s) throws DataAccessException {
					
					Query q = CQLUtils.toInsertQuery(keyspace.getKeyspace(), useTableName, entity, objectToSave);
					log.info(q.toString());
					
					ResultSet rs = s.execute(q);
					
					return null;
					
				}
			});
	    	
    	} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (LinkageError e) {
			e.printStackTrace();
		} finally {}
    	
		return objectToSave;
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

    /* (non-Javadoc)
     * @see org.springframework.data.cassandra.core.CassandraOperations#insert(java.lang.Object)
     */
    public void insert(Object objectToSave) {
            ensureNotIterable(objectToSave);
            insert(objectToSave, determineTableName(objectToSave));
    }

    /* (non-Javadoc)
     * @see org.springframework.data.cassandra.core.CassandraOperations#insert(java.lang.Object, java.lang.String)
     */
    public void insert(Object objectToSave, String tableName) {
            ensureNotIterable(objectToSave);
            doInsert(tableName, objectToSave);
    }

    /**
     * Verify the object is not an iterable type
     * @param o
     */
    protected void ensureNotIterable(Object o) {
            if (null != o) {
                    if (o.getClass().isArray() || ITERABLE_CLASSES.contains(o.getClass().getName())) {
                            throw new IllegalArgumentException("Cannot use a collection here.");
                    }
            }
    }


	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#remove(java.lang.Object)
	 */
	@Override
	public void remove(Object object) {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#remove(java.lang.Object, java.lang.String)
	 */
	@Override
	public void remove(Object object, String tableName) {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#createTable(java.lang.Class)
	 */
	@Override
	public void createTable(Class<?> entityClass) {


    	try {
    		
			final CassandraPersistentEntity<?> entity = mappingContext.getPersistentEntity(entityClass);
			final String useTableName = entity.getTable();
	
	    	createTable(entityClass, useTableName);

		} catch (LinkageError e) {
			e.printStackTrace();
		} finally {}

		
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#createTable(java.lang.Class, java.lang.String)
	 */
	@Override
	public void createTable(Class<?> entityClass, final String tableName) {

    	try {
    		
			final CassandraPersistentEntity<?> entity = mappingContext.getPersistentEntity(entityClass);
	
	    	execute(new SessionCallback<Object>() {
	
	    		public Object doInSession(Session s) throws DataAccessException {
	    			
	    			String cql = CQLUtils.createTable(tableName, entity);
	    			
	    			log.info("CREATE TABLE CQL -> " + cql);
					
					s.execute(cql);
					
					return null;
					
				}
			});
	    	
		} catch (LinkageError e) {
			e.printStackTrace();
		} finally {}
		
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#alterTable(java.lang.Class)
	 */
	@Override
	public void alterTable(Class<?> entityClass) {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#alterTable(java.lang.Class, java.lang.String)
	 */
	@Override
	public void alterTable(Class<?> entityClass, String tableName) {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#dropTable(java.lang.Class)
	 */
	@Override
	public void dropTable(Class<?> entityClass) {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#dropTable(java.lang.String)
	 */
	@Override
	public void dropTable(String tableName) {
		// TODO Auto-generated method stub
		
	}

}
