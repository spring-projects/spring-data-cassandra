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

import org.springframework.cassandra.core.CqlTemplate;
import org.springframework.cassandra.core.QueryOptions;
import org.springframework.cassandra.core.SessionCallback;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.exception.EntityWriterException;
import org.springframework.data.cassandra.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.mapping.DefaultCassandraMappingContext;
import org.springframework.data.cassandra.util.CqlUtils;
import org.springframework.util.Assert;

import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

/**
 * The Cassandra Data Template is a convenience API for all Cassandra Operations using POJOs. For low level Cassandra
 * Operations use the {@link CqlTemplate}
 * 
 * @author Alex Shvid
 * @author David Webb
 * @author Matthew T. Adams
 */
public class CassandraTemplate extends CqlTemplate implements CassandraOperations {

	/*
	 * List of iterable classes when testing POJOs for specific operations.
	 */
	public static final Collection<String> ITERABLE_CLASSES;
	static {

		Set<String> iterableClasses = new HashSet<String>();
		iterableClasses.add(List.class.getName());
		iterableClasses.add(Collection.class.getName());
		iterableClasses.add(Iterator.class.getName());

		ITERABLE_CLASSES = Collections.unmodifiableCollection(iterableClasses);

	}

	/*
	 * Required elements for successful Template Operations.  These can be set with the Constructor, or wired in
	 * later.
	 */
	private CassandraConverter cassandraConverter;
	private CassandraMappingContext mappingContext;

	/**
	 * Default Constructor for wiring in the required components later
	 */
	public CassandraTemplate() {
	}

	/**
	 * Constructor if only session is known at time of Template Creation
	 * 
	 * @param session must not be {@literal null}
	 */
	public CassandraTemplate(Session session) {
		this(session, new MappingCassandraConverter(new DefaultCassandraMappingContext()));
	}

	/**
	 * Constructor if only session and converter are known at time of Template Creation
	 * 
	 * @param session must not be {@literal null}
	 * @param converter must not be {@literal null}.
	 */
	public CassandraTemplate(Session session, CassandraConverter converter) {
		setSession(session);
		this.cassandraConverter = converter;
		this.mappingContext = cassandraConverter.getCassandraMappingContext();
	}

	public CassandraMappingContext getCassandraMappingContext() {
		return mappingContext;
	}

	@Override
	public Long count(Select selectQuery) {
		return doSelectCount(selectQuery);
	}

	@Override
	public Long count(String tableName) {
		Select select = QueryBuilder.select().countAll().from(tableName);
		return doSelectCount(select);
	}

	@Override
	public <T> void delete(List<T> entities) {
		String tableName = getTableName(entities.get(0).getClass());
		Assert.notNull(tableName);
		delete(entities, tableName);
	}

	@Override
	public <T> void delete(List<T> entities, QueryOptions options) {
		String tableName = getTableName(entities.get(0).getClass());
		Assert.notNull(tableName);
		delete(entities, tableName, options);
	}

	@Override
	public <T> void delete(List<T> entities, String tableName) {

		delete(entities, tableName, null);
	}

	@Override
	public <T> void delete(List<T> entities, String tableName, QueryOptions options) {
		Assert.notNull(entities);
		Assert.notEmpty(entities);
		Assert.notNull(tableName);
		doBatchDelete(tableName, entities, options, false);
	}

	@Override
	public <T> void delete(T entity) {
		String tableName = getTableName(entity.getClass());
		Assert.notNull(tableName);
		delete(entity, tableName);
	}

	@Override
	public <T> void delete(T entity, QueryOptions options) {
		String tableName = getTableName(entity.getClass());
		Assert.notNull(tableName);
		delete(entity, tableName, options);
	}

	@Override
	public <T> void delete(T entity, String tableName) {
		delete(entity, tableName, null);
	}

	@Override
	public <T> void delete(T entity, String tableName, QueryOptions options) {
		Assert.notNull(entity);
		Assert.notNull(tableName);
		doDelete(tableName, entity, options, false);
	}

	@Override
	public <T> void deleteAsynchronously(List<T> entities) {
		String tableName = getTableName(entities.get(0).getClass());
		Assert.notNull(tableName);
		deleteAsynchronously(entities, tableName);
	}

	@Override
	public <T> void deleteAsynchronously(List<T> entities, QueryOptions options) {
		String tableName = getTableName(entities.get(0).getClass());
		Assert.notNull(tableName);
		deleteAsynchronously(entities, tableName, options);
	}

	@Override
	public <T> void deleteAsynchronously(List<T> entities, String tableName) {
		deleteAsynchronously(entities, tableName, null);
	}

	@Override
	public <T> void deleteAsynchronously(List<T> entities, String tableName, QueryOptions options) {
		Assert.notNull(entities);
		Assert.notEmpty(entities);
		Assert.notNull(tableName);
		doBatchDelete(tableName, entities, options, true);
	}

	@Override
	public <T> void deleteAsynchronously(T entity) {
		String tableName = getTableName(entity.getClass());
		Assert.notNull(tableName);
		deleteAsynchronously(entity, tableName);
	}

	@Override
	public <T> void deleteAsynchronously(T entity, QueryOptions options) {
		String tableName = getTableName(entity.getClass());
		Assert.notNull(tableName);
		deleteAsynchronously(entity, tableName, options);
	}

	@Override
	public <T> void deleteAsynchronously(T entity, String tableName) {
		deleteAsynchronously(entity, tableName, null);
	}

	@Override
	public <T> void deleteAsynchronously(T entity, String tableName, QueryOptions options) {
		Assert.notNull(entity);
		Assert.notNull(tableName);
		doDelete(tableName, entity, options, true);
	}

	/**
	 * @param entityClass
	 * @return
	 */
	public String determineTableName(Class<?> entityClass) {

		if (entityClass == null) {
			throw new InvalidDataAccessApiUsageException(
					"No class parameter provided, entity table name can't be determined!");
		}

		CassandraPersistentEntity<?> entity = mappingContext.getPersistentEntity(entityClass);
		if (entity == null) {
			throw new InvalidDataAccessApiUsageException("No Persitent Entity information found for the class "
					+ entityClass.getName());
		}
		return entity.getTableName();
	}

	@Override
	public CassandraConverter getConverter() {
		return cassandraConverter;
	}

	@Override
	public String getTableName(Class<?> entityClass) {
		return determineTableName(entityClass);
	}

	@Override
	public <T> List<T> insert(List<T> entities) {
		String tableName = getTableName(entities.get(0).getClass());
		Assert.notNull(tableName);
		return insert(entities, tableName);
	}

	@Override
	public <T> List<T> insert(List<T> entities, QueryOptions options) {
		String tableName = getTableName(entities.get(0).getClass());
		Assert.notNull(tableName);
		return insert(entities, tableName, options);
	}

	@Override
	public <T> List<T> insert(List<T> entities, String tableName) {
		return insert(entities, tableName, null);
	}

	@Override
	public <T> List<T> insert(List<T> entities, String tableName, QueryOptions options) {
		Assert.notNull(entities);
		Assert.notEmpty(entities);
		Assert.notNull(tableName);
		return doBatchInsert(tableName, entities, options, false);
	}

	@Override
	public <T> T insert(T entity) {
		String tableName = determineTableName(entity);
		Assert.notNull(tableName);
		return insert(entity, tableName);
	}

	@Override
	public <T> T insert(T entity, QueryOptions options) {
		String tableName = determineTableName(entity);
		Assert.notNull(tableName);
		return insert(entity, tableName, options);
	}

	@Override
	public <T> T insert(T entity, String tableName) {
		return insert(entity, tableName, null);
	}

	@Override
	public <T> T insert(T entity, String tableName, QueryOptions options) {
		Assert.notNull(entity);
		Assert.notNull(tableName);
		ensureNotIterable(entity);
		return doInsert(tableName, entity, options, false);
	}

	@Override
	public <T> List<T> insertAsynchronously(List<T> entities) {
		String tableName = getTableName(entities.get(0).getClass());
		Assert.notNull(tableName);
		return insertAsynchronously(entities, tableName);
	}

	@Override
	public <T> List<T> insertAsynchronously(List<T> entities, QueryOptions options) {
		String tableName = getTableName(entities.get(0).getClass());
		Assert.notNull(tableName);
		return insertAsynchronously(entities, tableName, options);
	}

	@Override
	public <T> List<T> insertAsynchronously(List<T> entities, String tableName) {
		return insertAsynchronously(entities, tableName, null);
	}

	@Override
	public <T> List<T> insertAsynchronously(List<T> entities, String tableName, QueryOptions options) {
		Assert.notNull(entities);
		Assert.notEmpty(entities);
		Assert.notNull(tableName);
		return doBatchInsert(tableName, entities, options, true);
	}

	@Override
	public <T> T insertAsynchronously(T entity) {
		String tableName = determineTableName(entity);
		Assert.notNull(tableName);
		return insertAsynchronously(entity, tableName);
	}

	@Override
	public <T> T insertAsynchronously(T entity, QueryOptions options) {
		String tableName = determineTableName(entity);
		Assert.notNull(tableName);
		return insertAsynchronously(entity, tableName, options);
	}

	@Override
	public <T> T insertAsynchronously(T entity, String tableName) {
		return insertAsynchronously(entity, tableName, null);
	}

	@Override
	public <T> T insertAsynchronously(T entity, String tableName, QueryOptions options) {
		Assert.notNull(entity);
		Assert.notNull(tableName);

		ensureNotIterable(entity);

		return doInsert(tableName, entity, options, true);
	}

	@Override
	public <T> List<T> select(Select cql, Class<T> selectClass) {
		return select(cql.getQueryString(), selectClass);
	}

	@Override
	public <T> List<T> select(String cql, Class<T> selectClass) {
		return doSelect(cql, new ReadRowCallback<T>(cassandraConverter, selectClass));
	}

	@Override
	public <T> T selectOne(Select selectQuery, Class<T> selectClass) {
		return selectOne(selectQuery.getQueryString(), selectClass);
	}

	@Override
	public <T> T selectOne(String cql, Class<T> selectClass) {
		return doSelectOne(cql, new ReadRowCallback<T>(cassandraConverter, selectClass));
	}

	@Override
	public <T> List<T> update(List<T> entities) {
		String tableName = getTableName(entities.get(0).getClass());
		Assert.notNull(tableName);
		return update(entities, tableName);
	}

	@Override
	public <T> List<T> update(List<T> entities, QueryOptions options) {
		String tableName = getTableName(entities.get(0).getClass());
		Assert.notNull(tableName);
		return update(entities, tableName, options);
	}

	@Override
	public <T> List<T> update(List<T> entities, String tableName) {
		return update(entities, tableName, null);
	}

	@Override
	public <T> List<T> update(List<T> entities, String tableName, QueryOptions options) {
		Assert.notNull(entities);
		Assert.notEmpty(entities);
		Assert.notNull(tableName);
		return doBatchUpdate(tableName, entities, options, false);
	}

	@Override
	public <T> T update(T entity) {
		String tableName = getTableName(entity.getClass());
		Assert.notNull(tableName);
		return update(entity, tableName);
	}

	@Override
	public <T> T update(T entity, QueryOptions options) {
		String tableName = getTableName(entity.getClass());
		Assert.notNull(tableName);
		return update(entity, tableName, options);
	}

	@Override
	public <T> T update(T entity, String tableName) {
		return update(entity, tableName, null);
	}

	@Override
	public <T> T update(T entity, String tableName, QueryOptions options) {
		Assert.notNull(entity);
		Assert.notNull(tableName);
		return doUpdate(tableName, entity, options, false);
	}

	@Override
	public <T> List<T> updateAsynchronously(List<T> entities) {
		String tableName = getTableName(entities.get(0).getClass());
		Assert.notNull(tableName);
		return updateAsynchronously(entities, tableName);
	}

	@Override
	public <T> List<T> updateAsynchronously(List<T> entities, QueryOptions options) {
		String tableName = getTableName(entities.get(0).getClass());
		Assert.notNull(tableName);
		return updateAsynchronously(entities, tableName, options);
	}

	@Override
	public <T> List<T> updateAsynchronously(List<T> entities, String tableName) {
		return updateAsynchronously(entities, tableName, null);
	}

	@Override
	public <T> List<T> updateAsynchronously(List<T> entities, String tableName, QueryOptions options) {
		Assert.notNull(entities);
		Assert.notEmpty(entities);
		Assert.notNull(tableName);
		return doBatchUpdate(tableName, entities, options, true);
	}

	@Override
	public <T> T updateAsynchronously(T entity) {
		String tableName = getTableName(entity.getClass());
		Assert.notNull(tableName);
		return updateAsynchronously(entity, tableName);
	}

	@Override
	public <T> T updateAsynchronously(T entity, QueryOptions options) {
		String tableName = getTableName(entity.getClass());
		Assert.notNull(tableName);
		return updateAsynchronously(entity, tableName, options);
	}

	@Override
	public <T> T updateAsynchronously(T entity, String tableName) {

		return updateAsynchronously(entity, tableName, null);
	}

	@Override
	public <T> T updateAsynchronously(T entity, String tableName, QueryOptions options) {
		Assert.notNull(entity);
		Assert.notNull(tableName);
		return doUpdate(tableName, entity, options, true);
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
	 * @param query
	 * @param readRowCallback
	 * @return
	 */
	private <T> List<T> doSelect(final String query, ReadRowCallback<T> readRowCallback) {

		ResultSet resultSet = doExecute(new SessionCallback<ResultSet>() {

			@Override
			public ResultSet doInSession(Session s) throws DataAccessException {
				return s.execute(query);
			}
		});

		if (resultSet == null) {
			return null;
		}

		List<T> result = new ArrayList<T>();
		Iterator<Row> iterator = resultSet.iterator();
		while (iterator.hasNext()) {
			Row row = iterator.next();
			result.add(readRowCallback.doWith(row));
		}

		return result;
	}

	/**
	 * @param selectQuery
	 * @return
	 */
	private Long doSelectCount(final Select query) {

		Long count = null;

		ResultSet resultSet = doExecute(new SessionCallback<ResultSet>() {

			@Override
			public ResultSet doInSession(Session s) throws DataAccessException {
				return s.execute(query);
			}
		});

		if (resultSet == null) {
			return null;
		}

		Iterator<Row> iterator = resultSet.iterator();
		while (iterator.hasNext()) {
			Row row = iterator.next();
			count = row.getLong(0);
		}

		return count;

	}

	/**
	 * @param query
	 * @param readRowCallback
	 * @return
	 */
	private <T> T doSelectOne(final String query, ReadRowCallback<T> readRowCallback) {

		logger.info(query);

		/*
		 * Run the Query
		 */
		ResultSet resultSet = doExecute(new SessionCallback<ResultSet>() {

			@Override
			public ResultSet doInSession(Session s) throws DataAccessException {
				return s.execute(query);
			}
		});

		if (resultSet == null) {
			return null;
		}

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
	}

	/**
	 * Perform the deletion on a list of objects
	 * 
	 * @param tableName
	 * @param objectToRemove
	 */
	protected <T> void doBatchDelete(final String tableName, final List<T> entities, final QueryOptions options,
			final boolean deleteAsynchronously) {

		Assert.notEmpty(entities);

		try {

			final Batch b = CqlUtils.toDeleteBatchQuery(tableName, entities, options, cassandraConverter);
			logger.info(b.toString());

			doExecute(new SessionCallback<Object>() {

				@Override
				public Object doInSession(Session s) throws DataAccessException {

					if (deleteAsynchronously) {
						s.executeAsync(b);
					} else {
						s.execute(b);
					}

					return null;

				}
			});

		} catch (EntityWriterException e) {
			throw getExceptionTranslator().translateExceptionIfPossible(
					new RuntimeException("Failed to translate Object to Query", e));
		}
	}

	/**
	 * Insert a row into a Cassandra CQL Table
	 * 
	 * @param tableName
	 * @param entities
	 * @param optionsByName
	 * @param insertAsychronously
	 * @return
	 */
	protected <T> List<T> doBatchInsert(final String tableName, final List<T> entities, final QueryOptions options,
			final boolean insertAsychronously) {

		Assert.notEmpty(entities);

		try {

			final Batch b = CqlUtils.toInsertBatchQuery(tableName, entities, options, cassandraConverter);
			logger.info(b.getQueryString());

			return doExecute(new SessionCallback<List<T>>() {

				@Override
				public List<T> doInSession(Session s) throws DataAccessException {

					if (insertAsychronously) {
						s.executeAsync(b);
					} else {
						s.execute(b);
					}

					return entities;

				}
			});

		} catch (EntityWriterException e) {
			throw getExceptionTranslator().translateExceptionIfPossible(
					new RuntimeException("Failed to translate Object to Query", e));
		}
	}

	/**
	 * Update a Batch of rows in a Cassandra CQL Table
	 * 
	 * @param tableName
	 * @param entities
	 * @param optionsByName
	 * @param updateAsychronously
	 * @return
	 */
	protected <T> List<T> doBatchUpdate(final String tableName, final List<T> entities, final QueryOptions options,
			final boolean updateAsychronously) {

		Assert.notEmpty(entities);

		try {

			final Batch b = CqlUtils.toUpdateBatchQuery(tableName, entities, options, cassandraConverter);
			logger.info(b.toString());

			return doExecute(new SessionCallback<List<T>>() {

				@Override
				public List<T> doInSession(Session s) throws DataAccessException {

					if (updateAsychronously) {
						s.executeAsync(b);
					} else {
						s.execute(b);
					}

					return entities;

				}
			});

		} catch (EntityWriterException e) {
			throw getExceptionTranslator().translateExceptionIfPossible(
					new RuntimeException("Failed to translate Object to Query", e));
		}
	}

	/**
	 * Perform the removal of a Row.
	 * 
	 * @param tableName
	 * @param objectToRemove
	 */
	protected <T> void doDelete(final String tableName, final T objectToRemove, final QueryOptions options,
			final boolean deleteAsynchronously) {

		try {

			final Query q = CqlUtils.toDeleteQuery(tableName, objectToRemove, options, cassandraConverter);
			logger.info(q.toString());

			doExecute(new SessionCallback<Object>() {

				@Override
				public Object doInSession(Session s) throws DataAccessException {

					if (deleteAsynchronously) {
						s.executeAsync(q);
					} else {
						s.execute(q);
					}

					return null;

				}
			});

		} catch (EntityWriterException e) {
			throw getExceptionTranslator().translateExceptionIfPossible(
					new RuntimeException("Failed to translate Object to Query", e));
		}
	}

	/**
	 * Execute a command at the Session Level
	 * 
	 * @param callback
	 * @return
	 */
	@Override
	protected <T> T doExecute(SessionCallback<T> callback) {

		Assert.notNull(callback);

		try {

			return callback.doInSession(getSession());

		} catch (DataAccessException e) {
			throw translateExceptionIfPossible(e);
		}
	}

	/**
	 * Insert a row into a Cassandra CQL Table
	 * 
	 * @param tableName
	 * @param entity
	 */
	protected <T> T doInsert(final String tableName, final T entity, final QueryOptions options,
			final boolean insertAsychronously) {

		try {

			final Query q = CqlUtils.toInsertQuery(tableName, entity, options, cassandraConverter);

			logger.info(q.toString());
			if (q.getConsistencyLevel() != null) {
				logger.info(q.getConsistencyLevel().name());
			}
			if (q.getRetryPolicy() != null) {
				logger.info(q.getRetryPolicy().toString());
			}

			return doExecute(new SessionCallback<T>() {

				@Override
				public T doInSession(Session s) throws DataAccessException {

					if (insertAsychronously) {
						s.executeAsync(q);
					} else {
						s.execute(q);
					}

					return entity;

				}
			});

		} catch (EntityWriterException e) {
			throw getExceptionTranslator().translateExceptionIfPossible(
					new RuntimeException("Failed to translate Object to Query", e));
		}

	}

	/**
	 * Update a row into a Cassandra CQL Table
	 * 
	 * @param tableName
	 * @param entity
	 * @param optionsByName
	 * @param updateAsychronously
	 * @return
	 */
	protected <T> T doUpdate(final String tableName, final T entity, final QueryOptions options,
			final boolean updateAsychronously) {

		try {

			final Query q = CqlUtils.toUpdateQuery(tableName, entity, options, cassandraConverter);
			logger.info(q.toString());

			return doExecute(new SessionCallback<T>() {

				@Override
				public T doInSession(Session s) throws DataAccessException {

					if (updateAsychronously) {
						s.executeAsync(q);
					} else {
						s.execute(q);
					}

					return entity;

				}
			});

		} catch (EntityWriterException e) {
			throw getExceptionTranslator().translateExceptionIfPossible(
					new RuntimeException("Failed to translate Object to Query", e));
		}

	}

	/**
	 * Verify the object is not an iterable type
	 * 
	 * @param o
	 */
	protected void ensureNotIterable(Object o) {
		if (null != o) {
			if (o.getClass().isArray() || ITERABLE_CLASSES.contains(o.getClass().getName())) {
				throw new IllegalArgumentException("Cannot use a collection here.");
			}
		}
	}
}
