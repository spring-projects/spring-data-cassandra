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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.springframework.cassandra.core.CqlTemplate;
import org.springframework.cassandra.core.QueryOptions;
import org.springframework.cassandra.core.SessionCallback;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.mapping.CassandraPersistentProperty;
import org.springframework.data.convert.EntityWriter;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.model.BeanWrapper;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Delete.Where;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;

/**
 * The CassandraTemplate is a convenient API for all Cassandra operations using POJOs with their Spring Data Cassandra
 * mapping information. For low-level Cassandra operation, see {@link CqlTemplate}.
 * 
 * @author Alex Shvid
 * @author David Webb
 * @author Matthew T. Adams
 * 
 * @see CqlTemplate
 */
public class CassandraTemplate extends CqlTemplate implements CassandraOperations {

	/*
	 * Required elements for successful Template Operations.  These can be set with the Constructor, or wired in
	 * later.
	 */
	private CassandraConverter cassandraConverter;
	private CassandraMappingContext mappingContext;
	private boolean useFieldAccessOnly = false;

	/**
	 * Default Constructor for wiring in the required components later
	 */
	public CassandraTemplate() {
	}

	/**
	 * Constructor if only session and converter are known at time of Template Creation
	 * 
	 * @param session must not be {@literal null}
	 * @param converter must not be {@literal null}.
	 */
	public CassandraTemplate(Session session, CassandraConverter converter) {
		setSession(session);
		setConverter(converter);
	}

	public void setConverter(CassandraConverter cassandraConverter) {

		Assert.notNull(cassandraConverter);
		this.cassandraConverter = cassandraConverter;
		mappingContext = cassandraConverter.getCassandraMappingContext();
		Assert.notNull(mappingContext);
	}

	@Override
	public CassandraConverter getConverter() {
		return cassandraConverter;
	}

	public CassandraMappingContext getCassandraMappingContext() {
		return mappingContext;
	}

	@Override
	public void afterPropertiesSet() {
		super.afterPropertiesSet();

		Assert.notNull(cassandraConverter);
		Assert.notNull(mappingContext);
	}

	@Override
	public Long countById(Class<?> clazz, Object id) {

		Assert.notNull(clazz);
		Assert.notNull(id);

		CassandraPersistentEntity<?> entity = mappingContext.getPersistentEntity(clazz);
		if (entity == null) {
			throw new IllegalArgumentException(String.format("unknown persistent class [%s]", clazz.getName()));
		}

		Select select = QueryBuilder.select().countAll().from(entity.getTableName());
		appendIdCriteria(select.where(), entity, id);

		return count(select);
	}

	@Override
	public Long count(Select selectQuery) {
		return selectCount(selectQuery);
	}

	@Override
	public Long count(String tableName) {
		Select select = QueryBuilder.select().countAll().from(tableName);
		return selectCount(select);
	}

	@Override
	public <T> void delete(List<T> entities) {

		Assert.notEmpty(entities);

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
		batchDelete(tableName, entities, options, false);
	}

	@Override
	public void deleteById(Class<?> clazz, Object id) {

		Assert.notNull(clazz);
		Assert.notNull(id);

		CassandraPersistentEntity<?> entity = mappingContext.getPersistentEntity(clazz);

		Delete delete = QueryBuilder.delete().all().from(entity.getTableName());
		appendIdCriteria(delete.where(), entity, id);

		execute(delete.getQueryString());
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
		delete(tableName, entity, options, false);
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
		batchDelete(tableName, entities, options, true);
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
		delete(tableName, entity, options, true);
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
		return batchInsert(tableName, entities, options, false);
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
		return insert(tableName, entity, options, false);
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
		return batchInsert(tableName, entities, options, true);
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

		return insert(tableName, entity, options, true);
	}

	@Override
	public <T> List<T> selectAll(Class<T> selectClass) {

		Assert.notNull(selectClass);

		CassandraPersistentEntity<?> entity = mappingContext.getPersistentEntity(selectClass);
		if (entity == null) {
			throw new IllegalArgumentException(String.format("unknown persistent class [%s]", selectClass.getName()));
		}
		return select(QueryBuilder.select().all().from(entity.getTableName()), selectClass);
	}

	@Override
	public <T> List<T> select(Select cql, Class<T> selectClass) {

		Assert.notNull(cql);

		return select(cql.getQueryString(), selectClass);
	}

	@Override
	public <T> List<T> select(String cql, Class<T> selectClass) {

		Assert.hasText(cql);
		Assert.notNull(selectClass);

		return select(cql, new ReadRowCallback<T>(cassandraConverter, selectClass));
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public <T> List<T> selectByIds(Class<T> clazz, Iterable<?> ids) {

		CassandraPersistentEntity<?> entity = mappingContext.getPersistentEntity(clazz);
		if (entity == null) {
			throw new IllegalArgumentException(String.format("unknown persistent entity class [%s]", clazz.getName()));
		}
		if (entity.getIdProperty().isCompositePrimaryKey()) {
			throw new IllegalArgumentException(String.format(
					"entity class [%s] uses a composite primary key class [%s] which this method can't support", clazz.getName(),
					entity.getIdProperty().getCompositePrimaryKeyEntity().getType().getName()));
		}

		List idList = null;
		if (ids instanceof List) {
			idList = (List) ids;
		} else {
			idList = new ArrayList();
			for (Object id : ids) {
				idList.add(id);
			}
		}

		Select select = QueryBuilder.select().all().from(entity.getTableName());
		select.where(QueryBuilder.in(entity.getIdProperty().getColumnName(), idList.toArray()));

		return select(select, clazz);
	}

	@Override
	public <T> T selectOneById(Class<T> selectClass, Object id) {

		Assert.notNull(selectClass);
		Assert.notNull(id);

		CassandraPersistentEntity<?> entityClass = mappingContext.getPersistentEntity(selectClass);
		if (entityClass == null) {
			throw new IllegalArgumentException(String.format("unknown entity class [%s]", selectClass.getName()));
		}

		Select select = QueryBuilder.select().all().from(entityClass.getTableName());
		appendIdCriteria(select.where(), entityClass, id);

		return selectOne(select, selectClass);
	}

	protected interface ClauseCallback {
		void onClause(Clause clause);
	}

	protected void appendIdCriteria(final ClauseCallback clauseCallback, CassandraPersistentEntity<?> entity, Object id) {

		CassandraPersistentProperty idProperty = entity.getIdProperty();

		if (idProperty.isCompositePrimaryKey()) {

			CassandraPersistentEntity<?> idEntity = idProperty.getCompositePrimaryKeyEntity();

			final BeanWrapper<CassandraPersistentEntity<Object>, Object> idWrapper = BeanWrapper
					.<CassandraPersistentEntity<Object>, Object> create(id, cassandraConverter.getConversionService());

			idEntity.doWithProperties(new PropertyHandler<CassandraPersistentProperty>() {

				@Override
				public void doWithPersistentProperty(CassandraPersistentProperty p) {

					clauseCallback.onClause(QueryBuilder.eq(p.getColumnName(),
							idWrapper.getProperty(p, p.getActualType(), useFieldAccessOnly)));
				}
			});

			return;
		}

		clauseCallback.onClause(QueryBuilder.eq(idProperty.getColumnName(), id));
	}

	protected void appendIdCriteria(final com.datastax.driver.core.querybuilder.Select.Where where,
			CassandraPersistentEntity<?> entity, Object id) {

		appendIdCriteria(new ClauseCallback() {

			@Override
			public void onClause(Clause clause) {
				where.and(clause);
			}
		}, entity, id);
	}

	protected void appendIdCriteria(final Where where, CassandraPersistentEntity<?> entity, Object id) {

		appendIdCriteria(new ClauseCallback() {

			@Override
			public void onClause(Clause clause) {
				where.and(clause);
			}
		}, entity, id);
	}

	@Override
	public <T> T selectOne(Select selectQuery, Class<T> selectClass) {
		return selectOne(selectQuery.getQueryString(), selectClass);
	}

	@Override
	public <T> T selectOne(String cql, Class<T> selectClass) {
		return selectOne(cql, new ReadRowCallback<T>(cassandraConverter, selectClass));
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
		return batchUpdate(tableName, entities, options, false);
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
		return update(tableName, entity, options, false);
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
		return batchUpdate(tableName, entities, options, true);
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
		return update(tableName, entity, options, true);
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

	private <T> List<T> select(final String query, ReadRowCallback<T> readRowCallback) {

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

	private Long selectCount(final Select query) {

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
	private <T> T selectOne(final String query, ReadRowCallback<T> readRowCallback) {

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
	protected <T> void batchDelete(final String tableName, final List<T> entities, final QueryOptions options,
			final boolean deleteAsynchronously) {

		Assert.notEmpty(entities);

		final Batch b = createDeleteBatchQuery(tableName, entities, options, cassandraConverter);
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
	protected <T> List<T> batchInsert(final String tableName, final List<T> entities, final QueryOptions options,
			final boolean insertAsychronously) {

		Assert.notEmpty(entities);

		final Batch b = createInsertBatchQuery(tableName, entities, options, cassandraConverter);
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
	protected <T> List<T> batchUpdate(final String tableName, final List<T> entities, final QueryOptions options,
			final boolean updateAsychronously) {

		Assert.notEmpty(entities);

		final Batch b = toUpdateBatchQuery(tableName, entities, options, cassandraConverter);
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
	}

	/**
	 * Perform the removal of a Row.
	 * 
	 * @param tableName
	 * @param objectToRemove
	 */
	protected <T> void delete(final String tableName, final T objectToRemove, final QueryOptions options,
			final boolean deleteAsynchronously) {

		final Query q = createDeleteQuery(tableName, objectToRemove, options, cassandraConverter);
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
	}

	/**
	 * Insert a row into a Cassandra CQL Table
	 * 
	 * @param tableName
	 * @param entity
	 */
	protected <T> T insert(final String tableName, final T entity, final QueryOptions options,
			final boolean insertAsychronously) {

		final Query q = createInsertQuery(tableName, entity, options, cassandraConverter);

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
	protected <T> T update(final String tableName, final T entity, final QueryOptions options,
			final boolean updateAsychronously) {

		final Query q = toUpdateQuery(tableName, entity, options, cassandraConverter);
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
	}

	/**
	 * Verify the object is not an iterable type
	 * 
	 * @param o
	 */
	protected void ensureNotIterable(Object o) {

		if (null == o) {
			return;
		}

		if (o.getClass().isArray() || (o instanceof Iterable) || (o instanceof Iterator)) {
			throw new IllegalArgumentException("cannot use a multivalued object here.");
		}
	}

	/**
	 * Generates a Query Object for an insert
	 * 
	 * @param tableName
	 * @param objectToSave
	 * @param entity
	 * @param optionsByName
	 * 
	 * @return The Query object to run with session.execute();
	 */
	public static Query createInsertQuery(String tableName, final Object objectToSave, QueryOptions options,
			EntityWriter<Object, Object> entityWriter) {

		final Insert q = QueryBuilder.insertInto(tableName);

		/*
		 * Write properties
		 */
		entityWriter.write(objectToSave, q);

		/*
		 * Add Query Options
		 */
		CqlTemplate.addQueryOptions(q, options);

		/*
		 * Add TTL to Insert object
		 */
		if (options != null && options.getTtl() != null) {
			q.using(QueryBuilder.ttl(options.getTtl()));
		}

		return q;

	}

	/**
	 * Generates a Query Object for an Update
	 * 
	 * @param tableName
	 * @param objectToSave
	 * @param entity
	 * @param optionsByName
	 * 
	 * @return The Query object to run with session.execute();
	 */
	public static Query toUpdateQuery(String tableName, final Object objectToSave, QueryOptions options,
			EntityWriter<Object, Object> entityWriter) {

		final Update q = QueryBuilder.update(tableName);

		/*
		 * Write properties
		 */
		entityWriter.write(objectToSave, q);

		/*
		 * Add Query Options
		 */
		CqlTemplate.addQueryOptions(q, options);

		/*
		 * Add TTL to Insert object
		 */
		if (options != null && options.getTtl() != null) {
			q.using(QueryBuilder.ttl(options.getTtl()));
		}

		return q;

	}

	/**
	 * Generates a Batch Object for multiple Updates
	 * 
	 * @param tableName
	 * @param objectsToSave
	 * @param entity
	 * @param optionsByName
	 * 
	 * @return The Query object to run with session.execute();
	 */
	public static <T> Batch toUpdateBatchQuery(final String tableName, final List<T> objectsToSave, QueryOptions options,
			EntityWriter<Object, Object> entityWriter) {

		/*
		 * Return variable is a Batch statement
		 */
		final Batch b = QueryBuilder.batch();

		for (final T objectToSave : objectsToSave) {

			b.add((Statement) toUpdateQuery(tableName, objectToSave, options, entityWriter));

		}

		/*
		 * Add Query Options
		 */
		CqlTemplate.addQueryOptions(b, options);

		return b;

	}

	/**
	 * Generates a Batch Object for multiple inserts
	 * 
	 * @param tableName
	 * @param entities
	 * @param entity
	 * @param optionsByName
	 * 
	 * @return The Query object to run with session.execute();
	 */
	public static <T> Batch createInsertBatchQuery(final String tableName, final List<T> entities, QueryOptions options,
			EntityWriter<Object, Object> entityWriter) {

		Batch batch = QueryBuilder.batch();

		for (T entity : entities) {
			batch.add((Statement) createInsertQuery(tableName, entity, options, entityWriter));
		}

		CqlTemplate.addQueryOptions(batch, options);

		return batch;
	}

	/**
	 * Create a Delete Query Object from an annotated POJO
	 * 
	 * @param tableName
	 * @param object
	 * @param entity
	 * @param optionsByName
	 * @return
	 */
	public static Query createDeleteQuery(String tableName, final Object object, QueryOptions options,
			EntityWriter<Object, Object> entityWriter) {

		Delete.Selection ds = QueryBuilder.delete();
		Delete q = ds.from(tableName);
		Where w = q.where();

		entityWriter.write(object, w);

		CqlTemplate.addQueryOptions(q, options);

		return q;
	}

	/**
	 * Create a Batch Query object for multiple deletes.
	 * 
	 * @param tableName
	 * @param entities
	 * @param entity
	 * @param optionsByName
	 * 
	 * @return
	 */
	public static <T> Batch createDeleteBatchQuery(String tableName, List<T> entities, QueryOptions options,
			EntityWriter<Object, Object> entityWriter) {

		Batch batch = QueryBuilder.batch();

		for (T entity : entities) {
			batch.add((Statement) createDeleteQuery(tableName, entity, options, entityWriter));
		}

		CqlTemplate.addQueryOptions(batch, options);

		return batch;
	}

	public boolean getUseFieldAccessOnly() {
		return useFieldAccessOnly;
	}

	public void setUseFieldAccessOnly(boolean useFieldAccessOnly) {
		this.useFieldAccessOnly = useFieldAccessOnly;
	}
}
