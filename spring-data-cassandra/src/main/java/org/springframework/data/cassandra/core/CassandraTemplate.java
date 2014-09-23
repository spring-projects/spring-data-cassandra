/*
 * Copyright 2013-2014 the original author or authors
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.springframework.cassandra.core.AsynchronousQueryListener;
import org.springframework.cassandra.core.CqlOperations;
import org.springframework.cassandra.core.CqlTemplate;
import org.springframework.cassandra.core.Cancellable;
import org.springframework.cassandra.core.QueryForObjectListener;
import org.springframework.cassandra.core.QueryOptions;
import org.springframework.cassandra.core.SessionCallback;
import org.springframework.cassandra.core.WriteOptions;
import org.springframework.cassandra.core.cql.CqlIdentifier;
import org.springframework.cassandra.core.util.CollectionUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.mapping.CassandraPersistentProperty;
import org.springframework.data.convert.EntityWriter;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.model.BeanWrapper;
import org.springframework.util.Assert;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
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
 * @author Oliver Gierke
 * @see CqlTemplate
 */
public class CassandraTemplate extends CqlTemplate implements CassandraOperations {

	protected CassandraConverter cassandraConverter;
	protected CassandraMappingContext mappingContext;

	/**
	 * Default Constructor for wiring in the required components later
	 */
	public CassandraTemplate() {}

	public CassandraTemplate(Session session) {
		this(session, new MappingCassandraConverter());
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
		mappingContext = cassandraConverter.getMappingContext();
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
	public boolean exists(Class<?> type, Object id) {

		Assert.notNull(type);
		Assert.notNull(id);

		CassandraPersistentEntity<?> entity = mappingContext.getPersistentEntity(type);

		Select select = QueryBuilder.select().countAll().from(entity.getTableName().toCql());
		appendIdCriteria(select.where(), entity, id);

		Long count = queryForObject(select, Long.class);

		return count != 0;
	}

	@Override
	public long count(Class<?> type) {
		return count(getTableName(type).toCql());
	}

	@Override
	public <T> void delete(List<T> entities) {
		delete(entities, null);
	}

	@Override
	public <T> void delete(List<T> entities, QueryOptions options) {
		doBatchDelete(entities, options);
	}

	@Override
	public void deleteById(Class<?> type, Object id) {

		Assert.notNull(type);
		Assert.notNull(id);

		CassandraPersistentEntity<?> entity = mappingContext.getPersistentEntity(type);

		Delete delete = QueryBuilder.delete().from(entity.getTableName().toCql());
		appendIdCriteria(delete.where(), entity, id);

		execute(delete);
	}

	@Override
	public <T> void delete(T entity) {
		delete(entity, null);
	}

	@Override
	public <T> void delete(T entity, QueryOptions options) {
		doDelete(entity, options);
	}

	@Override
	public <T> Cancellable deleteAsynchronously(List<T> entities) {
		return doBatchDeleteAsync(entities, null, null);
	}

	@Override
	public <T> Cancellable deleteAsynchronously(List<T> entities, QueryOptions options) {
		return doBatchDeleteAsync(entities, null, options);
	}

	@Override
	public <T> Cancellable deleteAsynchronously(List<T> entities, DeletionListener<T> listener) {
		return doBatchDeleteAsync(entities, listener, null);
	}

	@Override
	public <T> Cancellable deleteAsynchronously(List<T> entities, DeletionListener<T> listener, QueryOptions options) {
		return doBatchDeleteAsync(entities, listener, options);
	}

	@Override
	public <T> Cancellable deleteAsynchronously(T entity) {
		return doDeleteAsync(entity, null, null);
	}

	@Override
	public <T> Cancellable deleteAsynchronously(T entity, QueryOptions options) {
		return doDeleteAsync(entity, null, options);
	}

	@Override
	public <T> Cancellable deleteAsynchronously(T entity, DeletionListener<T> listener) {
		return doDeleteAsync(entity, listener, null);
	}

	@Override
	public <T> Cancellable deleteAsynchronously(T entity, DeletionListener<T> listener, QueryOptions options) {
		return doDeleteAsync(entity, listener, options);
	}

	@Override
	public CqlIdentifier getTableName(Class<?> type) {
		return mappingContext.getPersistentEntity(type).getTableName();
	}

	@Override
	public <T> List<T> insert(List<T> entities) {
		return insert(entities, null);
	}

	@Override
	public <T> List<T> insert(List<T> entities, WriteOptions options) {
		return doBatchInsert(entities, options);
	}

	@Override
	public <T> T insert(T entity) {
		return insert(entity, null);
	}

	@Override
	public <T> T insert(T entity, WriteOptions options) {
		return doInsert(entity, options);
	}

	/**
	 * @deprecated See {@link CassandraTemplate#insertAsynchronously(List)}
	 */
	@Deprecated
	@Override
	public <T> List<T> insertAsynchronously(List<T> entities) {
		insertAsynchronously(entities, (WriteOptions) null);
		return entities;
	}

	/**
	 * @deprecated See {@link CassandraTemplate#insertAsynchronously(List, WriteOptions)}
	 */
	@Deprecated
	@Override
	public <T> List<T> insertAsynchronously(List<T> entities, WriteOptions options) {
		doInsertAsynchronously(entities, null, options);
		return entities;
	}

	@Override
	public <T> Cancellable insertAsynchronously(List<T> entities, WriteListener<T> listener) {
		return doInsertAsynchronously(entities, listener, null);
	}

	@Override
	public <T> Cancellable insertAsynchronously(List<T> entities, WriteListener<T> listener, WriteOptions options) {
		return doInsertAsynchronously(entities, listener, options);
	}

	/**
	 * This method resolves ambiguity the compiler sees as a result of type erasure between
	 * {@link #insertAsynchronously(Object, WriteListener, WriteOptions)} and {@link #insertAsynchronously(List<T>,
	 * WriteListener<T>, WriteOptions)}.
	 */
	protected <T> Cancellable doInsertAsynchronously(List<T> entities, WriteListener<T> listener, WriteOptions options) {
		return doBatchInsertAsync(entities, listener, options);
	}

	/**
	 * @deprecated See {@link CqlOperations#insert(Object)}
	 */
	@Deprecated
	@Override
	public <T> T insertAsynchronously(T entity) {
		insertAsynchronously(entity, null, null);
		return entity;
	}

	/**
	 * @deprecated See {@link CqlOperations#insert(Object,WriteOptions)}
	 */
	@Deprecated
	@Override
	public <T> T insertAsynchronously(T entity, WriteOptions options) {
		insertAsynchronously(entity, null, options);
		return entity;
	}

	@Override
	public <T> Cancellable insertAsynchronously(T entity, WriteListener<T> listener) {
		return insertAsynchronously(entity, listener, null);
	}

	@Override
	public <T> Cancellable insertAsynchronously(T entity, WriteListener<T> listener, WriteOptions options) {
		return doInsertAsync(entity, listener, options);
	}

	@Override
	public <T> List<T> selectAll(Class<T> type) {
		return select(QueryBuilder.select().all().from(getTableName(type).toCql()), type);
	}

	@Override
	public <T> List<T> select(String cql, Class<T> type) {

		Assert.hasText(cql);
		Assert.notNull(type);

		return select(cql, new CassandraConverterRowCallback<T>(cassandraConverter, type));
	}

	@Override
	public <T> List<T> select(Select select, Class<T> type) {

		Assert.notNull(select);

		return select(select, new CassandraConverterRowCallback<T>(cassandraConverter, type));
	}

	@Override
	public <T> List<T> selectBySimpleIds(Class<T> type, Iterable<?> ids) {

		CassandraPersistentEntity<?> entity = mappingContext.getPersistentEntity(type);

		if (entity.getIdProperty().isCompositePrimaryKey()) {
			throw new IllegalArgumentException(String.format(
					"entity class [%s] uses a composite primary key class [%s] which this method can't support", type.getName(),
					entity.getIdProperty().getCompositePrimaryKeyEntity().getType().getName()));
		}

		Select select = QueryBuilder.select().all().from(entity.getTableName().toCql());
		select.where(QueryBuilder.in(entity.getIdProperty().getColumnName().toCql(), CollectionUtils.toArray(ids)));

		return select(select, type);
	}

	@Override
	public <T> T selectOneById(Class<T> type, Object id) {

		Assert.notNull(type);
		Assert.notNull(id);

		CassandraPersistentEntity<?> entity = mappingContext.getPersistentEntity(type);
		if (entity == null) {
			throw new IllegalArgumentException(String.format("unknown entity class [%s]", type.getName()));
		}

		Select select = QueryBuilder.select().all().from(entity.getTableName().toCql());
		appendIdCriteria(select.where(), entity, id);

		return selectOne(select, type);
	}

	protected interface ClauseCallback {
		void doWithClause(Clause clause);
	}

	protected void appendIdCriteria(final ClauseCallback clauseCallback, CassandraPersistentEntity<?> entity,
			final Map<?, ?> id) {

		for (Map.Entry<?, ?> entry : id.entrySet()) {

			CassandraPersistentProperty property = entity.getPersistentProperty(entry.getKey().toString());
			clauseCallback.doWithClause(QueryBuilder.eq(property.getColumnName().toCql(), entry.getValue()));
		}
	}

	protected void appendIdCriteria(final ClauseCallback clauseCallback, CassandraPersistentEntity<?> entity, Object id) {

		if (id instanceof Map<?, ?>) {

			appendIdCriteria(clauseCallback, entity, (Map<?, ?>) id);
			return;
		}

		CassandraPersistentProperty idProperty = entity.getIdProperty();

		if (idProperty.isCompositePrimaryKey()) {

			CassandraPersistentEntity<?> idEntity = idProperty.getCompositePrimaryKeyEntity();

			final BeanWrapper<Object> idWrapper = BeanWrapper.create(id, cassandraConverter.getConversionService());

			idEntity.doWithProperties(new PropertyHandler<CassandraPersistentProperty>() {

				@Override
				public void doWithPersistentProperty(CassandraPersistentProperty p) {

					clauseCallback.doWithClause(QueryBuilder.eq(p.getColumnName().toCql(),
							idWrapper.getProperty(p, p.getActualType())));
				}
			});

			return;
		}

		clauseCallback.doWithClause(QueryBuilder.eq(idProperty.getColumnName().toCql(), id));
	}

	protected void appendIdCriteria(final com.datastax.driver.core.querybuilder.Select.Where where,
			CassandraPersistentEntity<?> entity, Object id) {

		appendIdCriteria(new ClauseCallback() {

			@Override
			public void doWithClause(Clause clause) {
				where.and(clause);
			}
		}, entity, id);
	}

	protected void appendIdCriteria(final Where where, CassandraPersistentEntity<?> entity, Object id) {

		appendIdCriteria(new ClauseCallback() {

			@Override
			public void doWithClause(Clause clause) {
				where.and(clause);
			}
		}, entity, id);
	}

	@Override
	public <T> T selectOne(String cql, Class<T> type) {
		return selectOne(cql, new CassandraConverterRowCallback<T>(cassandraConverter, type));
	}

	@Override
	public <T> T selectOne(Select select, Class<T> type) {
		return selectOne(select, new CassandraConverterRowCallback<T>(cassandraConverter, type));
	}

	@Override
	public <T> List<T> update(List<T> entities) {
		return update(entities, null);
	}

	@Override
	public <T> List<T> update(List<T> entities, WriteOptions options) {
		return doBatchUpdate(entities, options);
	}

	@Override
	public <T> T update(T entity) {
		return update(entity, null);
	}

	@Override
	public <T> T update(T entity, WriteOptions options) {
		return doUpdate(entity, options);
	}

	@Override
	public <T> List<T> updateAsynchronously(List<T> entities) {
		doUpdateAsynchronously(entities, null, null);
		return entities;
	}

	@Override
	public <T> List<T> updateAsynchronously(List<T> entities, WriteOptions options) {
		doUpdateAsynchronously(entities, null, options);
		return entities;
	}

	@Override
	public <T> Cancellable updateAsynchronously(List<T> entities, WriteListener<T> listener) {
		return doUpdateAsynchronously(entities, listener, null);
	}

	@Override
	public <T> Cancellable updateAsynchronously(List<T> entities, WriteListener<T> listener, WriteOptions options) {
		return doUpdateAsynchronously(entities, listener, options);
	}

	/**
	 * This method resolves ambiguity the compiler sees as a result of type erasure between
	 * {@link #updateAsynchronously(Object, WriteListener, WriteOptions)} and {@link #updateAsynchronously(List<T>,
	 * WriteListener<T>, WriteOptions)}.
	 */
	protected <T> Cancellable doUpdateAsynchronously(List<T> entities, WriteListener<T> listener, WriteOptions options) {
		return doBatchUpdateAsync(entities, listener, options);
	}

	@Override
	public <T> T updateAsynchronously(T entity) {
		updateAsynchronously(entity, null, null);
		return entity;
	}

	@Override
	public <T> T updateAsynchronously(T entity, WriteOptions options) {
		updateAsynchronously(entity, null, options);
		return entity;
	}

	@Override
	public <T> Cancellable updateAsynchronously(T entity, WriteListener<T> listener) {
		return updateAsynchronously(entity, listener, null);
	}

	@Override
	public <T> Cancellable updateAsynchronously(T entity, WriteListener<T> listener, WriteOptions options) {
		return doUpdateAsync(entity, listener, options);
	}

	protected <T> CqlIdentifier determineTableName(T obj) {
		return obj == null ? null : determineTableName(obj.getClass());
	}

	protected <T> List<T> select(final String query, CassandraConverterRowCallback<T> readRowCallback) {

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

	protected <T> List<T> select(final Select query, CassandraConverterRowCallback<T> readRowCallback) {

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
	 * @param query
	 * @param readRowCallback
	 * @return
	 */
	protected <T> T selectOne(String query, CassandraConverterRowCallback<T> readRowCallback) {

		logger.debug(query);

		ResultSet resultSet = query(query);

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

	protected <T> T selectOne(Select query, CassandraConverterRowCallback<T> readRowCallback) {

		ResultSet resultSet = query(query);

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

	protected <T> void doBatchDelete(List<T> entities, QueryOptions options) {
		execute(createDeleteBatchQuery(getTableName(entities.get(0).getClass()).toCql(), entities, options,
				cassandraConverter));
	}

	protected <T> Cancellable doBatchDeleteAsync(final List<T> entities, final DeletionListener listener,
			QueryOptions options) {

		AsynchronousQueryListener aql = listener == null ? null : new AsynchronousQueryListener() {

			@Override
			public void onQueryComplete(ResultSetFuture rsf) {
				try {
					rsf.getUninterruptibly();
					listener.onDeletionComplete(entities);
				} catch (Exception e) {
					listener.onException(translateExceptionIfPossible(e));
				}
			}
		};

		return executeAsynchronously(
				createDeleteBatchQuery(getTableName(entities.get(0).getClass()).toCql(), entities, options, cassandraConverter),
				aql);
	}

	protected <T> T doInsert(T entity, WriteOptions options) {

		Assert.notNull(entity);
		Insert insert = createInsertQuery(getTableName(entity.getClass()).toCql(), entity, options, cassandraConverter);
		execute(insert);
		return entity;
	}

	protected <T> Cancellable doInsertAsync(final T entity, final WriteListener<T> listener, WriteOptions options) {

		Assert.notNull(entity);

		Insert insert = createInsertQuery(getTableName(entity.getClass()).toCql(), entity, options, cassandraConverter);

		AsynchronousQueryListener aql = listener == null ? null : new AsynchronousQueryListener() {

			@SuppressWarnings("unchecked")
			@Override
			public void onQueryComplete(ResultSetFuture rsf) {
				try {
					rsf.getUninterruptibly();
					listener.onWriteComplete((Collection<T>) CollectionUtils.toList(entity));
				} catch (Exception x) {
					listener.onException(translateExceptionIfPossible(x));
				}
			}
		};

		return executeAsynchronously(insert, aql);
	}

	protected <T> List<T> doBatchInsert(List<T> entities, WriteOptions options) {
		return doBatchWrite(entities, options, true);
	}

	protected <T> List<T> doBatchUpdate(List<T> entities, WriteOptions options) {
		return doBatchWrite(entities, options, false);
	}

	protected <T> List<T> doBatchWrite(List<T> entities, WriteOptions options, boolean insert) {

		if (entities == null || entities.size() == 0) {
			if (logger.isWarnEnabled()) {
				logger.warn("no-op due to given null or empty list");
			}
			return entities;
		}

		String tableName = getTableName(entities.get(0).getClass()).toCql();
		Batch b = insert ? createInsertBatchQuery(tableName, entities, options, cassandraConverter)
				: createUpdateBatchQuery(tableName, entities, options, cassandraConverter);
		execute(b);

		return entities;
	}

	/**
	 * Asynchronously performs a batch insert or update.
	 * 
	 * @param entities The entities to insert or update.
	 * @param listener The listener that will receive notification of the completion of the batch insert or update. May be
	 *          <code>null</code>.
	 * @param options The {@link WriteOptions} to use. May be <code>null</code>.
	 * @return A {@link Cancellable} that can be used to cancel the query if necessary.
	 */
	protected <T> Cancellable doBatchInsertAsync(final List<T> entities, final WriteListener<T> listener,
			WriteOptions options) {
		return doBatchWriteAsync(entities, listener, options, true);
	}

	/**
	 * Asynchronously performs a batch insert or update.
	 * 
	 * @param entities The entities to insert or update.
	 * @param listener The listener that will receive notification of the completion of the batch insert or update. May be
	 *          <code>null</code>.
	 * @param options The {@link WriteOptions} to use. May be <code>null</code>.
	 * @return A {@link Cancellable} that can be used to cancel the query if necessary.
	 */
	protected <T> Cancellable doBatchUpdateAsync(final List<T> entities, final WriteListener<T> listener,
			WriteOptions options) {
		return doBatchWriteAsync(entities, listener, options, false);
	}

	/**
	 * Asynchronously performs a batch insert or update.
	 * 
	 * @param entities The entities to insert or update.
	 * @param listener The listener that will receive notification of the completion of the batch insert or update. May be
	 *          <code>null</code>.
	 * @param options The {@link WriteOptions} to use. May be <code>null</code>.
	 * @param insert If <code>true</code>, then an insert is performed, else an update is performed.
	 * @return A {@link Cancellable} that can be used to cancel the query if necessary.
	 */
	protected <T> Cancellable doBatchWriteAsync(final List<T> entities, final WriteListener<T> listener,
			WriteOptions options, boolean insert) {

		if (entities == null || entities.size() == 0) {
			if (logger.isWarnEnabled()) {
				logger.warn("no-op due to given null or empty list");
			}
			return new Cancellable() {

				@Override
				public void cancel() {
					if (logger.isWarnEnabled()) {
						logger.warn("no-op query cancellation due to given null or empty list");
					}
				}
			};
		}

		String tableName = getTableName(entities.get(0).getClass()).toCql();
		Batch b = insert ? createInsertBatchQuery(tableName, entities, options, cassandraConverter)
				: createUpdateBatchQuery(tableName, entities, options, cassandraConverter);

		AsynchronousQueryListener aql = listener == null ? null : new AsynchronousQueryListener() {

			@Override
			public void onQueryComplete(ResultSetFuture rsf) {
				try {
					rsf.getUninterruptibly();
					listener.onWriteComplete(entities);
				} catch (Exception e) {
					listener.onException(translateExceptionIfPossible(e));
				}
			}
		};

		return executeAsynchronously(b, aql);
	}

	protected <T> void doDelete(T entity, QueryOptions options) {

		Assert.notNull(entity);
		Delete delete = createDeleteQuery(getTableName(entity.getClass()).toCql(), entity, options, cassandraConverter);
		execute(delete);
	}

	protected <T> Cancellable doDeleteAsync(final T entity, final DeletionListener listener, QueryOptions options) {

		Assert.notNull(entity);

		Delete delete = createDeleteQuery(getTableName(entity.getClass()).toCql(), entity, options, cassandraConverter);

		AsynchronousQueryListener aql = listener == null ? null : new AsynchronousQueryListener() {
			@Override
			public void onQueryComplete(ResultSetFuture rsf) {
				try {
					rsf.getUninterruptibly();
					listener.onDeletionComplete(CollectionUtils.toList(entity));
				} catch (Exception x) {
					listener.onException(translateExceptionIfPossible(x));
				}
			}
		};

		return executeAsynchronously(delete, aql);
	}

	protected <T> T doUpdate(T entity, WriteOptions options) {

		Assert.notNull(entity);
		Update update = createUpdateQuery(getTableName(entity.getClass()).toCql(), entity, options, cassandraConverter);
		execute(update);
		return entity;
	}

	protected <T> Cancellable doUpdateAsync(final T entity, final WriteListener<T> listener, WriteOptions options) {

		Assert.notNull(entity);

		Update update = createUpdateQuery(getTableName(entity.getClass()).toCql(), entity, options, cassandraConverter);

		AsynchronousQueryListener aql = listener == null ? null : new AsynchronousQueryListener() {

			@SuppressWarnings("unchecked")
			@Override
			public void onQueryComplete(ResultSetFuture rsf) {
				try {
					rsf.getUninterruptibly();
					listener.onWriteComplete((Collection<T>) CollectionUtils.toList(entity));
				} catch (Exception x) {
					listener.onException(translateExceptionIfPossible(x));
				}
			}
		};

		return executeAsynchronously(update, aql);
	}

	/**
	 * Generates a Query Object for an insert
	 * 
	 * @param tableName
	 * @param objectToSave
	 * @param entity
	 * @param optionsByName
	 * @return The Query object to run with session.execute();
	 */
	public static Insert createInsertQuery(String tableName, Object objectToSave, WriteOptions options,
			EntityWriter<Object, Object> entityWriter) {

		Insert insert = QueryBuilder.insertInto(tableName);
		entityWriter.write(objectToSave, insert);
		CqlTemplate.addWriteOptions(insert, options);
		return insert;
	}

	/**
	 * @deprecated Method renamed. Use {@link #createUpdateQuery(String, Object, WriteOptions, EntityWriter)}
	 * @see #createUpdateQuery(String, Object, WriteOptions, EntityWriter)
	 */
	@Deprecated
	public static Update toUpdateQueryX(String tableName, Object objectToSave, WriteOptions options,
			EntityWriter<Object, Object> entityWriter) {
		return createUpdateQuery(tableName, objectToSave, options, entityWriter);
	}

	/**
	 * Generates a Query Object for an Update
	 * 
	 * @param tableName
	 * @param objectToSave
	 * @param entity
	 * @param optionsByName
	 * @return The Query object to run with session.execute();
	 */
	public static Update createUpdateQuery(String tableName, Object objectToSave, WriteOptions options,
			EntityWriter<Object, Object> entityWriter) {

		Update update = QueryBuilder.update(tableName);
		entityWriter.write(objectToSave, update);
		CqlTemplate.addWriteOptions(update, options);
		return update;
	}

	/**
	 * @deprecated Method renamed. Use {@link #createUpdateBatchQuery(String, List, WriteOptions, EntityWriter)}
	 * @see #createUpdateBatchQuery(String, List, WriteOptions, EntityWriter)
	 */
	@Deprecated
	public static <T> Batch toUpdateBatchQuery(String tableName, List<T> objectsToSave, WriteOptions options,
			EntityWriter<Object, Object> entityWriter) {
		return createUpdateBatchQuery(tableName, objectsToSave, options, entityWriter);
	}

	/**
	 * Generates a Batch Object for multiple Updates
	 * 
	 * @param tableName
	 * @param objectsToSave
	 * @param entity
	 * @param optionsByName
	 * @return The Query object to run with session.execute();
	 */
	public static <T> Batch createUpdateBatchQuery(String tableName, List<T> objectsToSave, WriteOptions options,
			EntityWriter<Object, Object> entityWriter) {

		Batch b = QueryBuilder.batch();

		for (T objectToSave : objectsToSave) {
			b.add(createUpdateQuery(tableName, objectToSave, options, entityWriter));
		}

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
	 * @return The Query object to run with session.execute();
	 */
	public static <T> Batch createInsertBatchQuery(String tableName, List<T> entities, WriteOptions options,
			EntityWriter<Object, Object> entityWriter) {

		Batch batch = QueryBuilder.batch();

		for (T entity : entities) {
			batch.add(createInsertQuery(tableName, entity, options, entityWriter));
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
	public static Delete createDeleteQuery(String tableName, Object object, QueryOptions options,
			EntityWriter<Object, Object> entityWriter) {

		Delete.Selection ds = QueryBuilder.delete();
		Delete delete = ds.from(tableName);
		Where w = delete.where();
		entityWriter.write(object, w);
		CqlTemplate.addQueryOptions(delete, options);
		return delete;
	}

	/**
	 * Create a Batch Query object for multiple deletes.
	 * 
	 * @param tableName
	 * @param entities
	 * @param entity
	 * @param optionsByName
	 * @return
	 */
	public static <T> Batch createDeleteBatchQuery(String tableName, List<T> entities, QueryOptions options,
			EntityWriter<Object, Object> entityWriter) {

		Assert.notEmpty(entities);
		Assert.hasText(tableName);

		Batch batch = QueryBuilder.batch();

		for (T entity : entities) {
			batch.add(createDeleteQuery(tableName, entity, options, entityWriter));
		}

		CqlTemplate.addQueryOptions(batch, options);

		return batch;
	}

	@Override
	public <T> void deleteAll(Class<T> clazz) {

		if (!mappingContext.contains(clazz)) {
			throw new IllegalArgumentException(String.format("unknown persistent entity class [%s]", clazz.getName()));
		}

		truncate(mappingContext.getPersistentEntity(clazz).getTableName());
	}

	@Override
	public <T> Cancellable selectOneAsynchronously(Select select, Class<T> type, QueryForObjectListener<T> listener) {
		return selectOneAsynchronously(select, type, listener, null);
	}

	@Override
	public <T> Cancellable selectOneAsynchronously(String cql, Class<T> type, QueryForObjectListener<T> listener) {
		return selectOneAsynchronously(cql, type, listener, null);
	}

	@Override
	public <T> Cancellable selectOneAsynchronously(Select select, Class<T> type, QueryForObjectListener<T> listener,
			QueryOptions options) {
		return doSelectOneAsync(select, type, listener, options);
	}

	@Override
	public <T> Cancellable selectOneAsynchronously(String cql, Class<T> type, QueryForObjectListener<T> listener,
			QueryOptions options) {
		return doSelectOneAsync(cql, type, listener, options);
	}

	protected <T> Cancellable doSelectOneAsync(final Object query, final Class<T> type,
			final QueryForObjectListener<T> listener, QueryOptions options) {

		AsynchronousQueryListener aql = new AsynchronousQueryListener() {

			@Override
			public void onQueryComplete(ResultSetFuture rsf) {
				try {
					ResultSet rs = rsf.getUninterruptibly();
					Iterator<Row> iterator = rs.iterator();
					if (iterator.hasNext()) {
						Row row = iterator.next();
						T result = new CassandraConverterRowCallback<T>(cassandraConverter, type).doWith(row);
						if (iterator.hasNext()) {
							throw new DuplicateKeyException("found two or more results in query " + query);
						}
						listener.onQueryComplete(result);
					}
				} catch (Exception e) {
					listener.onException(translateExceptionIfPossible(e));
				}
			}
		};
		if (query instanceof String) {
			return queryAsynchronously((String) query, aql, options);
		}
		if (query instanceof Select) {
			return queryAsynchronously((Select) query, aql);
		}
		throw new IllegalArgumentException(String.format("Expected type String or Select; got type [%s] with value [%s]",
				query.getClass(), query));
	}
}
