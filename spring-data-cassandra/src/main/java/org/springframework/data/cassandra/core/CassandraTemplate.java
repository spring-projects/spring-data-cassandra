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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.springframework.cassandra.core.CqlTemplate;
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
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
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
		batchDelete(entities, options, false);
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
		delete(entity, options, false);
	}

	@Override
	public <T> void deleteAsynchronously(List<T> entities) {
		deleteAsynchronously(entities, null);
	}

	@Override
	public <T> void deleteAsynchronously(List<T> entities, QueryOptions options) {
		batchDelete(entities, options, true);
	}

	@Override
	public <T> void deleteAsynchronously(T entity) {
		deleteAsynchronously(entity, null);
	}

	@Override
	public <T> void deleteAsynchronously(T entity, QueryOptions options) {
		delete(entity, options, true);
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
		return batchInsert(entities, options, false);
	}

	@Override
	public <T> T insert(T entity) {
		return insert(entity, null);
	}

	@Override
	public <T> T insert(T entity, WriteOptions options) {
		return insert(entity, options, false);
	}

	@Override
	public <T> List<T> insertAsynchronously(List<T> entities) {
		return insertAsynchronously(entities, null);
	}

	@Override
	public <T> List<T> insertAsynchronously(List<T> entities, WriteOptions options) {
		return batchInsert(entities, options, true);
	}

	@Override
	public <T> T insertAsynchronously(T entity) {
		return insertAsynchronously(entity, null);
	}

	@Override
	public <T> T insertAsynchronously(T entity, WriteOptions options) {
		return insert(entity, options, true);
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
		return batchUpdate(entities, options, false);
	}

	@Override
	public <T> T update(T entity) {
		return update(entity, null);
	}

	@Override
	public <T> T update(T entity, WriteOptions options) {
		return update(entity, options, false);
	}

	@Override
	public <T> List<T> updateAsynchronously(List<T> entities) {
		return updateAsynchronously(entities, null);
	}

	@Override
	public <T> List<T> updateAsynchronously(List<T> entities, WriteOptions options) {
		return batchUpdate(entities, options, true);
	}

	@Override
	public <T> T updateAsynchronously(T entity) {
		return updateAsynchronously(entity, null);
	}

	@Override
	public <T> T updateAsynchronously(T entity, WriteOptions options) {
		return update(entity, options, true);
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

	/**
	 * Perform the deletion on a list of objects
	 * 
	 * @param tableName
	 * @param objectToRemove
	 */
	protected <T> void batchDelete(List<T> entities, QueryOptions options, boolean asynchronously) {

		Assert.notEmpty(entities);

		Batch b = createDeleteBatchQuery(getTableName(entities.get(0).getClass()).toCql(), entities, options,
				cassandraConverter);

		if (asynchronously) {
			executeAsynchronously(b);
		} else {
			execute(b);
		}
	}

	protected <T> T insert(T entity, WriteOptions options, boolean asynchronously) {

		Assert.notNull(entity);

		Insert insert = createInsertQuery(getTableName(entity.getClass()).toCql(), entity, options, cassandraConverter);

		if (asynchronously) {
			executeAsynchronously(insert);
		} else {
			execute(insert);
		}

		return entity;
	}

	protected <T> List<T> batchInsert(List<T> entities, WriteOptions options, boolean asychronously) {

		if (entities == null || entities.size() == 0) {
			if (logger.isWarnEnabled()) {
				logger.warn("no-op due to given null or empty list");
			}
			return entities;
		}

		Batch b = createInsertBatchQuery(getTableName(entities.get(0).getClass()).toCql(), entities, options,
				cassandraConverter);

		if (asychronously) {
			executeAsynchronously(b);
		} else {
			execute(b);
		}

		return entities;
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
	protected <T> List<T> batchUpdate(List<T> entities, WriteOptions options, boolean asychronously) {

		Assert.notEmpty(entities);

		Batch b = toUpdateBatchQuery(getTableName(entities.get(0).getClass()).toCql(), entities, options,
				cassandraConverter);

		if (asychronously) {
			executeAsynchronously(b);
		} else {
			execute(b);
		}

		return entities;
	}

	/**
	 * Perform the removal of a Row.
	 * 
	 * @param tableName
	 * @param entity
	 */
	protected <T> void delete(T entity, QueryOptions options, boolean asynchronously) {

		Assert.notNull(entity);

		Delete delete = createDeleteQuery(getTableName(entity.getClass()).toCql(), entity, options, cassandraConverter);

		if (asynchronously) {
			executeAsynchronously(delete);
		} else {
			execute(delete);
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
	protected <T> T update(T entity, WriteOptions options, boolean asychronously) {

		Assert.notNull(entity);

		Update update = toUpdateQuery(getTableName(entity.getClass()).toCql(), entity, options, cassandraConverter);

		if (asychronously) {
			executeAsynchronously(update);
		} else {
			execute(update);
		}

		return entity;
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

		/*
		 * Write properties
		 */
		entityWriter.write(objectToSave, insert);

		/*
		 * Add Query Options
		 */
		CqlTemplate.addWriteOptions(insert, options);

		return insert;
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
	public static Update toUpdateQuery(String tableName, Object objectToSave, WriteOptions options,
			EntityWriter<Object, Object> entityWriter) {

		Update update = QueryBuilder.update(tableName);

		/*
		 * Write properties
		 */
		entityWriter.write(objectToSave, update);

		/*
		 * Add Query Options
		 */
		CqlTemplate.addWriteOptions(update, options);

		return update;
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
	public static <T> Batch toUpdateBatchQuery(String tableName, List<T> objectsToSave, WriteOptions options,
			EntityWriter<Object, Object> entityWriter) {

		/*
		 * Return variable is a Batch statement
		 */
		Batch b = QueryBuilder.batch();

		for (T objectToSave : objectsToSave) {

			b.add(toUpdateQuery(tableName, objectToSave, options, entityWriter));

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
}
