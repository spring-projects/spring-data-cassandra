/*
 * Copyright 2016 the original author or authors.
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
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;

import org.springframework.cassandra.core.AsyncCqlOperations;
import org.springframework.cassandra.core.AsyncCqlTemplate;
import org.springframework.cassandra.core.AsyncSessionCallback;
import org.springframework.cassandra.core.CqlProvider;
import org.springframework.cassandra.core.GuavaListenableFutureAdapter;
import org.springframework.cassandra.core.QueryOptions;
import org.springframework.cassandra.core.WriteOptions;
import org.springframework.cassandra.core.cql.CqlIdentifier;
import org.springframework.cassandra.core.support.CQLExceptionTranslator;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.concurrent.ListenableFuture;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Truncate;
import com.datastax.driver.core.querybuilder.Update;

/**
 * Primary implementation of {@link AsyncCassandraOperations}. It simplifies the use of asynchronous Cassandra usage and
 * helps to avoid common errors. It executes core Cassandra workflow. This class executes CQL queries or updates,
 * initiating iteration over {@link ResultSet} and catching Cassandra exceptions and translating them to the generic,
 * more informative exception hierarchy defined in the {@code org.springframework.dao} package.
 * <p>
 * Can be used within a service implementation via direct instantiation with a {@link Session} reference, or get
 * prepared in an application context and given to services as bean reference.
 * <p>
 * Note: The {@link Session} should always be configured as a bean in the application context, in the first case given
 * to the service directly, in the second case to the prepared template.
 *
 * @author Mark Paluch
 * @since 2.0
 */
public class AsyncCassandraTemplate implements AsyncCassandraOperations {

	private final CQLExceptionTranslator exceptionTranslator;
	private final CassandraConverter converter;
	private final CassandraMappingContext mappingContext;
	private final AsyncCqlOperations cqlOperations;

	/**
	 * Creates an instance of {@link AsyncCassandraTemplate} initialized with the given {@link Session} and a default
	 * {@link MappingCassandraConverter}.
	 *
	 * @param session {@link Session} used to interact with Cassandra; must not be {@literal null}.
	 * @see CassandraConverter
	 * @see Session
	 */
	public AsyncCassandraTemplate(Session session) {
		this(session, newConverter());
	}

	/**
	 * Creates an instance of {@link AsyncCassandraTemplate} initialized with the given {@link Session} and
	 * {@link CassandraConverter}.
	 *
	 * @param session {@link Session} used to interact with Cassandra; must not be {@literal null}.
	 * @param converter {@link CassandraConverter} used to convert between Java and Cassandra types; must not be
	 *          {@literal null}.
	 * @see CassandraConverter
	 * @see Session
	 */
	public AsyncCassandraTemplate(Session session, CassandraConverter converter) {

		Assert.notNull(session, "Session must not be null");
		Assert.notNull(converter, "CassandraConverter must not be null");

		this.converter = converter;
		this.mappingContext = converter.getMappingContext();

		AsyncCqlTemplate asyncCqlTemplate = new AsyncCqlTemplate(session);
		this.exceptionTranslator = asyncCqlTemplate.getExceptionTranslator();
		this.cqlOperations = asyncCqlTemplate;
	}

	/**
	 * Creates an instance of {@link AsyncCassandraTemplate} initialized with the given {@link AsyncCqlTemplate} and
	 * {@link CassandraConverter}.
	 *
	 * @param asyncCqlTemplate {@link AsyncCqlTemplate} used to interact with Cassandra; must not be {@literal null}.
	 * @param converter {@link CassandraConverter} used to convert between Java and Cassandra types; must not be
	 *          {@literal null}.
	 * @see CassandraConverter
	 * @see Session
	 */
	public AsyncCassandraTemplate(AsyncCqlTemplate asyncCqlTemplate, CassandraConverter converter) {

		Assert.notNull(asyncCqlTemplate, "AsyncCqlTemplate must not be null");
		Assert.notNull(converter, "CassandraConverter must not be null");

		this.converter = converter;
		this.mappingContext = converter.getMappingContext();
		this.cqlOperations = asyncCqlTemplate;
		this.exceptionTranslator = asyncCqlTemplate.getExceptionTranslator();
	}

	private static MappingCassandraConverter newConverter() {

		MappingCassandraConverter converter = new MappingCassandraConverter();
		converter.afterPropertiesSet();

		return converter;
	}

	// -------------------------------------------------------------------------
	// Methods dealing with static CQL
	// -------------------------------------------------------------------------

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.AsyncCassandraOperations#select(java.lang.String, java.lang.Class)
	 */
	@Override
	public <T> ListenableFuture<List<T>> select(String cql, Class<T> entityClass) {

		Assert.hasText(cql, "Statement must not be empty");

		return select(new SimpleStatement(cql), entityClass);
	}

	@Override
	public <T> ListenableFuture<Void> select(String cql, Consumer<T> entityConsumer, Class<T> entityClass)
			throws DataAccessException {

		Assert.hasText(cql, "Statement must not be empty");
		Assert.notNull(entityConsumer, "Entity Consumer must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		return select(new SimpleStatement(cql), entityConsumer, entityClass);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.AsyncCassandraOperations#selectOne(java.lang.String, java.lang.Class)
	 */
	@Override
	public <T> ListenableFuture<T> selectOne(String cql, Class<T> entityClass) {

		Assert.hasText(cql, "Statement must not be empty");
		Assert.notNull(entityClass, "Entity type must not be null");

		return selectOne(new SimpleStatement(cql), entityClass);
	}

	// -------------------------------------------------------------------------
	// Methods dealing with com.datastax.driver.core.Statement
	// -------------------------------------------------------------------------

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.AsyncCassandraOperations#select(com.datastax.driver.core.Statement, java.lang.Class)
	 */
	@Override
	public <T> ListenableFuture<List<T>> select(Statement statement, Class<T> entityClass) {

		Assert.notNull(statement, "Statement must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		return cqlOperations.query(statement, (row, rowNum) -> converter.read(entityClass, row));
	}

	@Override
	public <T> ListenableFuture<Void> select(Statement statement, Consumer<T> entityConsumer, Class<T> entityClass)
			throws DataAccessException {

		Assert.notNull(statement, "Statement must not be null");
		Assert.notNull(entityConsumer, "Entity Consumer must not be empty");
		Assert.notNull(entityClass, "Entity type must not be null");

		return cqlOperations.query(statement, (row) -> {
			entityConsumer.accept(converter.read(entityClass, row));
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.AsyncCassandraOperations#selectOne(com.datastax.driver.core.Statement, java.lang.Class)
	 */
	@Override
	public <T> ListenableFuture<T> selectOne(Statement statement, Class<T> entityClass) {

		return new MappingListenableFutureAdapter<>(select(statement, entityClass), list -> {

			if (list.isEmpty()) {
				return null;
			}
			return list.get(0);

		});
	}

	// -------------------------------------------------------------------------
	// Methods dealing with entities
	// -------------------------------------------------------------------------

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.AsyncCassandraOperations#selectOneById(java.lang.Object, java.lang.Class)
	 */
	@Override
	public <T> ListenableFuture<T> selectOneById(Object id, Class<T> entityClass) {

		Assert.notNull(id, "Id must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		CassandraPersistentEntity<?> entity = getPersistentEntity(entityClass);
		Select select = QueryBuilder.select().all().from(entity.getTableName().toCql());

		converter.write(id, select.where(), entity);

		return selectOne(select, entityClass);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.AsyncCassandraOperations#exists(java.lang.Object, java.lang.Class)
	 */
	@Override
	public ListenableFuture<Boolean> exists(Object id, Class<?> entityClass) {

		Assert.notNull(id, "Id must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		CassandraPersistentEntity<?> entity = getPersistentEntity(entityClass);
		Select select = QueryBuilder.select().from(entity.getTableName().toCql());
		converter.write(id, select.where(), entity);

		return new MappingListenableFutureAdapter<>(cqlOperations.queryForResultSet(select),
				resultSet -> resultSet.iterator().hasNext());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.AsyncCassandraOperations#count(java.lang.Class)
	 */
	@Override
	public ListenableFuture<Long> count(Class<?> entityClass) {

		Assert.notNull(entityClass, "Entity type must not be null");

		Select select = QueryBuilder.select().countAll().from(getPersistentEntity(entityClass).getTableName().toCql());

		return cqlOperations.queryForObject(select, Long.class);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.AsyncCassandraOperations#insert(java.lang.Object)
	 */
	@Override
	public <T> ListenableFuture<T> insert(T entity) {
		return insert(entity, null);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.AsyncCassandraOperations#insert(java.lang.Object, org.springframework.cassandra.core.WriteOptions)
	 */
	@Override
	public <T> ListenableFuture<T> insert(T entity, WriteOptions options) {

		Assert.notNull(entity, "Entity must not be null");

		CqlIdentifier tableName = getTableName(entity);

		Insert insert = QueryUtils.createInsertQuery(tableName.toCql(), entity, options, converter);

		return new MappingListenableFutureAdapter<>(cqlOperations.execute(new AsyncStatementCallback(insert)),
				resultSet -> resultSet.wasApplied() ? entity : null);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.AsyncCassandraOperations#update(java.lang.Object)
	 */
	@Override
	public <T> ListenableFuture<T> update(T entity) {
		return update(entity, null);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.AsyncCassandraOperations#update(java.lang.Object, org.springframework.cassandra.core.WriteOptions)
	 */
	@Override
	public <T> ListenableFuture<T> update(T entity, WriteOptions options) {

		Assert.notNull(entity, "Entity must not be null");

		CqlIdentifier tableName = getTableName(entity);

		Update update = QueryUtils.createUpdateQuery(tableName.toCql(), entity, options, converter);

		return new MappingListenableFutureAdapter<>(cqlOperations.execute(new AsyncStatementCallback(update)),
				resultSet -> resultSet.wasApplied() ? entity : null);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.AsyncCassandraOperations#deleteById(java.lang.Object, java.lang.Class)
	 */
	@Override
	public ListenableFuture<Boolean> deleteById(Object id, Class<?> entityClass) {

		Assert.notNull(id, "Id must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		CassandraPersistentEntity<?> entity = getPersistentEntity(entityClass);
		Delete delete = QueryBuilder.delete().from(entity.getTableName().toCql());

		converter.write(id, delete.where(), entity);

		return cqlOperations.execute(delete);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.AsyncCassandraOperations#delete(java.lang.Object)
	 */
	@Override
	public <T> ListenableFuture<T> delete(T entity) {
		return delete(entity, null);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.AsyncCassandraOperations#delete(java.lang.Object, org.springframework.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> ListenableFuture<T> delete(T entity, QueryOptions options) {

		Assert.notNull(entity, "Entity must not be null");

		CqlIdentifier tableName = getTableName(entity);

		Delete delete = QueryUtils.createDeleteQuery(tableName.toCql(), entity, options, converter);

		return new MappingListenableFutureAdapter<>(cqlOperations.execute(new AsyncStatementCallback(delete)),
				resultSet -> resultSet.wasApplied() ? entity : null);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.AsyncCassandraOperations#truncate(java.lang.Class)
	 */
	@Override
	public ListenableFuture<Void> truncate(Class<?> entityClass) {

		Assert.notNull(entityClass, "Entity type must not be null");
		Truncate truncate = QueryBuilder.truncate(getPersistentEntity(entityClass).getTableName().toCql());

		return new MappingListenableFutureAdapter<>(cqlOperations.execute(truncate), aBoolean -> null);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.AsyncCassandraOperations#getConverter()
	 */
	@Override
	public CassandraConverter getConverter() {
		return converter;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.AsyncCassandraOperations#getAsyncCqlOperations()
	 */
	@Override
	public AsyncCqlOperations getAsyncCqlOperations() {
		return cqlOperations;
	}

	private <T> CassandraPersistentEntity<?> getPersistentEntity(Class<T> entityClass) {

		Assert.notNull(entityClass, "Entity type must not be null");

		CassandraPersistentEntity<?> entity = mappingContext.getPersistentEntity(entityClass);

		if (entity == null) {
			throw new InvalidDataAccessApiUsageException(
					String.format("No Persistent Entity information found for the class [%s]", entityClass.getName()));
		}

		return entity;
	}

	private CqlIdentifier getTableName(Object entity) {
		return getPersistentEntity(ClassUtils.getUserClass(entity)).getTableName();
	}

	private static class MappingListenableFutureAdapter<T, S>
			extends org.springframework.util.concurrent.ListenableFutureAdapter<T, S> {

		private final Function<S, T> mapper;

		public MappingListenableFutureAdapter(ListenableFuture<S> adaptee, Function<S, T> mapper) {
			super(adaptee);
			this.mapper = mapper;
		}

		@Override
		protected T adapt(S adapteeResult) throws ExecutionException {
			return mapper.apply(adapteeResult);
		}
	}

	private class AsyncStatementCallback implements AsyncSessionCallback<ResultSet>, CqlProvider {

		private final Statement statement;

		AsyncStatementCallback(Statement statement) {
			this.statement = statement;
		}

		@Override
		public ListenableFuture<ResultSet> doInSession(Session session) throws DriverException, DataAccessException {
			return new GuavaListenableFutureAdapter<>(session.executeAsync(statement), e -> {

				if (e instanceof DriverException) {
					return exceptionTranslator.translate("AsyncStatementCallback", getCql(), (DriverException) e);
				}

				return exceptionTranslator.translateExceptionIfPossible(e);
			});
		}

		@Override
		public String getCql() {
			return statement.toString();
		}
	}
}
