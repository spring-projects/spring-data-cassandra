/*
 * Copyright 2016-2018 the original author or authors.
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

import lombok.Value;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;

import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.dao.DataAccessException;
import org.springframework.data.cassandra.SessionFactory;
import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.convert.QueryMapper;
import org.springframework.data.cassandra.core.convert.UpdateMapper;
import org.springframework.data.cassandra.core.cql.AsyncCqlOperations;
import org.springframework.data.cassandra.core.cql.AsyncCqlTemplate;
import org.springframework.data.cassandra.core.cql.AsyncSessionCallback;
import org.springframework.data.cassandra.core.cql.CassandraAccessor;
import org.springframework.data.cassandra.core.cql.CqlExceptionTranslator;
import org.springframework.data.cassandra.core.cql.CqlIdentifier;
import org.springframework.data.cassandra.core.cql.CqlProvider;
import org.springframework.data.cassandra.core.cql.GuavaListenableFutureAdapter;
import org.springframework.data.cassandra.core.cql.QueryOptions;
import org.springframework.data.cassandra.core.cql.session.DefaultSessionFactory;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty;
import org.springframework.data.cassandra.core.mapping.event.AfterConvertEvent;
import org.springframework.data.cassandra.core.mapping.event.AfterDeleteEvent;
import org.springframework.data.cassandra.core.mapping.event.AfterLoadEvent;
import org.springframework.data.cassandra.core.mapping.event.AfterSaveEvent;
import org.springframework.data.cassandra.core.mapping.event.BeforeDeleteEvent;
import org.springframework.data.cassandra.core.mapping.event.BeforeSaveEvent;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.domain.Slice;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.lang.Nullable;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.concurrent.ListenableFuture;

import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
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
 * @author John Blum
 * @see org.springframework.data.cassandra.core.AsyncCassandraOperations
 * @since 2.0
 */
public class AsyncCassandraTemplate implements AsyncCassandraOperations, ApplicationEventPublisherAware {

	private final AsyncCqlOperations cqlOperations;

	private final CassandraConverter converter;

	private final MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> mappingContext;

	private final CqlExceptionTranslator exceptionTranslator;

	private final SpelAwareProxyProjectionFactory projectionFactory;

	private final StatementFactory statementFactory;

	private @Nullable ApplicationEventPublisher eventPublisher;

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
		this(new DefaultSessionFactory(session), converter);
	}

	/**
	 * Creates an instance of {@link AsyncCassandraTemplate} initialized with the given {@link SessionFactory} and
	 * {@link CassandraConverter}.
	 *
	 * @param sessionFactory {@link SessionFactory} used to interact with Cassandra; must not be {@literal null}.
	 * @param converter {@link CassandraConverter} used to convert between Java and Cassandra types; must not be
	 *          {@literal null}.
	 * @see CassandraConverter
	 * @see Session
	 */
	public AsyncCassandraTemplate(SessionFactory sessionFactory, CassandraConverter converter) {
		this(new AsyncCqlTemplate(sessionFactory), converter);
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
		this.projectionFactory = new SpelAwareProxyProjectionFactory();
		this.statementFactory = new StatementFactory(new QueryMapper(converter), new UpdateMapper(converter));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.AsyncCassandraOperations#getAsyncCqlOperations()
	 */
	@Override
	public AsyncCqlOperations getAsyncCqlOperations() {
		return this.cqlOperations;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.AsyncCassandraOperations#getConverter()
	 */
	@Override
	public CassandraConverter getConverter() {
		return this.converter;
	}

	/* (non-Javadoc)
	 * @see org.springframework.context.ApplicationEventPublisherAware#setApplicationEventPublisher(org.springframework.context.ApplicationEventPublisher)
	 */
	@Override
	public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
		this.eventPublisher = applicationEventPublisher;
	}

	// -------------------------------------------------------------------------
	// Methods dealing with static CQL
	// -------------------------------------------------------------------------

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.AsyncCassandraOperations#select(java.lang.String, java.lang.Class)
	 */
	@Override
	public <T> ListenableFuture<List<T>> select(String cql, Class<T> entityClass) {

		Assert.hasText(cql, "Statement must not be empty");

		return select(new SimpleStatement(cql), entityClass);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.AsyncCassandraOperations#select(java.lang.String, java.util.function.Consumer, java.lang.Class)
	 */
	@Override
	public <T> ListenableFuture<Void> select(String cql, Consumer<T> entityConsumer, Class<T> entityClass)
			throws DataAccessException {

		Assert.hasText(cql, "Statement must not be empty");
		Assert.notNull(entityConsumer, "Entity Consumer must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		return select(new SimpleStatement(cql), entityConsumer, entityClass);
	}

	/* (non-Javadoc)
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

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.AsyncCassandraOperations#select(com.datastax.driver.core.Statement, java.lang.Class)
	 */
	@Override
	public <T> ListenableFuture<List<T>> select(Statement statement, Class<T> entityClass) {

		Assert.notNull(statement, "Statement must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		Function<Row, T> mapper = getMapper(entityClass, entityClass, QueryUtils.getTableName(statement));

		return getAsyncCqlOperations().query(statement, (row, rowNum) -> mapper.apply(row));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.AsyncCassandraOperations#slice(com.datastax.driver.core.Statement, java.lang.Class)
	 */
	@Override
	public <T> ListenableFuture<Slice<T>> slice(Statement statement, Class<T> entityClass) {

		Assert.notNull(statement, "Statement must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		ListenableFuture<ResultSet> resultSet = getAsyncCqlOperations().queryForResultSet(statement);

		Function<Row, T> mapper = getMapper(entityClass, entityClass, QueryUtils.getTableName(statement));

		return new MappingListenableFutureAdapter<>(resultSet,
				rs -> QueryUtils.readSlice(rs, (row, rowNum) -> mapper.apply(row), 0, getEffectiveFetchSize(statement)));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.AsyncCassandraOperations#select(com.datastax.driver.core.Statement, java.util.function.Consumer, java.lang.Class)
	 */
	@Override
	public <T> ListenableFuture<Void> select(Statement statement, Consumer<T> entityConsumer, Class<T> entityClass)
			throws DataAccessException {

		Assert.notNull(statement, "Statement must not be null");
		Assert.notNull(entityConsumer, "Entity Consumer must not be empty");
		Assert.notNull(entityClass, "Entity type must not be null");

		Function<Row, T> mapper = getMapper(entityClass, entityClass, QueryUtils.getTableName(statement));

		return getAsyncCqlOperations().query(statement, row -> {
			entityConsumer.accept(mapper.apply(row));
		});
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.AsyncCassandraOperations#selectOne(com.datastax.driver.core.Statement, java.lang.Class)
	 */
	@Override
	public <T> ListenableFuture<T> selectOne(Statement statement, Class<T> entityClass) {

		return new MappingListenableFutureAdapter<>(select(statement, entityClass),
				list -> list.stream().findFirst().orElse(null));
	}

	// -------------------------------------------------------------------------
	// Methods dealing with org.springframework.data.cassandra.core.query.Query
	// -------------------------------------------------------------------------

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.AsyncCassandraOperations#select(org.springframework.data.cassandra.core.query.Query, java.lang.Class)
	 */
	@Override
	public <T> ListenableFuture<List<T>> select(Query query, Class<T> entityClass) throws DataAccessException {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		return select(getStatementFactory().select(query, getRequiredPersistentEntity(entityClass)),
				entityClass);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.AsyncCassandraOperations#slice(org.springframework.data.cassandra.core.query.Query, java.lang.Class)
	 */
	@Override
	public <T> ListenableFuture<Slice<T>> slice(Query query, Class<T> entityClass) throws DataAccessException {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		return slice(this.statementFactory.select(query, getRequiredPersistentEntity(entityClass)),
				entityClass);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.AsyncCassandraOperations#select(org.springframework.data.cassandra.core.query.Query, java.util.function.Consumer, java.lang.Class)
	 */
	@Override
	public <T> ListenableFuture<Void> select(Query query, Consumer<T> entityConsumer, Class<T> entityClass)
			throws DataAccessException {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entityConsumer, "Entity Consumer must not be empty");
		Assert.notNull(entityClass, "Entity type must not be null");

		return select(getStatementFactory().select(query, getRequiredPersistentEntity(entityClass)),
				entityConsumer, entityClass);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.AsyncCassandraOperations#selectOne(org.springframework.data.cassandra.core.query.Query, java.lang.Class)
	 */
	@Override
	public <T> ListenableFuture<T> selectOne(Query query, Class<T> entityClass) throws DataAccessException {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		return selectOne(getStatementFactory().select(query, getRequiredPersistentEntity(entityClass)),
				entityClass);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.AsyncCassandraOperations#update(org.springframework.data.cassandra.core.query.Query, org.springframework.data.cassandra.core.query.Update, java.lang.Class)
	 */
	@Override
	public ListenableFuture<Boolean> update(Query query, org.springframework.data.cassandra.core.query.Update update,
			Class<?> entityClass) throws DataAccessException {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(update, "Update must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		return getAsyncCqlOperations().execute(
				getStatementFactory().update(query, update, getRequiredPersistentEntity(entityClass)));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.AsyncCassandraOperations#delete(org.springframework.data.cassandra.core.query.Query, java.lang.Class)
	 */
	@Override
	public ListenableFuture<Boolean> delete(Query query, Class<?> entityClass) throws DataAccessException {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		return doDelete(query, entityClass, getTableName(entityClass));
	}

	private ListenableFuture<Boolean> doDelete(Query query, Class<?> entityClass, CqlIdentifier tableName) {

		RegularStatement delete = getStatementFactory().delete(query, getRequiredPersistentEntity(entityClass), tableName);

		maybeEmitEvent(new BeforeDeleteEvent<>(delete, entityClass, tableName));

		ListenableFuture<Boolean> future = getAsyncCqlOperations()
				.execute(getStatementFactory().delete(query, getRequiredPersistentEntity(entityClass)));

		future.addCallback(success -> maybeEmitEvent(new AfterDeleteEvent<>(delete, entityClass, tableName)), e -> {});

		return future;
	}

	// -------------------------------------------------------------------------
	// Methods dealing with entities
	// -------------------------------------------------------------------------

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.AsyncCassandraOperations#count(java.lang.Class)
	 */
	@Override
	public ListenableFuture<Long> count(Class<?> entityClass) {

		Assert.notNull(entityClass, "Entity type must not be null");

		Select select = QueryBuilder.select().countAll().from(getTableName(entityClass).toCql());

		return getAsyncCqlOperations().queryForObject(select, Long.class);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.AsyncCassandraOperations#count(org.springframework.data.cassandra.core.query.Query, java.lang.Class)
	 */
	@Override
	public ListenableFuture<Long> count(Query query, Class<?> entityClass) throws DataAccessException {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		RegularStatement count = getStatementFactory().count(query, getRequiredPersistentEntity(entityClass));

		ListenableFuture<Long> result = getAsyncCqlOperations().queryForObject(count, Long.class);

		return new MappingListenableFutureAdapter<>(result, it -> it != null ? it : 0L);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.AsyncCassandraOperations#exists(java.lang.Object, java.lang.Class)
	 */
	@Override
	public ListenableFuture<Boolean> exists(Object id, Class<?> entityClass) {

		Assert.notNull(id, "Id must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		CassandraPersistentEntity<?> entity = getRequiredPersistentEntity(entityClass);

		Select select = QueryBuilder.select().from(entity.getTableName().toCql());
		getConverter().write(id, select.where(), entity);

		return new MappingListenableFutureAdapter<>(getAsyncCqlOperations().queryForResultSet(select),
				resultSet -> resultSet.iterator().hasNext());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.AsyncCassandraOperations#exists(org.springframework.data.cassandra.core.query.Query, java.lang.Class)
	 */
	@Override
	public ListenableFuture<Boolean> exists(Query query, Class<?> entityClass) throws DataAccessException {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		RegularStatement select = getStatementFactory().select(query.limit(1), getRequiredPersistentEntity(entityClass));

		return new MappingListenableFutureAdapter<>(getAsyncCqlOperations().queryForResultSet(select),
				resultSet -> resultSet.iterator().hasNext());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.AsyncCassandraOperations#selectOneById(java.lang.Object, java.lang.Class)
	 */
	@Override
	public <T> ListenableFuture<T> selectOneById(Object id, Class<T> entityClass) {

		Assert.notNull(id, "Id must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		CassandraPersistentEntity<?> entity = getRequiredPersistentEntity(entityClass);

		Select select = QueryBuilder.select().all().from(entity.getTableName().toCql());
		getConverter().write(id, select.where(), entity);

		Function<Row, T> mapper = getMapper(entityClass, entityClass, entity.getTableName());

		return new MappingListenableFutureAdapter<>(
				getAsyncCqlOperations().query(select, (row, rowNum) -> mapper.apply(row)),
						it -> it.isEmpty() ? null : (T) it.get(0));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.AsyncCassandraOperations#insert(java.lang.Object)
	 */
	@Override
	public <T> ListenableFuture<T> insert(T entity) {
		return new MappingListenableFutureAdapter<>(insert(entity, InsertOptions.empty()), writeResult -> entity);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.AsyncCassandraOperations#insert(java.lang.Object, org.springframework.data.cassandra.core.InsertOptions)
	 */
	@Override
	public ListenableFuture<WriteResult> insert(Object entity, InsertOptions options) {

		Assert.notNull(entity, "Entity must not be null");
		Assert.notNull(options, "InsertOptions must not be null");

		CqlIdentifier tableName = getTableName(entity);
		Insert insert = QueryUtils.createInsertQuery(tableName.toCql(), entity, options, getConverter());

		maybeEmitEvent(new BeforeSaveEvent<>(entity, tableName, insert));

		return new MappingListenableFutureAdapter<>(getAsyncCqlOperations().execute(new AsyncStatementCallback(insert)),
				resultSet -> {
					maybeEmitEvent(new AfterSaveEvent<>(entity, tableName));
					return WriteResult.of(resultSet);
				});
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.AsyncCassandraOperations#update(java.lang.Object)
	 */
	@Override
	public <T> ListenableFuture<T> update(T entity) {
		return new MappingListenableFutureAdapter<>(update(entity, UpdateOptions.empty()), writeResult -> entity);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.AsyncCassandraOperations#update(java.lang.Object, org.springframework.data.cassandra.core.UpdateOptions)
	 */
	@Override
	public ListenableFuture<WriteResult> update(Object entity, UpdateOptions options) {

		Assert.notNull(entity, "Entity must not be null");
		Assert.notNull(options, "UpdateOptions must not be null");

		CqlIdentifier tableName = getTableName(entity);
		Update update = QueryUtils.createUpdateQuery(tableName.toCql(), entity, options, getConverter());

		maybeEmitEvent(new BeforeSaveEvent<>(entity, tableName, update));

		return new MappingListenableFutureAdapter<>(getAsyncCqlOperations().execute(new AsyncStatementCallback(update)),
				resultSet -> {
					maybeEmitEvent(new AfterSaveEvent<>(entity, tableName));
					return WriteResult.of(resultSet);
				});
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.AsyncCassandraOperations#delete(java.lang.Object)
	 */
	@Override
	public <T> ListenableFuture<T> delete(T entity) {
		return new MappingListenableFutureAdapter<>(delete(entity, QueryOptions.empty()), writeResult -> entity);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.AsyncCassandraOperations#delete(java.lang.Object, org.springframework.data.cassandra.core.cql.QueryOptions)
	 */
	@Override
	public ListenableFuture<WriteResult> delete(Object entity, QueryOptions options) {

		Assert.notNull(entity, "Entity must not be null");
		Assert.notNull(options, "QueryOptions must not be null");

		CqlIdentifier tableName = getTableName(entity);
		Delete delete = QueryUtils.createDeleteQuery(tableName.toCql(), entity, options, getConverter());

		maybeEmitEvent(new BeforeDeleteEvent<>(delete, entity.getClass(), tableName));

		return new MappingListenableFutureAdapter<>(getAsyncCqlOperations().execute(new AsyncStatementCallback(delete)),
				resultSet -> {
					maybeEmitEvent(new AfterDeleteEvent<>(delete, entity.getClass(), tableName));
					return WriteResult.of(resultSet);
				});
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.AsyncCassandraOperations#deleteById(java.lang.Object, java.lang.Class)
	 */
	@Override
	public ListenableFuture<Boolean> deleteById(Object id, Class<?> entityClass) {

		Assert.notNull(id, "Id must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		CassandraPersistentEntity<?> entity = getRequiredPersistentEntity(entityClass);

		CqlIdentifier tableName = entity.getTableName();
		Delete delete = QueryBuilder.delete().from(tableName.toCql());
		getConverter().write(id, delete.where(), entity);

		maybeEmitEvent(new BeforeDeleteEvent<>(delete, entityClass, tableName));

		ListenableFuture<Boolean> future = getAsyncCqlOperations().execute(delete);
		future.addCallback(success -> maybeEmitEvent(new AfterDeleteEvent<>(delete, entityClass, tableName)), e -> {});

		return future;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.AsyncCassandraOperations#truncate(java.lang.Class)
	 */
	@Override
	public ListenableFuture<Void> truncate(Class<?> entityClass) {

		Assert.notNull(entityClass, "Entity type must not be null");

		CqlIdentifier tableName = getTableName(entityClass);
		Truncate truncate = QueryBuilder.truncate(tableName.toCql());

		maybeEmitEvent(new BeforeDeleteEvent<>(truncate, entityClass, tableName));

		ListenableFuture<Boolean> future = getAsyncCqlOperations().execute(truncate);
		future.addCallback(success -> maybeEmitEvent(new AfterDeleteEvent<>(truncate, entityClass, tableName)), e -> {});

		return new MappingListenableFutureAdapter<>(future, aBoolean -> null);
	}

	// -------------------------------------------------------------------------
	// Implementation hooks and helper methods
	// -------------------------------------------------------------------------

	/**
	 * Returns the {@link CassandraMappingContext} used by this template to access mapping meta-data in order to store
	 * (map) object to Cassandra tables.
	 *
	 * @return the {@link CassandraMappingContext} used by this template.
	 * @see org.springframework.data.cassandra.core.mapping.CassandraMappingContext
	 */
	protected MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> getMappingContext() {
		return this.mappingContext;
	}

	/**
	 * Returns a reference to the configured {@link ProjectionFactory} used by this template to process CQL query
	 * projections.
	 *
	 * @return a reference to the configured {@link ProjectionFactory} used by this template to process CQL query
	 *         projections.
	 * @see org.springframework.data.projection.SpelAwareProxyProjectionFactory
	 * @since 2.1
	 */
	protected SpelAwareProxyProjectionFactory getProjectionFactory() {
		return this.projectionFactory;
	}

	/**
	 * Returns the {@link StatementFactory} used by this template to construct and run Cassandra CQL statements.
	 *
	 * @return the {@link StatementFactory} used by this template to construct and run Cassandra CQL statements.
	 * @see org.springframework.data.cassandra.core.StatementFactory
	 * @since 2.1
	 */
	protected StatementFactory getStatementFactory() {
		return this.statementFactory;
	}

	private CqlIdentifier getTableName(Class<?> entityClass) {
		return getRequiredPersistentEntity(entityClass).getTableName();
	}

	private CqlIdentifier getTableName(Object entity) {
		return getRequiredPersistentEntity(entity.getClass()).getTableName();
	}

	private CassandraPersistentEntity<?> getRequiredPersistentEntity(Class<?> entityType) {
		return getMappingContext().getRequiredPersistentEntity(ClassUtils.getUserClass(entityType));
	}

	private int getConfiguredFetchSize(Session session) {
		return session.getCluster().getConfiguration().getQueryOptions().getFetchSize();
	}

	@SuppressWarnings("ConstantConditions")
	private int getEffectiveFetchSize(Statement statement) {

		if (statement.getFetchSize() > 0) {
			return statement.getFetchSize();
		}

		if (getAsyncCqlOperations() instanceof CassandraAccessor) {

			CassandraAccessor accessor = (CassandraAccessor) getAsyncCqlOperations();

			if (accessor.getFetchSize() != -1) {
				return accessor.getFetchSize();
			}
		}

		return getAsyncCqlOperations()
				.execute((AsyncSessionCallback<Integer>) session -> AsyncResult.forValue(getConfiguredFetchSize(session)))
				.completable().join();
	}

	@SuppressWarnings("unchecked")
	private <T> Function<Row, T> getMapper(Class<?> entityType, Class<T> targetType, CqlIdentifier tableName) {

		Class<?> typeToRead = resolveTypeToRead(entityType, targetType);

		return row -> {

			maybeEmitEvent(new AfterLoadEvent(row, targetType, tableName));

			Object source = getConverter().read(typeToRead, row);

			T result = (T) (targetType.isInterface() ? getProjectionFactory().createProjection(targetType, source) : source);

			if (result != null) {
				maybeEmitEvent(new AfterConvertEvent<>(row, result, tableName));
			}

			return result;
		};
	}

	private Class<?> resolveTypeToRead(Class<?> entityType, Class<?> targetType) {
		return targetType.isInterface() || targetType.isAssignableFrom(entityType) ? entityType : targetType;
	}

	private void maybeEmitEvent(ApplicationEvent event) {

		if (eventPublisher != null) {
			eventPublisher.publishEvent(event);
		}
	}

	private static MappingCassandraConverter newConverter() {

		MappingCassandraConverter converter = new MappingCassandraConverter();

		converter.afterPropertiesSet();

		return converter;
	}

	static class MappingListenableFutureAdapter<T, S>
			extends org.springframework.util.concurrent.ListenableFutureAdapter<T, S> {

		private final Function<S, T> mapper;

		MappingListenableFutureAdapter(ListenableFuture<S> adaptee, Function<S, T> mapper) {
			super(adaptee);
			this.mapper = mapper;
		}

		/* (non-Javadoc)
		 * @see org.springframework.util.concurrent.FutureAdapter#adapt(java.lang.Object)
		 */
		@Override
		protected T adapt(@Nullable S adapteeResult) throws ExecutionException {
			return mapper.apply(adapteeResult);
		}
	}

	@Value
	class AsyncStatementCallback implements AsyncSessionCallback<ResultSet>, CqlProvider {

		@lombok.NonNull Statement statement;

		AsyncStatementCallback(Statement statement) {
			this.statement = statement;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.AsyncSessionCallback#doInSession(com.datastax.driver.core.Session)
		 */
		@Override
		public ListenableFuture<ResultSet> doInSession(Session session) throws DriverException, DataAccessException {
			return new GuavaListenableFutureAdapter<>(session.executeAsync(statement),
					e -> (e instanceof DriverException
							? exceptionTranslator.translate("AsyncStatementCallback", getCql(), (DriverException) e)
							: exceptionTranslator.translateExceptionIfPossible(e)));
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.CqlProvider#getCql()
		 */
		@Override
		public String getCql() {
			return statement.toString();
		}
	}
}
