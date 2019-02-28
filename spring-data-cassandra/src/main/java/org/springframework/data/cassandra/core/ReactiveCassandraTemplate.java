/*
 * Copyright 2016-2019 the original author or authors.
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

import java.util.Collections;
import java.util.function.BiConsumer;
import java.util.function.Function;

import lombok.Value;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;

import org.reactivestreams.Publisher;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.cassandra.ReactiveResultSet;
import org.springframework.data.cassandra.ReactiveSession;
import org.springframework.data.cassandra.ReactiveSessionFactory;
import org.springframework.data.cassandra.core.EntityOperations.AdaptibleEntity;
import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.convert.QueryMapper;
import org.springframework.data.cassandra.core.convert.UpdateMapper;
import org.springframework.data.cassandra.core.cql.CassandraAccessor;
import org.springframework.data.cassandra.core.cql.CqlIdentifier;
import org.springframework.data.cassandra.core.cql.CqlProvider;
import org.springframework.data.cassandra.core.cql.QueryOptions;
import org.springframework.data.cassandra.core.cql.ReactiveCqlOperations;
import org.springframework.data.cassandra.core.cql.ReactiveCqlTemplate;
import org.springframework.data.cassandra.core.cql.ReactiveSessionCallback;
import org.springframework.data.cassandra.core.cql.RowMapper;
import org.springframework.data.cassandra.core.cql.WriteOptions;
import org.springframework.data.cassandra.core.cql.session.DefaultReactiveSessionFactory;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.event.AfterConvertEvent;
import org.springframework.data.cassandra.core.mapping.event.AfterDeleteEvent;
import org.springframework.data.cassandra.core.mapping.event.AfterLoadEvent;
import org.springframework.data.cassandra.core.mapping.event.AfterSaveEvent;
import org.springframework.data.cassandra.core.mapping.event.BeforeDeleteEvent;
import org.springframework.data.cassandra.core.mapping.event.BeforeSaveEvent;
import org.springframework.data.cassandra.core.query.Columns;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.SliceImpl;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.datastax.driver.core.RegularStatement;
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
 * Primary implementation of {@link ReactiveCassandraOperations}. It simplifies the use of Reactive Cassandra usage and
 * helps to avoid common errors. It executes core Cassandra workflow. This class executes CQL queries or updates,
 * initiating iteration over {@link ReactiveResultSet} and catching Cassandra exceptions and translating them to the
 * generic, more informative exception hierarchy defined in the {@code org.springframework.dao} package.
 * <p>
 * Can be used within a service implementation via direct instantiation with a {@link ReactiveSessionFactory} reference,
 * or get prepared in an application context and given to services as bean reference.
 * <p>
 * Note: The {@link ReactiveSessionFactory} should always be configured as a bean in the application context, in the
 * first case given to the service directly, in the second case to the prepared template.
 *
 * @author Mark Paluch
 * @author John Blum
 * @author Lukasz Antoniak
 * @author Hleb Albau
 * @since 2.0
 */
public class ReactiveCassandraTemplate implements ReactiveCassandraOperations, ApplicationEventPublisherAware {

	private @Nullable ApplicationEventPublisher eventPublisher;

	private final CassandraConverter converter;

	private final EntityOperations entityOperations;

	private final ReactiveCqlOperations cqlOperations;

	private final SpelAwareProxyProjectionFactory projectionFactory;

	private final StatementFactory statementFactory;

	/**
	 * Creates an instance of {@link ReactiveCassandraTemplate} initialized with the given {@link ReactiveSession} and a
	 * default {@link MappingCassandraConverter}.
	 *
	 * @param session {@link ReactiveSession} used to interact with Cassandra; must not be {@literal null}.
	 * @see CassandraConverter
	 * @see Session
	 */
	public ReactiveCassandraTemplate(ReactiveSession session) {
		this(session, newConverter());
	}

	/**
	 * Create an instance of {@link CassandraTemplate} initialized with the given {@link ReactiveSession} and
	 * {@link CassandraConverter}.
	 *
	 * @param session {@link ReactiveSession} used to interact with Cassandra; must not be {@literal null}.
	 * @param converter {@link CassandraConverter} used to convert between Java and Cassandra types; must not be
	 *          {@literal null}.
	 * @see org.springframework.data.cassandra.core.convert.CassandraConverter
	 * @see com.datastax.driver.core.Session
	 */
	public ReactiveCassandraTemplate(ReactiveSession session, CassandraConverter converter) {
		this(new DefaultReactiveSessionFactory(session), converter);
	}

	/**
	 * Create an instance of {@link ReactiveCassandraTemplate} initialized with the given {@link ReactiveSessionFactory}
	 * and {@link CassandraConverter}.
	 *
	 * @param sessionFactory {@link ReactiveSessionFactory} used to interact with Cassandra; must not be {@literal null}.
	 * @param converter {@link CassandraConverter} used to convert between Java and Cassandra types; must not be
	 *          {@literal null}.
	 * @see org.springframework.data.cassandra.core.convert.CassandraConverter
	 * @see com.datastax.driver.core.Session
	 */
	public ReactiveCassandraTemplate(ReactiveSessionFactory sessionFactory, CassandraConverter converter) {
		this(new ReactiveCqlTemplate(sessionFactory), converter);
	}

	/**
	 * Create an instance of {@link ReactiveCassandraTemplate} initialized with the given {@link ReactiveCqlOperations}
	 * and {@link CassandraConverter}.
	 *
	 * @param reactiveCqlOperations {@link ReactiveCqlOperations} used to interact with Cassandra; must not be
	 *          {@literal null}.
	 * @param converter {@link CassandraConverter} used to convert between Java and Cassandra types; must not be
	 *          {@literal null}.
	 * @see org.springframework.data.cassandra.core.convert.CassandraConverter
	 * @see com.datastax.driver.core.Session
	 */
	public ReactiveCassandraTemplate(ReactiveCqlOperations reactiveCqlOperations, CassandraConverter converter) {

		Assert.notNull(reactiveCqlOperations, "ReactiveCqlOperations must not be null");
		Assert.notNull(converter, "CassandraConverter must not be null");

		this.converter = converter;
		this.cqlOperations = reactiveCqlOperations;
		this.entityOperations = new EntityOperations(converter.getMappingContext());
		this.projectionFactory = new SpelAwareProxyProjectionFactory();
		this.statementFactory = new StatementFactory(new QueryMapper(converter), new UpdateMapper(converter));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#batchOps()
	 */
	@Override
	public ReactiveCassandraBatchOperations batchOps() {
		return new ReactiveCassandraBatchTemplate(this);
	}

	/* (non-Javadoc)
	 * @see org.springframework.context.ApplicationEventPublisherAware#setApplicationEventPublisher(org.springframework.context.ApplicationEventPublisher)
	 */
	@Override
	public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
		this.eventPublisher = applicationEventPublisher;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#getConverter()
	 */
	@Override
	public CassandraConverter getConverter() {
		return this.converter;
	}

	/**
	 * Returns the {@link EntityOperations} used to perform data access operations on an entity
	 * inside a Cassandra data source.
	 *
	 * @return the configured {@link EntityOperations} for this template.
	 * @see org.springframework.data.cassandra.core.EntityOperations
	 */
	protected EntityOperations getEntityOperations() {
		return this.entityOperations;
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

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#getReactiveCqlOperations()
	 */
	@Override
	public ReactiveCqlOperations getReactiveCqlOperations() {
		return this.cqlOperations;
	}

	private CassandraPersistentEntity<?> getRequiredPersistentEntity(Class<?> entityType) {
		return getEntityOperations().getRequiredPersistentEntity(entityType);
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

	CqlIdentifier getTableName(Class<?> entityClass) {
		return getEntityOperations().getTableName(entityClass);
	}

	// -------------------------------------------------------------------------
	// Methods dealing with static CQL
	// -------------------------------------------------------------------------

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#select(java.lang.String, java.lang.Class)
	 */
	@Override
	public <T> Flux<T> select(String cql, Class<T> entityClass) {

		Assert.hasText(cql, "CQL must not be empty");

		return select(new SimpleStatement(cql), entityClass);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#selectOne(java.lang.String, java.lang.Class)
	 */
	@Override
	public <T> Mono<T> selectOne(String cql, Class<T> entityClass) {
		return select(cql, entityClass).next();
	}

	// -------------------------------------------------------------------------
	// Methods dealing with com.datastax.driver.core.Statement
	// -------------------------------------------------------------------------

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#select(com.datastax.driver.core.Statement, java.lang.Class)
	 */
	@Override
	public <T> Flux<T> select(Statement statement, Class<T> entityClass) {

		Assert.notNull(statement, "Statement must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		Function<Row, T> mapper = getMapper(entityClass, entityClass, EntityQueryUtils.getTableName(statement));

		return getReactiveCqlOperations().query(statement, (row, rowNum) -> mapper.apply(row));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#selectOne(com.datastax.driver.core.Statement, java.lang.Class)
	 */
	@Override
	public <T> Mono<T> selectOne(Statement statement, Class<T> entityClass) {
		return select(statement, entityClass).next();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#slice(com.datastax.driver.core.Statement, java.lang.Class)
	 */
	@Override
	public <T> Mono<Slice<T>> slice(Statement statement, Class<T> entityClass) {

		Assert.notNull(statement, "Statement must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		Mono<ReactiveResultSet> resultSetMono = getReactiveCqlOperations().queryForResultSet(statement);
		Mono<Integer> effectiveFetchSizeMono = getEffectiveFetchSize(statement);
		RowMapper<T> rowMapper = (row, i) -> getConverter().read(entityClass, row);

		return resultSetMono.zipWith(effectiveFetchSizeMono)
				.flatMap(tuple -> {

					ReactiveResultSet resultSet = tuple.getT1();
					Integer effectiveFetchSize = tuple.getT2();

					return resultSet.availableRows().collectList().map(it ->
							EntityQueryUtils.readSlice(it, resultSet.getExecutionInfo().getPagingState(), rowMapper,
								1, effectiveFetchSize));

				}).defaultIfEmpty(new SliceImpl<>(Collections.emptyList()));
	}

	// -------------------------------------------------------------------------
	// Methods dealing with org.springframework.data.cassandra.core.query.Query
	// -------------------------------------------------------------------------

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#select(org.springframework.data.cassandra.core.query.Query, java.lang.Class)
	 */
	@Override
	public <T> Flux<T> select(Query query, Class<T> entityClass) throws DataAccessException {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		return doSelect(query, entityClass, getTableName(entityClass), entityClass);
	}

	<T> Flux<T> doSelect(Query query, Class<?> entityClass, CqlIdentifier tableName, Class<T> returnType) {

		CassandraPersistentEntity<?> persistentEntity = getRequiredPersistentEntity(entityClass);

		Columns columns = getStatementFactory()
				.computeColumnsForProjection(query.getColumns(), persistentEntity, returnType);

		Query queryToUse = query.columns(columns);

		RegularStatement select = getStatementFactory().select(queryToUse, persistentEntity, tableName);

		Function<Row, T> mapper = getMapper(entityClass, returnType, tableName);

		return getReactiveCqlOperations().query(select, (row, rowNum) -> mapper.apply(row));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#selectOne(org.springframework.data.cassandra.core.query.Query, java.lang.Class)
	 */
	@Override
	public <T> Mono<T> selectOne(Query query, Class<T> entityClass) throws DataAccessException {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		return select(query, entityClass).next();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#slice(org.springframework.data.cassandra.core.query.Query, java.lang.Class)
	 */
	@Override
	public <T> Mono<Slice<T>> slice(Query query, Class<T> entityClass) throws DataAccessException {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		RegularStatement select = getStatementFactory().select(query, getRequiredPersistentEntity(entityClass));

		return slice(select, entityClass);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#update(org.springframework.data.cassandra.core.query.Query, org.springframework.data.cassandra.core.query.Update, java.lang.Class)
	 */
	@Override
	public Mono<Boolean> update(Query query, org.springframework.data.cassandra.core.query.Update update,
			Class<?> entityClass) throws DataAccessException {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(update, "Update must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		return doUpdate(query, update, entityClass, getTableName(entityClass)).map(WriteResult::wasApplied);
	}

	Mono<WriteResult> doUpdate(Query query, org.springframework.data.cassandra.core.query.Update update,
			Class<?> entityClass, CqlIdentifier tableName) {

		RegularStatement statement = getStatementFactory()
				.update(query, update, getRequiredPersistentEntity(entityClass), tableName);

		return getReactiveCqlOperations().execute(new StatementCallback(statement)).next();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#delete(org.springframework.data.cassandra.core.query.Query, java.lang.Class)
	 */
	@Override
	public Mono<Boolean> delete(Query query, Class<?> entityClass) throws DataAccessException {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		return doDelete(query, entityClass, getTableName(entityClass)).map(WriteResult::wasApplied);
	}

	Mono<WriteResult> doDelete(Query query, Class<?> entityClass, CqlIdentifier tableName) {

		RegularStatement delete = getStatementFactory()
				.delete(query, getRequiredPersistentEntity(entityClass), tableName);

		Mono<WriteResult> writeResult = getReactiveCqlOperations().execute(new StatementCallback(delete))
				.doOnSubscribe(it -> maybeEmitEvent(new BeforeDeleteEvent<>(delete, entityClass, tableName))).next();

		return writeResult.doOnNext(it -> maybeEmitEvent(new AfterDeleteEvent<>(delete, entityClass, tableName)));
	}

	// -------------------------------------------------------------------------
	// Methods dealing with entities
	// -------------------------------------------------------------------------

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#count(java.lang.Class)
	 */
	@Override
	public Mono<Long> count(Class<?> entityClass) {

		Assert.notNull(entityClass, "Entity type must not be null");

		Select select = QueryBuilder.select().countAll().from(getTableName(entityClass).toCql());

		return getReactiveCqlOperations().queryForObject(select, Long.class);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#count(org.springframework.data.cassandra.core.query.Query, java.lang.Class)
	 */
	@Override
	public Mono<Long> count(Query query, Class<?> entityClass) throws DataAccessException {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		return doCount(query, entityClass, getTableName(entityClass));
	}

	Mono<Long> doCount(Query query, Class<?> entityClass, CqlIdentifier tableName) {

		RegularStatement count = getStatementFactory().count(query, getRequiredPersistentEntity(entityClass), tableName);

		return getReactiveCqlOperations().queryForObject(count, Long.class).switchIfEmpty(Mono.just(0L));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#exists(java.lang.Object, java.lang.Class)
	 */
	@Override
	public Mono<Boolean> exists(Object id, Class<?> entityClass) {

		Assert.notNull(id, "Id must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		CassandraPersistentEntity<?> entity = getRequiredPersistentEntity(entityClass);

		Select select = QueryBuilder.select().from(entity.getTableName().toCql());

		getConverter().write(id, select.where(), entity);

		return getReactiveCqlOperations().queryForRows(select).hasElements();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#exists(org.springframework.data.cassandra.core.query.Query, java.lang.Class)
	 */
	@Override
	public Mono<Boolean> exists(Query query, Class<?> entityClass) throws DataAccessException {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		return doExists(query, entityClass, getTableName(entityClass));
	}

	Mono<Boolean> doExists(Query query, Class<?> entityClass, CqlIdentifier tableName) {

		RegularStatement select = getStatementFactory()
				.select(query.limit(1), getRequiredPersistentEntity(entityClass), tableName);

		return getReactiveCqlOperations().queryForRows(select).hasElements();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#selectOneById(java.lang.Object, java.lang.Class)
	 */
	@Override
	public <T> Mono<T> selectOneById(Object id, Class<T> entityClass) {

		Assert.notNull(id, "Id must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		CassandraPersistentEntity<?> entity = getRequiredPersistentEntity(entityClass);

		Select select = QueryBuilder.select().all().from(entity.getTableName().toCql());

		getConverter().write(id, select.where(), entity);

		return selectOne(select, entityClass);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#insert(java.lang.Object)
	 */
	@Override
	public <T> Mono<T> insert(T entity) {
		return insert(entity, InsertOptions.empty()).map(EntityWriteResult::getEntity);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#insert(java.lang.Object, org.springframework.data.cassandra.core.InsertOptions)
	 */
	@Override
	public <T> Mono<EntityWriteResult<T>> insert(T entity, InsertOptions options) {

		Assert.notNull(entity, "Entity must not be null");
		Assert.notNull(options, "InsertOptions must not be null");

		return doInsert(entity, options, getTableName(entity.getClass()));
	}

	<T> Mono<EntityWriteResult<T>> doInsert(T entity, WriteOptions options, CqlIdentifier tableName) {

		AdaptibleEntity<T> source = this.entityOperations.forEntity(entity, getConverter().getConversionService());
		CassandraPersistentEntity<?> persistentEntity = getRequiredPersistentEntity(entity.getClass());

		T entityToUse = source.isVersionedEntity() ? source.initializeVersionProperty() : entity;

		Insert insert = EntityQueryUtils.createInsertQuery(tableName.toCql(), entityToUse, options, getConverter(),
				persistentEntity);

		return source.isVersionedEntity()
				? doInsertVersioned(insert.ifNotExists(), entityToUse, source, tableName)
				: doInsert(insert, entityToUse, tableName);
	}

	private <T> Mono<EntityWriteResult<T>> doInsertVersioned(Insert insert, T entity, AdaptibleEntity<T> source,
			CqlIdentifier tableName) {

		return executeSave(entity, tableName, insert, (result, sink) -> {

			if (!result.wasApplied()) {

				sink.error(new OptimisticLockingFailureException(
						String.format("Cannot insert entity %s with version %s into table %s as it already exists",
								entity, source.getVersion(), tableName)));

				return;
			}

			sink.next(result);
		});
	}

	private <T> Mono<EntityWriteResult<T>> doInsert(Insert insert, T entity, CqlIdentifier tableName) {
		return executeSave(entity, tableName, insert);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#update(java.lang.Object)
	 */
	@Override
	public <T> Mono<T> update(T entity) {
		return update(entity, UpdateOptions.empty()).map(EntityWriteResult::getEntity);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#update(java.lang.Object, org.springframework.data.cassandra.core.UpdateOptions)
	 */
	@Override
	public <T> Mono<EntityWriteResult<T>> update(T entity, UpdateOptions options) {

		Assert.notNull(entity, "Entity must not be null");
		Assert.notNull(options, "UpdateOptions must not be null");

		AdaptibleEntity<T> source = this.entityOperations.forEntity(entity, getConverter().getConversionService());
		CassandraPersistentEntity<?> persistentEntity = getRequiredPersistentEntity(entity.getClass());
		CqlIdentifier tableName = persistentEntity.getTableName();

		return source.isVersionedEntity()
				? doUpdateVersioned(source, options, tableName, persistentEntity)
				: doUpdate(entity, options, tableName, persistentEntity);
	}

	private <T> Mono<EntityWriteResult<T>> doUpdateVersioned(AdaptibleEntity<T> source, UpdateOptions options,
			CqlIdentifier tableName, CassandraPersistentEntity<?> persistentEntity) {

		Number previousVersion = source.getVersion();

		T entity = source.incrementVersion();

		Update update = getStatementFactory().update(entity, options, getConverter(), persistentEntity, tableName);

		return executeSave(entity, tableName, source.appendVersionCondition(update, previousVersion), (result, sink) -> {

			if (!result.wasApplied()) {

				sink.error(new OptimisticLockingFailureException(
						String.format("Cannot save entity %s with version %s to table %s. Has it been modified meanwhile?",
								entity, source.getVersion(), tableName)));

				return;
			}

			sink.next(result);
		});
	}

	private <T> Mono<EntityWriteResult<T>> doUpdate(T entity, UpdateOptions options, CqlIdentifier tableName,
			CassandraPersistentEntity<?> persistentEntity) {

		Update update = getStatementFactory().update(entity, options, getConverter(), persistentEntity, tableName);

		return executeSave(entity, tableName, update);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#delete(java.lang.Object)
	 */
	@Override
	public <T> Mono<T> delete(T entity) {
		return delete(entity, QueryOptions.empty()).map(reactiveWriteResult -> entity);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#delete(java.lang.Object, org.springframework.data.cql.core.QueryOptions)
	 */
	@Override
	public Mono<WriteResult> delete(Object entity, QueryOptions options) {

		Assert.notNull(entity, "Entity must not be null");
		Assert.notNull(options, "QueryOptions must not be null");

		AdaptibleEntity<Object> source = this.entityOperations.forEntity(entity, getConverter().getConversionService());
		CassandraPersistentEntity<?> persistentEntity = getRequiredPersistentEntity(entity.getClass());
		CqlIdentifier tableName = persistentEntity.getTableName();

		Delete delete = getStatementFactory().delete(entity, options, getConverter(), persistentEntity, tableName);

		return source.isVersionedEntity()
				? doDeleteVersioned(delete, entity, source, tableName)
				: doDelete(delete, entity, tableName);
	}

	private Mono<WriteResult> doDeleteVersioned(Delete delete, Object entity, AdaptibleEntity<Object> source,
			CqlIdentifier tableName) {

		return executeDelete(entity, tableName, source.appendVersionCondition(delete), (result, sink) -> {

			if (!result.wasApplied()) {

				sink.error(new OptimisticLockingFailureException(
						String.format("Cannot delete entity %s with version %s in table %s. Has it been modified meanwhile?",
								entity, source.getVersion(), tableName)));

				return;
			}

			sink.next(result);
		});
	}

	private Mono<WriteResult> doDelete(Delete delete, Object entity, CqlIdentifier tableName) {
		return executeDelete(entity, tableName, delete, (result, sink) -> sink.next(result));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#deleteById(java.lang.Object, java.lang.Class)
	 */
	@Override
	public Mono<Boolean> deleteById(Object id, Class<?> entityClass) {

		Assert.notNull(id, "Id must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		CassandraPersistentEntity<?> entity = getRequiredPersistentEntity(entityClass);
		CqlIdentifier tableName = entity.getTableName();
		Delete delete = QueryBuilder.delete().from(tableName.toCql());

		getConverter().write(id, delete.where(), entity);

		Mono<Boolean> result = getReactiveCqlOperations().execute(delete)
				.doOnSubscribe(it -> maybeEmitEvent(new BeforeDeleteEvent<>(delete, entityClass, tableName)));

		return result.doOnNext(it -> maybeEmitEvent(new AfterDeleteEvent<>(delete, entityClass, tableName)));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#truncate(java.lang.Class)
	 */
	@Override
	public Mono<Void> truncate(Class<?> entityClass) {

		Assert.notNull(entityClass, "Entity type must not be null");

		CqlIdentifier tableName = getTableName(entityClass);
		Truncate truncate = QueryBuilder.truncate(tableName.toCql());

		Mono<Boolean> result = getReactiveCqlOperations().execute(truncate)
				.doOnSubscribe(it -> maybeEmitEvent(new BeforeDeleteEvent<>(truncate, entityClass, tableName)));

		return result.doOnNext(it -> maybeEmitEvent(new AfterDeleteEvent<>(truncate, entityClass, tableName))).then();
	}

	// -------------------------------------------------------------------------
	// Fluent API entry points
	// -------------------------------------------------------------------------

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveDeleteOperation#remove(java.lang.Class)
	 */
	@Override
	public ReactiveDelete delete(Class<?> domainType) {
		return new ReactiveDeleteOperationSupport(this).delete(domainType);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveInsertOperation#insert(java.lang.Class)
	 */
	@Override
	public <T> ReactiveInsert<T> insert(Class<T> domainType) {
		return new ReactiveInsertOperationSupport(this).insert(domainType);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveSelectOperation#query(java.lang.Class)
	 */
	@Override
	public <T> ReactiveSelect<T> query(Class<T> domainType) {
		return new ReactiveSelectOperationSupport(this).query(domainType);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveUpdateOperation#update(java.lang.Class)
	 */
	@Override
	public ReactiveUpdate update(Class<?> domainType) {
		return new ReactiveUpdateOperationSupport(this).update(domainType);
	}

	// -------------------------------------------------------------------------
	// Implementation hooks and utility methods
	// -------------------------------------------------------------------------

	private <T> Mono<EntityWriteResult<T>> executeSave(T entity, CqlIdentifier tableName, Statement statement) {
		return executeSave(entity, tableName, statement, (writeResult, sink) -> sink.next(writeResult));
	}

	private <T> Mono<EntityWriteResult<T>> executeSave(T entity, CqlIdentifier tableName, Statement statement,
			BiConsumer<EntityWriteResult<T>, SynchronousSink<EntityWriteResult<T>>> handler) {

		maybeEmitEvent(new BeforeSaveEvent<>(entity, tableName, statement));

		Flux<WriteResult> execute = getReactiveCqlOperations().execute(new StatementCallback(statement));

		return execute.map(it -> EntityWriteResult.of(it, entity)).handle(handler) //
				.doOnSubscribe(it -> maybeEmitEvent(new BeforeSaveEvent<>(entity, tableName, statement))) //
				.doOnNext(it -> maybeEmitEvent(new AfterSaveEvent<>(it, tableName))) //
				.next();
	}

	private Mono<WriteResult> executeDelete(Object entity, CqlIdentifier tableName, Statement statement,
			BiConsumer<WriteResult, SynchronousSink<WriteResult>> handler) {

		maybeEmitEvent(new BeforeDeleteEvent<>(statement, entity.getClass(), tableName));

		Flux<WriteResult> execute = getReactiveCqlOperations().execute(new StatementCallback(statement));

		return execute.map(it -> EntityWriteResult.of(it, entity)).handle(handler) //
				.doOnSubscribe(it -> maybeEmitEvent(new BeforeSaveEvent<>(entity, tableName, statement))) //
				.doOnNext(it -> maybeEmitEvent(new AfterDeleteEvent<>(statement, entity.getClass(), tableName))) //
				.next();
	}

	@SuppressWarnings("ConstantConditions")
	private Mono<Integer> getEffectiveFetchSize(Statement statement) {

		if (statement.getFetchSize() > 0) {
			return Mono.just(statement.getFetchSize());
		}

		if (getReactiveCqlOperations() instanceof CassandraAccessor) {
			CassandraAccessor accessor = (CassandraAccessor) getReactiveCqlOperations();
			if (accessor.getFetchSize() != -1) {
				return Mono.just(accessor.getFetchSize());
			}
		}

		return getReactiveCqlOperations().execute((ReactiveSessionCallback<Integer>) session ->
				Mono.just(session.getCluster().getConfiguration().getQueryOptions().getFetchSize())).single();
	}

	@SuppressWarnings("unchecked")
	private <T> Function<Row, T> getMapper(Class<?> entityType, Class<T> targetType, CqlIdentifier tableName) {

		Class<?> typeToRead = resolveTypeToRead(entityType, targetType);

		return row -> {

			maybeEmitEvent(new AfterLoadEvent<>(row, targetType, tableName));

			Object source = getConverter().read(typeToRead, row);

			T result = (T) (targetType.isInterface()
					? getProjectionFactory().createProjection(targetType, source)
					: source);

			maybeEmitEvent(new AfterConvertEvent<>(row, result, tableName));

			return result;
		};
	}

	private Class<?> resolveTypeToRead(Class<?> entityType, Class<?> targetType) {
		return targetType.isInterface() || targetType.isAssignableFrom(entityType) ? entityType : targetType;
	}

	private static MappingCassandraConverter newConverter() {

		MappingCassandraConverter converter = new MappingCassandraConverter();

		converter.afterPropertiesSet();

		return converter;
	}

	private void maybeEmitEvent(ApplicationEvent event) {

		if (this.eventPublisher != null) {
			this.eventPublisher.publishEvent(event);
		}
	}

	@Value
	static class StatementCallback implements ReactiveSessionCallback<WriteResult>, CqlProvider {

		@lombok.NonNull Statement statement;

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.ReactiveSessionCallback#doInSession(org.springframework.data.cassandra.ReactiveSession)
		 */
		@Override
		public Publisher<WriteResult> doInSession(ReactiveSession session) throws DriverException, DataAccessException {
			return session.execute(this.statement).flatMap(StatementCallback::toWriteResult);
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.CqlProvider#getCql()
		 */
		@Override
		public String getCql() {
			return this.statement.toString();
		}

		private static Mono<WriteResult> toWriteResult(ReactiveResultSet resultSet) {
			return resultSet.rows().collectList()
					.map(rows -> new WriteResult(resultSet.getAllExecutionInfo(), resultSet.wasApplied(), rows));
		}
	}
}
