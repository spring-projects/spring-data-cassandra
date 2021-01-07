/*
 * Copyright 2016-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;

import java.util.Collections;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.dao.support.DataAccessUtils;
import org.springframework.data.cassandra.ReactiveResultSet;
import org.springframework.data.cassandra.ReactiveSession;
import org.springframework.data.cassandra.ReactiveSessionFactory;
import org.springframework.data.cassandra.core.EntityOperations.AdaptibleEntity;
import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.cql.*;
import org.springframework.data.cassandra.core.cql.session.DefaultReactiveSessionFactory;
import org.springframework.data.cassandra.core.cql.util.StatementBuilder;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.event.AfterConvertEvent;
import org.springframework.data.cassandra.core.mapping.event.AfterDeleteEvent;
import org.springframework.data.cassandra.core.mapping.event.AfterLoadEvent;
import org.springframework.data.cassandra.core.mapping.event.AfterSaveEvent;
import org.springframework.data.cassandra.core.mapping.event.BeforeDeleteEvent;
import org.springframework.data.cassandra.core.mapping.event.BeforeSaveEvent;
import org.springframework.data.cassandra.core.mapping.event.CassandraMappingEvent;
import org.springframework.data.cassandra.core.mapping.event.ReactiveBeforeConvertCallback;
import org.springframework.data.cassandra.core.mapping.event.ReactiveBeforeSaveCallback;
import org.springframework.data.cassandra.core.query.Columns;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.SliceImpl;
import org.springframework.data.mapping.callback.EntityCallbacks;
import org.springframework.data.mapping.callback.ReactiveEntityCallbacks;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.truncate.Truncate;
import com.datastax.oss.driver.api.querybuilder.update.Update;

/**
 * Primary implementation of {@link ReactiveCassandraOperations}. It simplifies the use of Reactive Cassandra usage and
 * helps to avoid common errors. It executes core Cassandra workflow. This class executes CQL queries or updates,
 * initiating iteration over {@link ReactiveResultSet} and catching Cassandra exceptions and translating them to the
 * generic, more informative exception hierarchy defined in the {@code org.springframework.dao} package.
 * <p>
 * Can be used within a service implementation via direct instantiation with a {@link ReactiveSessionFactory} reference,
 * or get prepared in an application context and given to services as bean reference.
 * <p>
 * This class supports the use of prepared statements when enabling {@link #setUsePreparedStatements(boolean)}. All
 * statements created by methods of this class (such as {@link #select(Query, Class)} or
 * {@link #update(Query, org.springframework.data.cassandra.core.query.Update, Class)} will be executed as prepared
 * statements. Also, statements accepted by methods (such as {@link #select(String, Class)} or
 * {@link #select(Statement, Class) and others}) will be prepared prior to execution. Note that {@link Statement}
 * objects passed to methods must be {@link SimpleStatement} so that these can be prepared.
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
public class ReactiveCassandraTemplate
		implements ReactiveCassandraOperations, ApplicationEventPublisherAware, ApplicationContextAware {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	private final ReactiveCqlOperations cqlOperations;

	private final CassandraConverter converter;

	private final EntityOperations entityOperations;

	private final SpelAwareProxyProjectionFactory projectionFactory;

	private final StatementFactory statementFactory;

	private @Nullable ApplicationEventPublisher eventPublisher;

	private @Nullable ReactiveEntityCallbacks entityCallbacks;

	private boolean usePreparedStatements = true;

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
	 * @see com.datastax.oss.driver.api.core.CqlSession
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
	 * @see com.datastax.oss.driver.api.core.CqlSession
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
	 * @see com.datastax.oss.driver.api.core.CqlSession
	 */
	public ReactiveCassandraTemplate(ReactiveCqlOperations reactiveCqlOperations, CassandraConverter converter) {

		Assert.notNull(reactiveCqlOperations, "ReactiveCqlOperations must not be null");
		Assert.notNull(converter, "CassandraConverter must not be null");

		this.converter = converter;
		this.cqlOperations = reactiveCqlOperations;
		this.entityOperations = new EntityOperations(converter.getMappingContext());
		this.projectionFactory = new SpelAwareProxyProjectionFactory();
		this.statementFactory = new StatementFactory(converter);
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
	* @see org.springframework.context.ApplicationContextAware(org.springframework.context.ApplicationContext)
	*/
	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {

		if (entityCallbacks == null) {
			setEntityCallbacks(ReactiveEntityCallbacks.create(applicationContext));
		}

		projectionFactory.setBeanFactory(applicationContext);
		projectionFactory.setBeanClassLoader(applicationContext.getClassLoader());
	}

	/**
	 * Configure {@link EntityCallbacks} to pre-/post-process entities during persistence operations.
	 *
	 * @param entityCallbacks
	 */
	public void setEntityCallbacks(@Nullable ReactiveEntityCallbacks entityCallbacks) {
		this.entityCallbacks = entityCallbacks;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#getReactiveCqlOperations()
	 */
	@Override
	public ReactiveCqlOperations getReactiveCqlOperations() {
		return this.cqlOperations;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#getConverter()
	 */
	@Override
	public CassandraConverter getConverter() {
		return this.converter;
	}

	/**
	 * Returns whether this instance is configured to use {@link PreparedStatement prepared statements}. If enabled
	 * (default), then all persistence methods (such as {@link #select}, {@link #update}, and others) will make use of
	 * prepared statements. Note that methods accepting a {@link Statement} must be called with {@link SimpleStatement}
	 * instances to participate in statement preparation.
	 *
	 * @return {@literal true} if prepared statements usage is enabled; {@literal false} otherwise.
	 * @since 3.2
	 */
	public boolean isUsePreparedStatements() {
		return usePreparedStatements;
	}

	/**
	 * Enable/disable {@link PreparedStatement prepared statements} usage. If enabled (default), then all persistence
	 * methods (such as {@link #select}, {@link #update}, and others) will make use of prepared statements. Note that
	 * methods accepting a {@link Statement} must be called with {@link SimpleStatement} instances to participate in
	 * statement preparation.
	 *
	 * @param usePreparedStatements whether to use prepared statements.
	 * @since 3.2
	 */
	public void setUsePreparedStatements(boolean usePreparedStatements) {
		this.usePreparedStatements = usePreparedStatements;
	}

	/**
	 * Returns the {@link EntityOperations} used to perform data access operations on an entity inside a Cassandra data
	 * source.
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
		return getRequiredPersistentEntity(entityClass).getTableName();
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

		return select(SimpleStatement.newInstance(cql), entityClass);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#selectOne(java.lang.String, java.lang.Class)
	 */
	@Override
	public <T> Mono<T> selectOne(String cql, Class<T> entityClass) {
		return select(cql, entityClass).next();
	}

	// -------------------------------------------------------------------------
	// Methods dealing with com.datastax.oss.driver.api.core.cql.Statement
	// -------------------------------------------------------------------------

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#execute(com.datastax.oss.driver.api.core.cql.Statement)
	 */
	@Override
	public Mono<ReactiveResultSet> execute(Statement<?> statement) throws DataAccessException {

		Assert.notNull(statement, "Statement must not be null");

		return doExecute(statement, Function.identity());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#select(com.datastax.oss.driver.api.core.cql.Statement, java.lang.Class)
	 */
	@Override
	public <T> Flux<T> select(Statement<?> statement, Class<T> entityClass) {

		Assert.notNull(statement, "Statement must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		Function<Row, T> mapper = getMapper(entityClass, entityClass, EntityQueryUtils.getTableName(statement));

		return doQuery(statement, (row, rowNum) -> mapper.apply(row));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#selectOne(com.datastax.oss.driver.api.core.cql.Statement, java.lang.Class)
	 */
	@Override
	public <T> Mono<T> selectOne(Statement<?> statement, Class<T> entityClass) {
		return select(statement, entityClass).next();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#slice(com.datastax.oss.driver.api.core.cql.Statement, java.lang.Class)
	 */
	@Override
	public <T> Mono<Slice<T>> slice(Statement<?> statement, Class<T> entityClass) {

		Assert.notNull(statement, "Statement must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		Mono<ReactiveResultSet> resultSetMono = doExecute(statement, Function.identity());
		Mono<Integer> effectiveFetchSizeMono = getEffectiveFetchSize(statement);
		RowMapper<T> rowMapper = (row, i) -> getConverter().read(entityClass, row);

		return resultSetMono.zipWith(effectiveFetchSizeMono).flatMap(tuple -> {

			ReactiveResultSet resultSet = tuple.getT1();
			Integer effectiveFetchSize = tuple.getT2();

			return resultSet.availableRows().collectList().map(it -> EntityQueryUtils.readSlice(it,
					resultSet.getExecutionInfo().getPagingState(), rowMapper, 1, effectiveFetchSize));

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

		Columns columns = getStatementFactory().computeColumnsForProjection(query.getColumns(), persistentEntity,
				returnType);

		Query queryToUse = query.columns(columns);

		StatementBuilder<Select> select = getStatementFactory().select(queryToUse, persistentEntity, tableName);

		Function<Row, T> mapper = getMapper(entityClass, returnType, tableName);

		return doQuery(select.build(), (row, rowNum) -> mapper.apply(row));
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

		StatementBuilder<Select> select = getStatementFactory().select(query, getRequiredPersistentEntity(entityClass));

		return slice(select.build(), entityClass);
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

		StatementBuilder<Update> statement = getStatementFactory().update(query, update,
				getRequiredPersistentEntity(entityClass), tableName);

		return doExecuteAndFlatMap(statement.build(), ReactiveCassandraTemplate::toWriteResult);
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

		StatementBuilder<Delete> builder = getStatementFactory().delete(query, getRequiredPersistentEntity(entityClass),
				tableName);

		SimpleStatement delete = builder.build();

		Mono<WriteResult> writeResult = doExecuteAndFlatMap(delete, ReactiveCassandraTemplate::toWriteResult)
				.doOnSubscribe(it -> maybeEmitEvent(new BeforeDeleteEvent<>(delete, entityClass, tableName)));

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

		return doCount(Query.empty(), entityClass, getTableName(entityClass));
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

		StatementBuilder<Select> count = getStatementFactory().count(query, getRequiredPersistentEntity(entityClass),
				tableName);

		SingleColumnRowMapper<Long> mapper = SingleColumnRowMapper.newInstance(Long.class);

		Mono<Long> mono = doExecuteAndFlatMap(count.build(), rs -> rs.rows() //
				.map(it -> mapper.mapRow(it, 0)) //
				.buffer() //
				.map(DataAccessUtils::requiredSingleResult).next());

		return mono.switchIfEmpty(Mono.just(0L));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#exists(java.lang.Object, java.lang.Class)
	 */
	@Override
	public Mono<Boolean> exists(Object id, Class<?> entityClass) {

		Assert.notNull(id, "Id must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		CassandraPersistentEntity<?> entity = getRequiredPersistentEntity(entityClass);
		StatementBuilder<Select> builder = getStatementFactory().selectOneById(id, entity, entity.getTableName());

		return doQuery(builder.build(), (row, rowNum) -> row).hasElements();
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

		StatementBuilder<Select> builder = getStatementFactory().select(query.limit(1),
				getRequiredPersistentEntity(entityClass), tableName);

		return doQuery(builder.build(), (row, rowNum) -> row).hasElements();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#selectOneById(java.lang.Object, java.lang.Class)
	 */
	@Override
	public <T> Mono<T> selectOneById(Object id, Class<T> entityClass) {

		Assert.notNull(id, "Id must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		StatementBuilder<Select> builder = getStatementFactory().selectOneById(id, getRequiredPersistentEntity(entityClass),
				getTableName(entityClass));

		return selectOne(builder.build(), entityClass);
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

		return maybeCallBeforeConvert(entity, tableName).flatMap(entityToInsert -> {

			AdaptibleEntity<T> source = this.entityOperations.forEntity(entityToInsert,
					getConverter().getConversionService());
			CassandraPersistentEntity<?> persistentEntity = getRequiredPersistentEntity(entityToInsert.getClass());

			T entityToUse = source.isVersionedEntity() ? source.initializeVersionProperty() : entityToInsert;

			StatementBuilder<RegularInsert> builder = getStatementFactory().insert(entityToUse, options, persistentEntity,
					tableName);

			if (source.isVersionedEntity()) {
				builder.apply(Insert::ifNotExists);
				return doInsertVersioned(builder.build(), entityToUse, source, tableName);
			}

			return doInsert(builder.build(), entityToUse, tableName);
		});
	}

	private <T> Mono<EntityWriteResult<T>> doInsertVersioned(SimpleStatement insert, T entity, AdaptibleEntity<T> source,
			CqlIdentifier tableName) {

		return executeSave(entity, tableName, insert, (result, sink) -> {

			if (!result.wasApplied()) {

				sink.error(new OptimisticLockingFailureException(
						String.format("Cannot insert entity %s with version %s into table %s as it already exists", entity,
								source.getVersion(), tableName)));

				return;
			}

			sink.next(result);
		});
	}

	private <T> Mono<EntityWriteResult<T>> doInsert(SimpleStatement insert, T entity, CqlIdentifier tableName) {
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

		return maybeCallBeforeConvert(entity, tableName).flatMap(entityToUpdate -> {
			return source.isVersionedEntity() ? doUpdateVersioned(entity, options, tableName, persistentEntity)
					: doUpdate(entity, options, tableName, persistentEntity);
		});
	}

	private <T> Mono<EntityWriteResult<T>> doUpdateVersioned(T entity, UpdateOptions options, CqlIdentifier tableName,
			CassandraPersistentEntity<?> persistentEntity) {

		AdaptibleEntity<T> source = getEntityOperations().forEntity(entity, getConverter().getConversionService());

		Number previousVersion = source.getVersion();
		T toSave = source.incrementVersion();

		StatementBuilder<Update> builder = getStatementFactory().update(toSave, options, persistentEntity, tableName);
		SimpleStatement update = source.appendVersionCondition(builder, previousVersion).build();

		return executeSave(toSave, tableName, update, (result, sink) -> {

			if (!result.wasApplied()) {

				sink.error(new OptimisticLockingFailureException(
						String.format("Cannot save entity %s with version %s to table %s. Has it been modified meanwhile?", toSave,
								source.getVersion(), tableName)));

				return;
			}

			sink.next(result);
		});
	}

	private <T> Mono<EntityWriteResult<T>> doUpdate(T entity, UpdateOptions options, CqlIdentifier tableName,
			CassandraPersistentEntity<?> persistentEntity) {

		StatementBuilder<Update> builder = getStatementFactory().update(entity, options, persistentEntity, tableName);

		return executeSave(entity, tableName, builder.build());
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

		StatementBuilder<Delete> builder = getStatementFactory().delete(entity, options, getConverter(), tableName);

		return source.isVersionedEntity()
				? doDeleteVersioned(source.appendVersionCondition(builder).build(), entity, source, tableName)
				: doDelete(builder.build(), entity, tableName);
	}

	private Mono<WriteResult> doDeleteVersioned(SimpleStatement delete, Object entity, AdaptibleEntity<Object> source,
			CqlIdentifier tableName) {

		return executeDelete(entity, tableName, delete, (result, sink) -> {

			if (!result.wasApplied()) {

				sink.error(new OptimisticLockingFailureException(
						String.format("Cannot delete entity %s with version %s in table %s. Has it been modified meanwhile?",
								entity, source.getVersion(), tableName)));

				return;
			}

			sink.next(result);
		});
	}

	private Mono<WriteResult> doDelete(SimpleStatement delete, Object entity, CqlIdentifier tableName) {
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

		StatementBuilder<Delete> builder = getStatementFactory().deleteById(id, entity, tableName);
		SimpleStatement delete = builder.build();

		Mono<Boolean> result = doExecute(delete, ReactiveResultSet::wasApplied)
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
		Truncate truncate = QueryBuilder.truncate(tableName);
		SimpleStatement statement = truncate.build();

		Mono<Boolean> result = doExecute(statement, ReactiveResultSet::wasApplied)
				.doOnSubscribe(it -> maybeEmitEvent(new BeforeDeleteEvent<>(statement, entityClass, tableName)));

		return result.doOnNext(it -> maybeEmitEvent(new AfterDeleteEvent<>(statement, entityClass, tableName))).then();
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

	private <T> Mono<EntityWriteResult<T>> executeSave(T entity, CqlIdentifier tableName, SimpleStatement statement) {
		return executeSave(entity, tableName, statement, (writeResult, sink) -> sink.next(writeResult));
	}

	private <T> Mono<EntityWriteResult<T>> executeSave(T entity, CqlIdentifier tableName, SimpleStatement statement,
			BiConsumer<EntityWriteResult<T>, SynchronousSink<EntityWriteResult<T>>> handler) {

		return Mono.defer(() -> {

			maybeEmitEvent(new BeforeSaveEvent<>(entity, tableName, statement));

			return maybeCallBeforeSave(entity, tableName, statement).flatMapMany(entityToSave -> {
				Mono<WriteResult> execute = doExecuteAndFlatMap(statement, ReactiveCassandraTemplate::toWriteResult);

				return execute.map(it -> EntityWriteResult.of(it, entityToSave)).handle(handler) //
						.doOnNext(it -> maybeEmitEvent(new AfterSaveEvent<>(entityToSave, tableName)));
			}).next();
		});
	}

	private Mono<WriteResult> executeDelete(Object entity, CqlIdentifier tableName, SimpleStatement statement,
			BiConsumer<WriteResult, SynchronousSink<WriteResult>> handler) {

		maybeEmitEvent(new BeforeDeleteEvent<>(statement, entity.getClass(), tableName));

		Mono<WriteResult> execute = doExecuteAndFlatMap(statement, ReactiveCassandraTemplate::toWriteResult);

		return execute.map(it -> EntityWriteResult.of(it, entity)).handle(handler) //
				.doOnSubscribe(it -> maybeEmitEvent(new BeforeSaveEvent<>(entity, tableName, statement))) //
				.doOnNext(it -> maybeEmitEvent(new AfterDeleteEvent<>(statement, entity.getClass(), tableName)));
	}

	private <T> Flux<T> doQuery(Statement<?> statement, RowMapper<T> rowMapper) {

		if (PreparedStatementDelegate.canPrepare(isUsePreparedStatements(), statement, logger)) {

			PreparedStatementHandler statementHandler = new PreparedStatementHandler(statement);
			return getReactiveCqlOperations().query(statementHandler, statementHandler, rowMapper);
		}

		return getReactiveCqlOperations().query(statement, rowMapper);
	}

	private <T> Mono<T> doExecute(Statement<?> statement, Function<ReactiveResultSet, T> mappingFunction) {

		if (PreparedStatementDelegate.canPrepare(isUsePreparedStatements(), statement, logger)) {

			PreparedStatementHandler statementHandler = new PreparedStatementHandler(statement);
			return getReactiveCqlOperations()
					.query(statementHandler, statementHandler, rs -> Mono.just(mappingFunction.apply(rs))).next();
		}

		return getReactiveCqlOperations().queryForResultSet(statement).map(mappingFunction);
	}

	private <T> Mono<T> doExecuteAndFlatMap(Statement<?> statement,
			Function<ReactiveResultSet, Mono<T>> mappingFunction) {

		if (PreparedStatementDelegate.canPrepare(isUsePreparedStatements(), statement, logger)) {

			PreparedStatementHandler statementHandler = new PreparedStatementHandler(statement);
			return getReactiveCqlOperations().query(statementHandler, statementHandler, mappingFunction::apply).next();
		}

		return getReactiveCqlOperations().queryForResultSet(statement).flatMap(mappingFunction);
	}

	private int getConfiguredPageSize(DriverContext context) {
		return context.getConfig().getDefaultProfile().getInt(DefaultDriverOption.REQUEST_PAGE_SIZE, 5000);
	}

	private Mono<Integer> getEffectiveFetchSize(Statement<?> statement) {

		if (statement.getPageSize() > 0) {
			return Mono.just(statement.getPageSize());
		}

		if (getReactiveCqlOperations() instanceof CassandraAccessor) {
			CassandraAccessor accessor = (CassandraAccessor) getReactiveCqlOperations();
			if (accessor.getPageSize() != -1) {
				return Mono.just(accessor.getPageSize());
			}
		}

		return getReactiveCqlOperations()
				.execute((ReactiveSessionCallback<Integer>) session -> Mono.just(getConfiguredPageSize(session.getContext())))
				.single();
	}

	@SuppressWarnings("unchecked")
	private <T> Function<Row, T> getMapper(Class<?> entityType, Class<T> targetType, CqlIdentifier tableName) {

		Class<?> typeToRead = resolveTypeToRead(entityType, targetType);

		return row -> {

			maybeEmitEvent(new AfterLoadEvent<>(row, targetType, tableName));

			Object source = getConverter().read(typeToRead, row);

			T result = (T) (targetType.isInterface() ? getProjectionFactory().createProjection(targetType, source) : source);

			if (result != null) {
				maybeEmitEvent(new AfterConvertEvent<>(row, result, tableName));
			}

			return result;
		};
	}

	static Mono<WriteResult> toWriteResult(ReactiveResultSet resultSet) {
		return resultSet.rows().collectList()
				.map(rows -> new WriteResult(resultSet.getAllExecutionInfo(), resultSet.wasApplied(), rows));
	}

	private Class<?> resolveTypeToRead(Class<?> entityType, Class<?> targetType) {
		return targetType.isInterface() || targetType.isAssignableFrom(entityType) ? entityType : targetType;
	}

	private static MappingCassandraConverter newConverter() {

		MappingCassandraConverter converter = new MappingCassandraConverter();

		converter.afterPropertiesSet();

		return converter;
	}

	protected <E extends CassandraMappingEvent<T>, T> void maybeEmitEvent(E event) {

		if (this.eventPublisher != null) {
			this.eventPublisher.publishEvent(event);
		}
	}

	protected <T> Mono<T> maybeCallBeforeConvert(T object, CqlIdentifier tableName) {

		if (null != entityCallbacks) {
			return entityCallbacks.callback(ReactiveBeforeConvertCallback.class, object, tableName);
		}

		return Mono.just(object);
	}

	protected <T> Mono<T> maybeCallBeforeSave(T object, CqlIdentifier tableName, Statement<?> statement) {

		if (null != entityCallbacks) {
			return entityCallbacks.callback(ReactiveBeforeSaveCallback.class, object, tableName, statement);
		}

		return Mono.just(object);
	}

	/**
	 * Utility class to prepare a {@link SimpleStatement} and bind values associated with the statement to a
	 * {@link BoundStatement}.
	 *
	 * @since 3.2
	 */
	private static class PreparedStatementHandler
			implements ReactivePreparedStatementCreator, PreparedStatementBinder, CqlProvider {

		private final SimpleStatement statement;

		public PreparedStatementHandler(Statement<?> statement) {
			this.statement = PreparedStatementDelegate.getStatementForPrepare(statement);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.ReactivePreparedStatementCreator#doInSession(org.springframework.data.cassandra.ReactiveSession)
		 */
		@Override
		public Mono<PreparedStatement> createPreparedStatement(ReactiveSession session) throws DriverException {
			return session.prepare(statement);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.PreparedStatementBinder#bindValues(com.datastax.oss.driver.api.core.cql.PreparedStatement)
		 */
		@Override
		public BoundStatement bindValues(PreparedStatement ps) throws DriverException {
			return PreparedStatementDelegate.bind(statement, ps);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.CqlProvider#getCql()
		 */
		@Override
		public String getCql() {
			return statement.getQuery();
		}
	}
}
