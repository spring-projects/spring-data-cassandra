/*
 * Copyright 2016-2025 the original author or authors.
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
import java.util.function.Supplier;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jspecify.annotations.Nullable;
import org.reactivestreams.Publisher;

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
import org.springframework.data.cassandra.core.mapping.SimpleUserTypeResolver;
import org.springframework.data.cassandra.core.mapping.event.AfterDeleteEvent;
import org.springframework.data.cassandra.core.mapping.event.AfterLoadEvent;
import org.springframework.data.cassandra.core.mapping.event.AfterSaveEvent;
import org.springframework.data.cassandra.core.mapping.event.BeforeDeleteEvent;
import org.springframework.data.cassandra.core.mapping.event.BeforeSaveEvent;
import org.springframework.data.cassandra.core.mapping.event.CassandraMappingEvent;
import org.springframework.data.cassandra.core.mapping.event.ReactiveBeforeConvertCallback;
import org.springframework.data.cassandra.core.mapping.event.ReactiveBeforeSaveCallback;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.SliceImpl;
import org.springframework.data.mapping.callback.EntityCallbacks;
import org.springframework.data.mapping.callback.ReactiveEntityCallbacks;
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
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
 * @author Sam Lightfoot
 * @since 2.0
 */
public class ReactiveCassandraTemplate
		implements ReactiveCassandraOperations, ApplicationEventPublisherAware, ApplicationContextAware {

	private final Log log = LogFactory.getLog(getClass());

	private final ReactiveCqlOperations cqlOperations;

	private final EntityLifecycleEventDelegate eventDelegate;

	private final CassandraConverter converter;

	private final StatementFactory statementFactory;

	private final EntityOperations entityOperations;

	private final QueryOperations queryOperations;

	private @Nullable ReactiveEntityCallbacks entityCallbacks;

	private boolean usePreparedStatements = true;

	/**
	 * Creates an instance of {@link ReactiveCassandraTemplate} initialized with the given {@link ReactiveSession} and a
	 * default {@link MappingCassandraConverter}.
	 *
	 * @param session {@link ReactiveSession} used to interact with Cassandra; must not be {@literal null}.
	 * @see CassandraConverter
	 * @see ReactiveSession
	 */
	public ReactiveCassandraTemplate(ReactiveSession session) {
		this(session, CassandraTemplate.createConverter(
				new SimpleUserTypeResolver(session::getMetadata, session.getKeyspace().orElse(CqlIdentifier.fromCql("system"))),
				session.getContext()));
	}

	/**
	 * Create an instance of {@link CassandraTemplate} initialized with the given {@link ReactiveSession} and
	 * {@link CassandraConverter}.
	 *
	 * @param session {@link ReactiveSession} used to interact with Cassandra; must not be {@literal null}.
	 * @param converter {@link CassandraConverter} used to convert between Java and Cassandra types; must not be
	 *          {@literal null}.
	 * @see org.springframework.data.cassandra.core.convert.CassandraConverter
	 * @see ReactiveSession
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
	 * @see ReactiveSession
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
	 * @see ReactiveSession
	 */
	public ReactiveCassandraTemplate(ReactiveCqlOperations reactiveCqlOperations, CassandraConverter converter) {

		Assert.notNull(reactiveCqlOperations, "ReactiveCqlOperations must not be null");
		Assert.notNull(converter, "CassandraConverter must not be null");

		this.converter = converter;
		this.eventDelegate = new EntityLifecycleEventDelegate();
		this.cqlOperations = reactiveCqlOperations;
		this.statementFactory = new StatementFactory(converter);
		this.entityOperations = new EntityOperations(converter);
		this.queryOperations = new QueryOperations(converter, this.statementFactory, this.eventDelegate);
	}

	@Override
	public ReactiveCassandraBatchOperations batchOps(BatchType batchType) {
		return new ReactiveCassandraBatchTemplate(this, batchType);
	}

	@Override
	public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
		this.eventDelegate.setPublisher(applicationEventPublisher);
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {

		if (entityCallbacks == null) {
			setEntityCallbacks(ReactiveEntityCallbacks.create(applicationContext));
		}
	}

	/**
	 * Configure {@link EntityCallbacks} to pre-/post-process entities during persistence operations.
	 *
	 * @param entityCallbacks
	 */
	public void setEntityCallbacks(@Nullable ReactiveEntityCallbacks entityCallbacks) {
		this.entityCallbacks = entityCallbacks;
	}

	/**
	 * Configure whether lifecycle events such as {@link AfterLoadEvent}, {@link BeforeSaveEvent}, etc. should be
	 * published or whether emission should be suppressed. Enabled by default.
	 *
	 * @param enabled {@code true} to enable entity lifecycle events; {@code false} to disable entity lifecycle events.
	 * @since 4.0
	 * @see CassandraMappingEvent
	 */
	public void setEntityLifecycleEventsEnabled(boolean enabled) {
		this.eventDelegate.setEventsEnabled(enabled);
	}

	@Override
	public ReactiveCqlOperations getReactiveCqlOperations() {
		return this.cqlOperations;
	}

	@Override
	public CassandraConverter getConverter() {
		return this.converter;
	}

	/**
	 * Returns the {@link StatementFactory} used by this template to construct and run Cassandra CQL statements.
	 *
	 * @return the {@link StatementFactory} used by this template to construct and run Cassandra CQL statements.
	 * @see org.springframework.data.cassandra.core.StatementFactory
	 * @since 2.1
	 */
	public StatementFactory getStatementFactory() {
		return this.statementFactory;
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

	// -------------------------------------------------------------------------
	// Methods dealing with static CQL
	// -------------------------------------------------------------------------

	@Override
	public <T> Flux<T> select(String cql, Class<T> entityClass) {

		Assert.hasText(cql, "CQL must not be empty");

		return select(SimpleStatement.newInstance(cql), entityClass);
	}

	@Override
	public <T> Mono<T> selectOne(String cql, Class<T> entityClass) {
		return select(cql, entityClass).next();
	}

	// -------------------------------------------------------------------------
	// Methods dealing with com.datastax.oss.driver.api.core.cql.Statement
	// -------------------------------------------------------------------------

	@Override
	public Mono<ReactiveResultSet> execute(Statement<?> statement) throws DataAccessException {

		Assert.notNull(statement, "Statement must not be null");

		return execute(statement, Function.identity());
	}

	@Override
	public <T> Flux<T> select(Statement<?> statement, Class<T> entityClass) {

		Assert.notNull(statement, "Statement must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		RowMapper<T> mapper = queryOperations.getRowMapper(entityClass, statement);
		return doQuery(statement, mapper);
	}

	@Override
	public <T> Mono<T> selectOne(Statement<?> statement, Class<T> entityClass) {
		return select(statement, entityClass).next();
	}

	@Override
	public <T> Mono<Slice<T>> slice(Statement<?> statement, Class<T> entityClass) {

		Assert.notNull(statement, "Statement must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		RowMapper<T> mapper = queryOperations.getRowMapper(entityClass, statement);
		return doSlice(statement, mapper);
	}

	// -------------------------------------------------------------------------
	// Methods dealing with org.springframework.data.cassandra.core.query.Query
	// -------------------------------------------------------------------------

	@Override
	public <T> Flux<T> select(Query query, Class<T> entityClass) throws DataAccessException {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		return select(query, entityClass, getTableName(entityClass), entityClass, QueryResultConverter.entity());
	}

	<T, R> Flux<R> select(Query query, Class<?> entityClass, CqlIdentifier tableName, Class<T> returnType,
			QueryResultConverter<? super T, ? extends R> mappingFunction) {

		return queryOperations.select(entityClass, tableName).<T, R> project(returnType, mappingFunction).matching(query)
				.select(this::doQuery);
	}

	@Override
	public <T> Mono<T> selectOne(Query query, Class<T> entityClass) throws DataAccessException {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		return select(query.limit(1), entityClass).next();
	}

	@Override
	public <T> Mono<Slice<T>> slice(Query query, Class<T> entityClass) throws DataAccessException {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		return queryOperations.select(entityClass).matching(query).select(this::doSlice);
	}

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
				.doOnSubscribe(it -> maybeEmitEvent(() -> new BeforeDeleteEvent<>(delete, entityClass, tableName)));

		return writeResult.doOnNext(it -> maybeEmitEvent(() -> new AfterDeleteEvent<>(delete, entityClass, tableName)));
	}

	// -------------------------------------------------------------------------
	// Methods dealing with entities
	// -------------------------------------------------------------------------

	@Override
	public Mono<Long> count(Class<?> entityClass) {

		Assert.notNull(entityClass, "Entity type must not be null");

		return doCount(Query.empty(), entityClass, getTableName(entityClass));
	}

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
				.map(DataAccessUtils::nullableSingleResult).next());

		return mono.switchIfEmpty(Mono.just(0L));
	}

	@Override
	public Mono<Boolean> exists(Object id, Class<?> entityClass) {

		Assert.notNull(id, "Id must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		return doExists(queryOperations.select(entityClass).matchingId(id));
	}

	@Override
	public Mono<Boolean> exists(Query query, Class<?> entityClass) throws DataAccessException {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		return doExists(query, entityClass, getTableName(entityClass));
	}

	Mono<Boolean> doExists(Query query, Class<?> entityClass, CqlIdentifier tableName) {
		return doExists(queryOperations.select(entityClass, tableName).matching(query));
	}

	private Mono<Boolean> doExists(QueryOperations.TerminalExists exists) {

		return exists.exists(statement -> execute(statement, ReactiveResultSet::rows)).flatMapMany(Function.identity())
				.hasElements();
	}

	@Override
	public <T> Mono<T> selectOneById(Object id, Class<T> entityClass) {

		Assert.notNull(id, "Id must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		return queryOperations.select(entityClass).matchingId(id).select(this::doQuery).next();
	}

	@Override
	public <T> Mono<T> insert(T entity) {
		return insert(entity, InsertOptions.empty()).map(EntityWriteResult::getEntity);
	}

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

	@Override
	public <T> Mono<T> update(T entity) {
		return update(entity, UpdateOptions.empty()).map(EntityWriteResult::getEntity);
	}

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

		AdaptibleEntity<T> source = entityOperations.forEntity(entity, getConverter().getConversionService());

		Number previousVersion = source.getVersion();
		T toSave = source.incrementVersion();

		StatementBuilder<Update> builder = getStatementFactory().update(toSave, options, persistentEntity, tableName);
		SimpleStatement update = source.appendVersionCondition(builder, previousVersion).build();

		return executeSave(toSave, tableName, update, (result, sink) -> {

			if (!result.wasApplied()) {

				sink.error(new OptimisticLockingFailureException(
						String.format("Cannot save entity %s with version %s to table %s; Has it been modified meanwhile", toSave,
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

	@Override
	public <T> Mono<T> delete(T entity) {
		return delete(entity, QueryOptions.empty()).map(reactiveWriteResult -> entity);
	}

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
						String.format("Cannot delete entity %s with version %s in table %s; Has it been modified meanwhile", entity,
								source.getVersion(), tableName)));

				return;
			}

			sink.next(result);
		});
	}

	private Mono<WriteResult> doDelete(SimpleStatement delete, Object entity, CqlIdentifier tableName) {
		return executeDelete(entity, tableName, delete, (result, sink) -> sink.next(result));
	}

	@Override
	public Mono<Boolean> deleteById(Object id, Class<?> entityClass) {

		Assert.notNull(id, "Id must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		CassandraPersistentEntity<?> entity = getRequiredPersistentEntity(entityClass);
		CqlIdentifier tableName = entity.getTableName();

		StatementBuilder<Delete> builder = getStatementFactory().deleteById(id, entity, tableName);
		SimpleStatement delete = builder.build();

		Mono<Boolean> result = execute(delete, ReactiveResultSet::wasApplied)
				.doOnSubscribe(it -> maybeEmitEvent(() -> new BeforeDeleteEvent<>(delete, entityClass, tableName)));

		return result.doOnNext(it -> maybeEmitEvent(() -> new AfterDeleteEvent<>(delete, entityClass, tableName)));
	}

	@Override
	public Mono<Void> truncate(Class<?> entityClass) {

		Assert.notNull(entityClass, "Entity type must not be null");

		CassandraPersistentEntity<?> entity = getRequiredPersistentEntity(entityClass);
		CqlIdentifier tableName = entity.getTableName();

		Truncate truncate = QueryBuilder.truncate(entity.getKeyspace(), tableName);
		SimpleStatement statement = truncate.build();

		Mono<Boolean> result = execute(statement, ReactiveResultSet::wasApplied)
				.doOnSubscribe(it -> maybeEmitEvent(() -> new BeforeDeleteEvent<>(statement, entityClass, tableName)));

		return result.doOnNext(it -> maybeEmitEvent(() -> new AfterDeleteEvent<>(statement, entityClass, tableName)))
				.then();
	}

	// -------------------------------------------------------------------------
	// Fluent API entry points
	// -------------------------------------------------------------------------

	@Override
	public ReactiveDelete delete(Class<?> domainType) {
		return new ReactiveDeleteOperationSupport(this).delete(domainType);
	}

	@Override
	public <T> ReactiveInsert<T> insert(Class<T> domainType) {
		return new ReactiveInsertOperationSupport(this).insert(domainType);
	}

	@Override
	public <T> ReactiveSelect<T> query(Class<T> domainType) {
		return new ReactiveSelectOperationSupport(this, this.queryOperations).query(domainType);
	}

	@Override
	public UntypedSelect query(String cql) {
		return new ReactiveSelectOperationSupport(this, this.queryOperations).query(cql);
	}

	@Override
	public UntypedSelect query(Statement<?> statement) {
		return new ReactiveSelectOperationSupport(this, this.queryOperations).query(statement);
	}

	@Override
	public ReactiveUpdate update(Class<?> domainType) {
		return new ReactiveUpdateOperationSupport(this).update(domainType);
	}

	// -------------------------------------------------------------------------
	// Implementation hooks and utility methods
	// -------------------------------------------------------------------------

	/**
	 * Create a new statement-based {@link ReactivePreparedStatementHandler} using the statement passed in.
	 * <p>
	 * This method allows for the creation to be overridden by subclasses.
	 *
	 * @param statement the statement to be prepared.
	 * @return the new {@link PreparedStatementHandler} to use.
	 * @since 3.3.3
	 */
	protected ReactivePreparedStatementHandler createPreparedStatementHandler(Statement<?> statement) {
		return new PreparedStatementHandler(statement);
	}

	protected <E extends CassandraMappingEvent<T>, T> void maybeEmitEvent(Supplier<E> event) {
		this.eventDelegate.publishEvent(event);
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

	private <T> Mono<EntityWriteResult<T>> executeSave(T entity, CqlIdentifier tableName, SimpleStatement statement) {
		return executeSave(entity, tableName, statement, (writeResult, sink) -> sink.next(writeResult));
	}

	private <T> Mono<EntityWriteResult<T>> executeSave(T entity, CqlIdentifier tableName, SimpleStatement statement,
			BiConsumer<EntityWriteResult<T>, SynchronousSink<EntityWriteResult<T>>> handler) {

		return Mono.defer(() -> {

			maybeEmitEvent(() -> new BeforeSaveEvent<>(entity, tableName, statement));

			return maybeCallBeforeSave(entity, tableName, statement).flatMapMany(entityToSave -> {
				Mono<WriteResult> execute = doExecuteAndFlatMap(statement, ReactiveCassandraTemplate::toWriteResult);

				return execute.map(it -> EntityWriteResult.of(it, entityToSave)).handle(handler) //
						.doOnNext(it -> maybeEmitEvent(() -> new AfterSaveEvent<>(entityToSave, tableName)));
			}).next();
		});
	}

	private Mono<WriteResult> executeDelete(Object entity, CqlIdentifier tableName, SimpleStatement statement,
			BiConsumer<WriteResult, SynchronousSink<WriteResult>> handler) {

		maybeEmitEvent(() -> new BeforeDeleteEvent<>(statement, entity.getClass(), tableName));

		Mono<WriteResult> execute = doExecuteAndFlatMap(statement, ReactiveCassandraTemplate::toWriteResult);

		return execute.map(it -> EntityWriteResult.of(it, entity)).handle(handler) //
				.doOnSubscribe(it -> maybeEmitEvent(() -> new BeforeSaveEvent<>(entity, tableName, statement))) //
				.doOnNext(it -> maybeEmitEvent(() -> new AfterDeleteEvent<>(statement, entity.getClass(), tableName)));
	}

	<T> Flux<T> doQuery(Statement<?> statement, RowMapper<T> rowMapper) {

		if (PreparedStatementDelegate.canPrepare(isUsePreparedStatements(), statement, log)) {

			ReactivePreparedStatementHandler statementHandler = createPreparedStatementHandler(statement);
			return getReactiveCqlOperations().query(statementHandler, statementHandler, rowMapper);
		}

		return getReactiveCqlOperations().query(statement, rowMapper);
	}

	<T> Mono<Slice<T>> doSlice(Statement<?> statement, RowMapper<T> rowMapper) {

		Mono<ReactiveResultSet> resultSetMono = execute(statement, Function.identity());
		Mono<Integer> effectiveFetchSizeMono = getEffectiveFetchSize(statement);

		return resultSetMono.zipWith(effectiveFetchSizeMono).flatMap(tuple -> {

			ReactiveResultSet resultSet = tuple.getT1();
			Integer effectiveFetchSize = tuple.getT2();

			return resultSet.availableRows().collectList().map(it -> EntityQueryUtils.readSlice(it,
					resultSet.getExecutionInfo().getPagingState(), rowMapper, 1, effectiveFetchSize));

		}).defaultIfEmpty(new SliceImpl<>(Collections.emptyList()));
	}

	<T> Mono<T> execute(Statement<?> statement, Function<ReactiveResultSet, T> mappingFunction) {

		if (PreparedStatementDelegate.canPrepare(isUsePreparedStatements(), statement, log)) {

			ReactivePreparedStatementHandler statementHandler = createPreparedStatementHandler(statement);
			return getReactiveCqlOperations()
					.query(statementHandler, statementHandler, rs -> Mono.just(mappingFunction.apply(rs))).next();
		}

		return getReactiveCqlOperations().queryForResultSet(statement).map(mappingFunction);
	}

	private <T> Mono<T> doExecuteAndFlatMap(Statement<?> statement,
			Function<ReactiveResultSet, Mono<T>> mappingFunction) {

		if (PreparedStatementDelegate.canPrepare(isUsePreparedStatements(), statement, log)) {

			ReactivePreparedStatementHandler statementHandler = createPreparedStatementHandler(statement);
			return getReactiveCqlOperations().query(statementHandler, statementHandler, mappingFunction::apply).next();
		}

		return getReactiveCqlOperations().queryForResultSet(statement).flatMap(mappingFunction);
	}

	CqlIdentifier getTableName(Class<?> entityClass) {
		return queryOperations.getTableName(entityClass);
	}

	private CassandraPersistentEntity<?> getRequiredPersistentEntity(Class<?> entityType) {
		return queryOperations.getRequiredPersistentEntity(entityType);
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

		class GetConfiguredPageSize implements ReactiveSessionCallback<Integer>, CqlProvider {
			@Override
			public Publisher<Integer> doInSession(ReactiveSession session) {
				return Mono.just(CassandraTemplate.getConfiguredPageSize(session.getContext()));
			}

			@Override
			public String getCql() {
				return QueryExtractorDelegate.getCql(statement);
			}
		}

		return getReactiveCqlOperations().execute(new GetConfiguredPageSize()).single();
	}

	static Mono<WriteResult> toWriteResult(ReactiveResultSet resultSet) {
		return resultSet.rows().collectList()
				.map(rows -> new WriteResult(resultSet.getAllExecutionInfo(), resultSet.wasApplied(), rows));
	}

	/**
	 * General callback interface used to create and bind prepared CQL statements.
	 * <p>
	 * This interface prepares the CQL statement and sets values on a {@link PreparedStatement} as union-type comprised
	 * from {@link ReactivePreparedStatementCreator}, {@link PreparedStatementBinder}, and {@link CqlProvider}.
	 *
	 * @since 3.3.3
	 */
	public interface ReactivePreparedStatementHandler
			extends ReactivePreparedStatementCreator, PreparedStatementBinder, CqlProvider {

	}

	/**
	 * Utility class to prepare a {@link SimpleStatement} and bind values associated with the statement to a
	 * {@link BoundStatement}.
	 *
	 * @since 3.2
	 */
	public static class PreparedStatementHandler implements ReactivePreparedStatementHandler {

		private final SimpleStatement statement;

		public PreparedStatementHandler(Statement<?> statement) {
			this.statement = PreparedStatementDelegate.getStatementForPrepare(statement);
		}

		@Override
		public Mono<PreparedStatement> createPreparedStatement(ReactiveSession session) throws DriverException {

			// Note that prepared statement settings like the keyspace are gone because using the prepare method with a
			// statement object causes cache pollution
			return session.prepare(statement.getQuery());
		}

		@Override
		public BoundStatement bindValues(PreparedStatement ps) throws DriverException {
			return PreparedStatementDelegate.bind(statement, ps);
		}

		@Override
		public String getCql() {
			return statement.getQuery();
		}
	}
}
