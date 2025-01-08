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
package org.springframework.data.cassandra.core.legacy;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.dao.support.DataAccessUtils;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.cassandra.SessionFactory;
import org.springframework.data.cassandra.core.EntityWriteResult;
import org.springframework.data.cassandra.core.InsertOptions;
import org.springframework.data.cassandra.core.StatementFactory;
import org.springframework.data.cassandra.core.UpdateOptions;
import org.springframework.data.cassandra.core.WriteResult;
import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.cql.CassandraAccessor;
import org.springframework.data.cassandra.core.cql.CqlExceptionTranslator;
import org.springframework.data.cassandra.core.cql.CqlProvider;
import org.springframework.data.cassandra.core.cql.PreparedStatementBinder;
import org.springframework.data.cassandra.core.cql.QueryExtractorDelegate;
import org.springframework.data.cassandra.core.cql.QueryOptions;
import org.springframework.data.cassandra.core.cql.RowCallbackHandler;
import org.springframework.data.cassandra.core.cql.RowMapper;
import org.springframework.data.cassandra.core.cql.SingleColumnRowMapper;
import org.springframework.data.cassandra.core.cql.WriteOptions;
import org.springframework.data.cassandra.core.cql.legacy.AsyncCqlOperations;
import org.springframework.data.cassandra.core.cql.legacy.AsyncCqlTemplate;
import org.springframework.data.cassandra.core.cql.legacy.AsyncPreparedStatementCreator;
import org.springframework.data.cassandra.core.cql.legacy.AsyncResultSetExtractor;
import org.springframework.data.cassandra.core.cql.legacy.AsyncSessionCallback;
import org.springframework.data.cassandra.core.cql.session.DefaultSessionFactory;
import org.springframework.data.cassandra.core.cql.util.CassandraFutureAdapter;
import org.springframework.data.cassandra.core.cql.util.StatementBuilder;
import org.springframework.data.cassandra.core.legacy.EntityOperations.AdaptibleEntity;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.SimpleUserTypeResolver;
import org.springframework.data.cassandra.core.mapping.event.AfterConvertEvent;
import org.springframework.data.cassandra.core.mapping.event.AfterDeleteEvent;
import org.springframework.data.cassandra.core.mapping.event.AfterLoadEvent;
import org.springframework.data.cassandra.core.mapping.event.AfterSaveEvent;
import org.springframework.data.cassandra.core.mapping.event.BeforeConvertCallback;
import org.springframework.data.cassandra.core.mapping.event.BeforeDeleteEvent;
import org.springframework.data.cassandra.core.mapping.event.BeforeSaveCallback;
import org.springframework.data.cassandra.core.mapping.event.BeforeSaveEvent;
import org.springframework.data.cassandra.core.mapping.event.CassandraMappingEvent;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.domain.Slice;
import org.springframework.data.mapping.callback.EntityCallbacks;
import org.springframework.data.projection.EntityProjection;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.util.Streamable;
import org.springframework.lang.Nullable;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.util.Assert;
import org.springframework.util.concurrent.ListenableFuture;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
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
 * Primary implementation of {@link AsyncCassandraOperations}. It simplifies the use of asynchronous Cassandra usage and
 * helps to avoid common errors. It executes core Cassandra workflow. This class executes CQL queries or updates,
 * initiating iteration over {@link ResultSet} and catching Cassandra exceptions and translating them to the generic,
 * more informative exception hierarchy defined in the {@code org.springframework.dao} package.
 * <p>
 * Can be used within a service implementation via direct instantiation with a {@link CqlSession} reference, or get
 * prepared in an application context and given to services as bean reference.
 * <p>
 * This class supports the use of prepared statements when enabling {@link #setUsePreparedStatements(boolean)}. All
 * statements created by methods of this class (such as {@link #select(Query, Class)} or
 * {@link #update(Query, org.springframework.data.cassandra.core.query.Update, Class)} will be executed as prepared
 * statements. Also, statements accepted by methods (such as {@link #select(String, Class)} or
 * {@link #select(Statement, Class) and others}) will be prepared prior to execution. Note that {@link Statement}
 * objects passed to methods must be {@link SimpleStatement} so that these can be prepared.
 * <p>
 * Note: The {@link CqlSession} should always be configured as a bean in the application context, in the first case
 * given to the service directly, in the second case to the prepared template.
 *
 * @author Mark Paluch
 * @author John Blum
 * @see org.springframework.data.cassandra.core.AsyncCassandraOperations
 * @since 4.0
 * @deprecated since 4.0, use the {@link java.util.concurrent.CompletableFuture}-based variant
 *             {@link org.springframework.data.cassandra.core.AsyncCassandraTemplate}.
 */
@Deprecated(since = "4.0", forRemoval = true)
@SuppressWarnings("removal")
public class AsyncCassandraTemplate
		implements AsyncCassandraOperations, ApplicationEventPublisherAware, ApplicationContextAware {

	private final Log log = LogFactory.getLog(getClass());

	private final AsyncCqlOperations cqlOperations;

	private final CassandraConverter converter;

	private final CqlExceptionTranslator exceptionTranslator;

	private final EntityOperations entityOperations;

	private final StatementFactory statementFactory;

	private final EntityLifecycleEventDelegate eventDelegate;

	private @Nullable EntityCallbacks entityCallbacks;

	private boolean usePreparedStatements = true;

	/**
	 * Creates an instance of {@link AsyncCassandraTemplate} initialized with the given {@link CqlSession} and a default
	 * {@link MappingCassandraConverter}.
	 *
	 * @param session {@link CqlSession} used to interact with Cassandra; must not be {@literal null}.
	 * @see CassandraConverter
	 * @see CqlSession
	 */
	public AsyncCassandraTemplate(CqlSession session) {
		this(session, newConverter(session));
	}

	/**
	 * Creates an instance of {@link AsyncCassandraTemplate} initialized with the given {@link CqlSession} and
	 * {@link CassandraConverter}.
	 *
	 * @param session {@link CqlSession} used to interact with Cassandra; must not be {@literal null}.
	 * @param converter {@link CassandraConverter} used to convert between Java and Cassandra types; must not be
	 *          {@literal null}.
	 * @see CassandraConverter
	 * @see CqlSession
	 */
	public AsyncCassandraTemplate(CqlSession session, CassandraConverter converter) {
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
	 * @see CqlSession
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
	 * @see CqlSession
	 */
	public AsyncCassandraTemplate(AsyncCqlTemplate asyncCqlTemplate, CassandraConverter converter) {

		Assert.notNull(asyncCqlTemplate, "AsyncCqlTemplate must not be null");
		Assert.notNull(converter, "CassandraConverter must not be null");

		this.converter = converter;
		this.cqlOperations = asyncCqlTemplate;
		this.entityOperations = new EntityOperations(converter);
		this.exceptionTranslator = asyncCqlTemplate.getExceptionTranslator();
		this.statementFactory = new StatementFactory(converter);
		this.eventDelegate = new EntityLifecycleEventDelegate();
	}

	@Override
	public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
		this.eventDelegate.setPublisher(applicationEventPublisher);
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {

		if (entityCallbacks == null) {
			setEntityCallbacks(EntityCallbacks.create(applicationContext));
		}
	}

	/**
	 * Configure {@link EntityCallbacks} to pre-/post-process entities during persistence operations.
	 *
	 * @param entityCallbacks
	 */
	public void setEntityCallbacks(@Nullable EntityCallbacks entityCallbacks) {
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
	public AsyncCqlOperations getAsyncCqlOperations() {
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
	 * @see StatementFactory
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
	 * @deprecated since 3.4, use {@link CassandraConverter#getProjectionFactory()} instead.
	 */
	@Deprecated
	protected SpelAwareProxyProjectionFactory getProjectionFactory() {
		return (SpelAwareProxyProjectionFactory) getConverter().getProjectionFactory();
	}

	private CassandraPersistentEntity<?> getRequiredPersistentEntity(Class<?> entityType) {
		return getEntityOperations().getRequiredPersistentEntity(entityType);
	}

	private CqlIdentifier getTableName(Class<?> entityClass) {
		return getEntityOperations().getTableName(entityClass);
	}

	// -------------------------------------------------------------------------
	// Methods dealing with static CQL
	// -------------------------------------------------------------------------

	@Override
	public <T> ListenableFuture<List<T>> select(String cql, Class<T> entityClass) {

		Assert.hasText(cql, "CQL must not be empty");

		return select(SimpleStatement.newInstance(cql), entityClass);
	}

	@Override
	public <T> ListenableFuture<Void> select(String cql, Consumer<T> entityConsumer, Class<T> entityClass)
			throws DataAccessException {

		Assert.hasText(cql, "CQL must not be empty");
		Assert.notNull(entityConsumer, "Entity Consumer must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		return select(SimpleStatement.newInstance(cql), entityConsumer, entityClass);
	}

	@Override
	public <T> ListenableFuture<T> selectOne(String cql, Class<T> entityClass) {

		Assert.hasText(cql, "CQL must not be empty");
		Assert.notNull(entityClass, "Entity type must not be null");

		return selectOne(SimpleStatement.newInstance(cql), entityClass);
	}

	// -------------------------------------------------------------------------
	// Methods dealing with com.datastax.oss.driver.api.core.cql.Statement
	// -------------------------------------------------------------------------

	@Override
	public ListenableFuture<AsyncResultSet> execute(Statement<?> statement) throws DataAccessException {

		Assert.notNull(statement, "Statement must not be null");

		return doQueryForResultSet(statement);
	}

	@Override
	public <T> ListenableFuture<List<T>> select(Statement<?> statement, Class<T> entityClass) {

		Assert.notNull(statement, "Statement must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		Function<Row, T> mapper = getMapper(entityClass, entityClass, EntityQueryUtils.getTableName(statement));

		return doQuery(statement, (row, rowNum) -> mapper.apply(row));
	}

	@Override
	public <T> ListenableFuture<Void> select(Statement<?> statement, Consumer<T> entityConsumer, Class<T> entityClass)
			throws DataAccessException {

		Assert.notNull(statement, "Statement must not be null");
		Assert.notNull(entityConsumer, "Entity Consumer must not be empty");
		Assert.notNull(entityClass, "Entity type must not be null");

		Function<Row, T> mapper = getMapper(entityClass, entityClass, EntityQueryUtils.getTableName(statement));

		return doQuery(statement, row -> {
			entityConsumer.accept(mapper.apply(row));
		});
	}

	@Override
	public <T> ListenableFuture<T> selectOne(Statement<?> statement, Class<T> entityClass) {
		return new MappingListenableFutureAdapter<>(select(statement, entityClass),
				list -> list.isEmpty() ? null : list.get(0));
	}

	@Override
	public <T> ListenableFuture<Slice<T>> slice(Statement<?> statement, Class<T> entityClass) {

		Assert.notNull(statement, "Statement must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		ListenableFuture<AsyncResultSet> resultSet = doQueryForResultSet(statement);

		Function<Row, T> mapper = getMapper(entityClass, entityClass, EntityQueryUtils.getTableName(statement));

		return new MappingListenableFutureAdapter<>(resultSet,
				rs -> EntityQueryUtils.readSlice(rs, (row, rowNum) -> mapper.apply(row), 0, getEffectivePageSize(statement)));
	}

	// -------------------------------------------------------------------------
	// Methods dealing with org.springframework.data.cassandra.core.query.Query
	// -------------------------------------------------------------------------

	@Override
	public <T> ListenableFuture<List<T>> select(Query query, Class<T> entityClass) throws DataAccessException {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		return select(getStatementFactory().select(query, getRequiredPersistentEntity(entityClass)).build(), entityClass);
	}

	@Override
	public <T> ListenableFuture<Void> select(Query query, Consumer<T> entityConsumer, Class<T> entityClass)
			throws DataAccessException {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entityConsumer, "Entity Consumer must not be empty");
		Assert.notNull(entityClass, "Entity type must not be null");

		return select(getStatementFactory().select(query, getRequiredPersistentEntity(entityClass)).build(), entityConsumer,
				entityClass);
	}

	@Override
	public <T> ListenableFuture<T> selectOne(Query query, Class<T> entityClass) throws DataAccessException {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		return selectOne(getStatementFactory().select(query, getRequiredPersistentEntity(entityClass)).build(),
				entityClass);
	}

	@Override
	public <T> ListenableFuture<Slice<T>> slice(Query query, Class<T> entityClass) throws DataAccessException {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		return slice(getStatementFactory().select(query, getRequiredPersistentEntity(entityClass)).build(), entityClass);
	}

	@Override
	public ListenableFuture<Boolean> update(Query query, org.springframework.data.cassandra.core.query.Update update,
			Class<?> entityClass) throws DataAccessException {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(update, "Update must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		return doExecute(getStatementFactory().update(query, update, getRequiredPersistentEntity(entityClass)).build(),
				AsyncResultSet::wasApplied);
	}

	@Override
	public ListenableFuture<Boolean> delete(Query query, Class<?> entityClass) throws DataAccessException {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		return doDelete(query, entityClass, getTableName(entityClass));
	}

	private ListenableFuture<Boolean> doDelete(Query query, Class<?> entityClass, CqlIdentifier tableName) {

		StatementBuilder<Delete> builder = getStatementFactory().delete(query, getRequiredPersistentEntity(entityClass),
				tableName);
		SimpleStatement delete = builder.build();

		maybeEmitEvent(() -> new BeforeDeleteEvent<>(delete, entityClass, tableName));

		ListenableFuture<Boolean> future = doExecute(delete, AsyncResultSet::wasApplied);

		future.addCallback(success -> maybeEmitEvent(() -> new AfterDeleteEvent<>(delete, entityClass, tableName)),
				e -> {});

		return future;
	}

	// -------------------------------------------------------------------------
	// Methods dealing with entities
	// -------------------------------------------------------------------------

	@Override
	public ListenableFuture<Long> count(Class<?> entityClass) {

		Assert.notNull(entityClass, "Entity type must not be null");

		return doCount(Query.empty(), entityClass, getTableName(entityClass));
	}

	@Override
	public ListenableFuture<Long> count(Query query, Class<?> entityClass) throws DataAccessException {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		return doCount(query, entityClass, getTableName(entityClass));
	}

	ListenableFuture<Long> doCount(Query query, Class<?> entityClass, CqlIdentifier tableName) {

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.select.Select> countStatement = getStatementFactory()
				.count(query, getRequiredPersistentEntity(entityClass), tableName);

		SimpleStatement statement = countStatement.build();

		ListenableFuture<Long> result = doExecute(statement, it -> {

			SingleColumnRowMapper<Long> mapper = SingleColumnRowMapper.newInstance(Long.class);

			Row row = DataAccessUtils.nullableSingleResult(Streamable.of(it.currentPage()).toList());
			return mapper.mapRow(row, 0);
		});

		return new MappingListenableFutureAdapter<>(result, it -> it != null ? it : 0L);
	}

	@Override
	public ListenableFuture<Boolean> exists(Object id, Class<?> entityClass) {

		Assert.notNull(id, "Id must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		CassandraPersistentEntity<?> entity = getRequiredPersistentEntity(entityClass);

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.select.Select> select = getStatementFactory()
				.selectOneById(id, entity, entity.getTableName());

		return doExecute(select.build(), resultSet -> resultSet.one() != null);
	}

	@Override
	public ListenableFuture<Boolean> exists(Query query, Class<?> entityClass) throws DataAccessException {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.select.Select> select = getStatementFactory()
				.select(query.limit(1), getRequiredPersistentEntity(entityClass), getTableName(entityClass));

		return doExecute(select.build(), resultSet -> resultSet.one() != null);
	}

	@Override
	public <T> ListenableFuture<T> selectOneById(Object id, Class<T> entityClass) {

		Assert.notNull(id, "Id must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		CassandraPersistentEntity<?> entity = getRequiredPersistentEntity(entityClass);
		CqlIdentifier tableName = entity.getTableName();
		StatementBuilder<Select> select = getStatementFactory().selectOneById(id, entity, tableName);
		Function<Row, T> mapper = getMapper(entityClass, entityClass, tableName);

		return new MappingListenableFutureAdapter<>(doQuery(select.build(), (row, rowNum) -> mapper.apply(row)),
				it -> it.isEmpty() ? null : it.get(0));
	}

	@Override
	public <T> ListenableFuture<T> insert(T entity) {
		return new MappingListenableFutureAdapter<>(insert(entity, InsertOptions.empty()), EntityWriteResult::getEntity);
	}

	@Override
	public <T> ListenableFuture<EntityWriteResult<T>> insert(T entity, InsertOptions options) {

		Assert.notNull(entity, "Entity must not be null");
		Assert.notNull(options, "InsertOptions must not be null");

		return doInsert(entity, options, getTableName(entity.getClass()));
	}

	private <T> ListenableFuture<EntityWriteResult<T>> doInsert(T entity, WriteOptions options, CqlIdentifier tableName) {

		AdaptibleEntity<T> source = getEntityOperations().forEntity(maybeCallBeforeConvert(entity, tableName),
				getConverter().getConversionService());
		CassandraPersistentEntity<?> persistentEntity = getRequiredPersistentEntity(entity.getClass());

		T entityToUse = source.isVersionedEntity() ? source.initializeVersionProperty() : source.getBean();

		StatementBuilder<RegularInsert> builder = getStatementFactory().insert(entityToUse, options, persistentEntity,
				tableName);

		if (source.isVersionedEntity()) {

			builder.apply(Insert::ifNotExists);
			return doInsertVersioned(builder.build(), entityToUse, source, tableName);
		}

		return doInsert(builder.build(), entityToUse, source, tableName);
	}

	private <T> ListenableFuture<EntityWriteResult<T>> doInsertVersioned(SimpleStatement insert, T entity,
			AdaptibleEntity<T> source, CqlIdentifier tableName) {

		return executeSave(entity, tableName, insert, result -> {

			if (!result.wasApplied()) {
				throw new OptimisticLockingFailureException(
						String.format("Cannot insert entity %s with version %s into table %s as it already exists", entity,
								source.getVersion(), tableName));
			}
		});
	}

	@SuppressWarnings("unused")
	private <T> ListenableFuture<EntityWriteResult<T>> doInsert(SimpleStatement insert, T entity,
			AdaptibleEntity<T> source, CqlIdentifier tableName) {

		return executeSave(entity, tableName, insert);
	}

	@Override
	public <T> ListenableFuture<T> update(T entity) {
		return new MappingListenableFutureAdapter<>(update(entity, UpdateOptions.empty()), EntityWriteResult::getEntity);
	}

	@Override
	public <T> ListenableFuture<EntityWriteResult<T>> update(T entity, UpdateOptions options) {

		Assert.notNull(entity, "Entity must not be null");
		Assert.notNull(options, "UpdateOptions must not be null");

		AdaptibleEntity<T> source = getEntityOperations().forEntity(entity, getConverter().getConversionService());
		CassandraPersistentEntity<?> persistentEntity = getRequiredPersistentEntity(entity.getClass());
		CqlIdentifier tableName = persistentEntity.getTableName();

		T entityToUpdate = maybeCallBeforeConvert(entity, tableName);

		return source.isVersionedEntity() ? doUpdateVersioned(entityToUpdate, options, tableName, persistentEntity)
				: doUpdate(entityToUpdate, options, tableName, persistentEntity);
	}

	private <T> ListenableFuture<EntityWriteResult<T>> doUpdateVersioned(T entity, UpdateOptions options,
			CqlIdentifier tableName, CassandraPersistentEntity<?> persistentEntity) {

		AdaptibleEntity<T> source = getEntityOperations().forEntity(entity, getConverter().getConversionService());
		Number previousVersion = source.getVersion();
		T toSave = source.incrementVersion();

		StatementBuilder<Update> update = getStatementFactory().update(toSave, options, persistentEntity, tableName);
		source.appendVersionCondition(update, previousVersion);

		return executeSave(toSave, tableName, update.build(), result -> {

			if (!result.wasApplied()) {
				throw new OptimisticLockingFailureException(
						String.format("Cannot save entity %s with version %s to table %s; Has it been modified meanwhile", toSave,
								source.getVersion(), tableName));
			}
		});
	}

	private <T> ListenableFuture<EntityWriteResult<T>> doUpdate(T entity, UpdateOptions options, CqlIdentifier tableName,
			CassandraPersistentEntity<?> persistentEntity) {

		StatementBuilder<Update> update = getStatementFactory().update(entity, options, persistentEntity, tableName);

		return executeSave(entity, tableName, update.build());
	}

	@Override
	public <T> ListenableFuture<T> delete(T entity) {
		return new MappingListenableFutureAdapter<>(delete(entity, QueryOptions.empty()), writeResult -> entity);
	}

	@Override
	public ListenableFuture<WriteResult> delete(Object entity, QueryOptions options) {

		Assert.notNull(entity, "Entity must not be null");
		Assert.notNull(options, "QueryOptions must not be null");

		AdaptibleEntity<Object> source = getEntityOperations().forEntity(entity, getConverter().getConversionService());
		CassandraPersistentEntity<?> persistentEntity = getRequiredPersistentEntity(entity.getClass());
		CqlIdentifier tableName = persistentEntity.getTableName();

		return source.isVersionedEntity() ? doDeleteVersioned(entity, options, source, tableName)
				: doDelete(entity, options, tableName);
	}

	private ListenableFuture<WriteResult> doDeleteVersioned(Object entity, QueryOptions options,
			AdaptibleEntity<Object> source, CqlIdentifier tableName) {

		StatementBuilder<Delete> delete = getStatementFactory().delete(entity, options, getConverter(), tableName);

		return executeDelete(entity, tableName, source.appendVersionCondition(delete).build(), result -> {

			if (!result.wasApplied()) {
				throw new OptimisticLockingFailureException(
						String.format("Cannot delete entity %s with version %s in table %s; Has it been modified meanwhile", entity,
								source.getVersion(), tableName));
			}
		});
	}

	private ListenableFuture<WriteResult> doDelete(Object entity, QueryOptions options, CqlIdentifier tableName) {

		StatementBuilder<Delete> delete = getStatementFactory().delete(entity, options, getConverter(), tableName);

		return executeDelete(entity, tableName, delete.build(), result -> {});
	}

	@Override
	public ListenableFuture<Boolean> deleteById(Object id, Class<?> entityClass) {

		Assert.notNull(id, "Id must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		CassandraPersistentEntity<?> entity = getRequiredPersistentEntity(entityClass);
		CqlIdentifier tableName = entity.getTableName();

		StatementBuilder<Delete> builder = getStatementFactory().deleteById(id, entity, tableName);
		SimpleStatement delete = builder.build();

		maybeEmitEvent(() -> new BeforeDeleteEvent<>(delete, entityClass, tableName));

		ListenableFuture<Boolean> future = doExecute(delete, AsyncResultSet::wasApplied);
		future.addCallback(success -> maybeEmitEvent(() -> new AfterDeleteEvent<>(delete, entityClass, tableName)),
				e -> {});

		return future;
	}

	@Override
	public ListenableFuture<Void> truncate(Class<?> entityClass) {

		Assert.notNull(entityClass, "Entity type must not be null");

		CassandraPersistentEntity<?> entity = getRequiredPersistentEntity(entityClass);
		CqlIdentifier tableName = entity.getTableName();

		Truncate truncate = QueryBuilder.truncate(entity.getKeyspace(), tableName);
		SimpleStatement statement = truncate.build();

		maybeEmitEvent(() -> new BeforeDeleteEvent<>(statement, entityClass, tableName));

		ListenableFuture<Boolean> future = doExecute(statement, AsyncResultSet::wasApplied);
		future.addCallback(success -> maybeEmitEvent(() -> new AfterDeleteEvent<>(statement, entityClass, tableName)),
				e -> {});

		return new MappingListenableFutureAdapter<>(future, aBoolean -> null);
	}

	// -------------------------------------------------------------------------
	// Implementation hooks and utility methods
	// -------------------------------------------------------------------------

	/**
	 * Create a new statement-based {@link AsyncPreparedStatementHandler} using the statement passed in.
	 * <p>
	 * This method allows for the creation to be overridden by subclasses.
	 *
	 * @param statement the statement to be prepared.
	 * @return the new {@link PreparedStatementHandler} to use.
	 * @since 3.3.3
	 */
	protected AsyncPreparedStatementHandler createPreparedStatementHandler(Statement<?> statement) {
		return new PreparedStatementHandler(statement, exceptionTranslator);
	}

	private <T> ListenableFuture<EntityWriteResult<T>> executeSave(T entity, CqlIdentifier tableName,
			SimpleStatement statement) {

		return executeSave(entity, tableName, statement, ignore -> {});
	}

	private <T> ListenableFuture<EntityWriteResult<T>> executeSave(T entity, CqlIdentifier tableName,
			SimpleStatement statement, Consumer<WriteResult> beforeAfterSaveEvent) {

		maybeEmitEvent(() -> new BeforeSaveEvent<>(entity, tableName, statement));
		T entityToSave = maybeCallBeforeSave(entity, tableName, statement);

		ListenableFuture<AsyncResultSet> result = doQueryForResultSet(statement);

		return new MappingListenableFutureAdapter<>(result, resultSet -> {

			EntityWriteResult<T> writeResult = new org.springframework.data.cassandra.core.legacy.EntityWriteResult<>(
					Collections.singletonList(resultSet.getExecutionInfo()), resultSet.wasApplied(), getFirstPage(resultSet),
					entityToSave);

			beforeAfterSaveEvent.accept(writeResult);

			maybeEmitEvent(() -> new AfterSaveEvent<>(entityToSave, tableName));

			return writeResult;
		});
	}

	private ListenableFuture<WriteResult> executeDelete(Object entity, CqlIdentifier tableName, SimpleStatement statement,
			Consumer<WriteResult> resultConsumer) {

		maybeEmitEvent(() -> new BeforeDeleteEvent<>(statement, entity.getClass(), tableName));

		ListenableFuture<AsyncResultSet> result = doQueryForResultSet(statement);

		return new MappingListenableFutureAdapter<>(result, resultSet -> {

			WriteResult writeResult = new org.springframework.data.cassandra.core.legacy.WriteResult(
					Collections.singletonList(resultSet.getExecutionInfo()), resultSet.wasApplied(), getFirstPage(resultSet));

			resultConsumer.accept(writeResult);

			maybeEmitEvent(() -> new AfterDeleteEvent<>(statement, entity.getClass(), tableName));

			return writeResult;
		});
	}

	private <T> ListenableFuture<List<T>> doQuery(Statement<?> statement, RowMapper<T> rowMapper) {

		if (PreparedStatementDelegate.canPrepare(isUsePreparedStatements(), statement, log)) {

			AsyncPreparedStatementHandler statementHandler = createPreparedStatementHandler(statement);
			return getAsyncCqlOperations().query(statementHandler, statementHandler, rowMapper);
		}

		return getAsyncCqlOperations().query(statement, rowMapper);
	}

	private ListenableFuture<Void> doQuery(Statement<?> statement, RowCallbackHandler callbackHandler) {

		if (PreparedStatementDelegate.canPrepare(isUsePreparedStatements(), statement, log)) {

			AsyncPreparedStatementHandler statementHandler = createPreparedStatementHandler(statement);
			return getAsyncCqlOperations().query(statementHandler, statementHandler, callbackHandler);
		}

		return getAsyncCqlOperations().query(statement, callbackHandler);
	}

	private ListenableFuture<AsyncResultSet> doQueryForResultSet(Statement<?> statement) {
		return doExecute(statement, Function.identity());
	}

	private <T> ListenableFuture<T> doExecute(Statement<?> statement, Function<AsyncResultSet, T> mappingFunction) {

		if (PreparedStatementDelegate.canPrepare(isUsePreparedStatements(), statement, log)) {

			AsyncPreparedStatementHandler statementHandler = createPreparedStatementHandler(statement);
			return getAsyncCqlOperations().query(statementHandler, statementHandler,
					(AsyncResultSetExtractor<T>) resultSet -> new AsyncResult<>(mappingFunction.apply(resultSet)));
		}

		return new MappingListenableFutureAdapter<>(getAsyncCqlOperations().queryForResultSet(statement), mappingFunction);
	}

	private static List<Row> getFirstPage(AsyncResultSet resultSet) {
		return StreamSupport.stream(resultSet.currentPage().spliterator(), false).collect(Collectors.toList());
	}

	private static int getConfiguredPageSize(CqlSession session) {
		return session.getContext().getConfig().getDefaultProfile().getInt(DefaultDriverOption.REQUEST_PAGE_SIZE, 5000);
	}

	private int getEffectivePageSize(Statement<?> statement) {

		if (statement.getPageSize() > 0) {
			return statement.getPageSize();
		}

		if (getAsyncCqlOperations() instanceof CassandraAccessor) {

			CassandraAccessor accessor = (CassandraAccessor) getAsyncCqlOperations();

			if (accessor.getFetchSize() != -1) {
				return accessor.getFetchSize();
			}
		}
		class GetConfiguredPageSize implements AsyncSessionCallback<Integer>, CqlProvider {
			@Override
			public ListenableFuture<Integer> doInSession(CqlSession session) {
				return AsyncResult.forValue(getConfiguredPageSize(session));
			}

			@Override
			public String getCql() {
				return QueryExtractorDelegate.getCql(statement);
			}
		}

		return getAsyncCqlOperations().execute(new GetConfiguredPageSize()).completable().join();
	}

	@SuppressWarnings("unchecked")
	private <T> Function<Row, T> getMapper(Class<?> entityType, Class<T> targetType, CqlIdentifier tableName) {

		EntityProjection<T, ?> projection = entityOperations.introspectProjection(targetType, entityType);

		return row -> {

			maybeEmitEvent(() -> new AfterLoadEvent<>(row, targetType, tableName));

			T result = getConverter().project(projection, row);

			if (result != null) {
				maybeEmitEvent(() -> new AfterConvertEvent<>(row, result, tableName));
			}

			return result;
		};
	}

	private static MappingCassandraConverter newConverter(CqlSession session) {

		MappingCassandraConverter converter = new MappingCassandraConverter();
		converter.setUserTypeResolver(new SimpleUserTypeResolver(session));
		converter.setCodecRegistry(session.getContext().getCodecRegistry());

		converter.afterPropertiesSet();

		return converter;
	}

	protected <E extends CassandraMappingEvent<T>, T> void maybeEmitEvent(Supplier<E> event) {
		this.eventDelegate.publishEvent(event);
	}

	protected <T> T maybeCallBeforeConvert(T object, CqlIdentifier tableName) {

		if (null != entityCallbacks) {
			return entityCallbacks.callback(BeforeConvertCallback.class, object, tableName);
		}

		return object;
	}

	protected <T> T maybeCallBeforeSave(T object, CqlIdentifier tableName, Statement<?> statement) {

		if (null != entityCallbacks) {
			return entityCallbacks.callback(BeforeSaveCallback.class, object, tableName, statement);
		}

		return object;
	}

	static class MappingListenableFutureAdapter<T, S>
			extends org.springframework.util.concurrent.ListenableFutureAdapter<T, S> {

		private final Function<S, T> mapper;

		MappingListenableFutureAdapter(ListenableFuture<S> adaptee, Function<S, T> mapper) {
			super(adaptee);
			this.mapper = mapper;
		}

		@Override
		protected T adapt(@Nullable S adapteeResult) {
			return this.mapper.apply(adapteeResult);
		}
	}

	/**
	 * General callback interface used to create and bind prepared CQL statements.
	 * <p>
	 * This interface prepares the CQL statement and sets values on a {@link PreparedStatement} as union-type comprised
	 * from {@link AsyncPreparedStatementCreator}, {@link PreparedStatementBinder}, and {@link CqlProvider}.
	 *
	 * @since 3.3.3
	 */
	public interface AsyncPreparedStatementHandler
			extends AsyncPreparedStatementCreator, PreparedStatementBinder, CqlProvider {

	}

	/**
	 * Utility class to prepare a {@link SimpleStatement} and bind values associated with the statement to a
	 * {@link BoundStatement}.
	 *
	 * @since 3.2
	 */
	public static class PreparedStatementHandler implements AsyncPreparedStatementHandler {

		private final SimpleStatement statement;
		private final PersistenceExceptionTranslator exceptionTranslator;

		public PreparedStatementHandler(Statement<?> statement, PersistenceExceptionTranslator exceptionTranslator) {
			this.statement = PreparedStatementDelegate.getStatementForPrepare(statement);
			this.exceptionTranslator = exceptionTranslator;
		}

		@Override
		public ListenableFuture<PreparedStatement> createPreparedStatement(CqlSession session) throws DriverException {
			return new CassandraFutureAdapter<>(doPrepare(session), exceptionTranslator);
		}

		/**
		 * Invokes the statement preparation.
		 *
		 * @param session
		 * @return
		 */
		protected CompletionStage<PreparedStatement> doPrepare(CqlSession session) {
			return session.prepareAsync(statement.getQuery());
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
