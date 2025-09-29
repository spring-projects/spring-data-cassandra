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

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jspecify.annotations.Nullable;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.dao.support.DataAccessUtils;
import org.springframework.data.cassandra.SessionFactory;
import org.springframework.data.cassandra.core.EntityOperations.AdaptibleEntity;
import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.cql.*;
import org.springframework.data.cassandra.core.cql.session.DefaultSessionFactory;
import org.springframework.data.cassandra.core.cql.util.StatementBuilder;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.SimpleUserTypeResolver;
import org.springframework.data.cassandra.core.mapping.UserTypeResolver;
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
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
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
 * Primary implementation of {@link CassandraOperations}. It simplifies the use of Cassandra usage and helps to avoid
 * common errors. It executes core Cassandra workflow. This class executes CQL queries or updates, initiating iteration
 * over {@link ResultSet} and catching Cassandra exceptions and translating them to the generic, more informative
 * exception hierarchy defined in the {@code org.springframework.dao} package.
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
 * @author Lukasz Antoniak
 * @author Sam Lightfoot
 * @see org.springframework.data.cassandra.core.CassandraOperations
 * @since 2.0
 */
public class CassandraTemplate implements CassandraOperations, ApplicationEventPublisherAware, ApplicationContextAware {

	private final Log log = LogFactory.getLog(getClass());

	private final CqlOperations cqlOperations;

	private final EntityLifecycleEventDelegate eventDelegate;

	private final CassandraConverter converter;

	private final StatementFactory statementFactory;

	private final EntityOperations entityOperations;

	private final QueryOperations queryOperations;

	private @Nullable EntityCallbacks entityCallbacks;

	private boolean usePreparedStatements = true;

	/**
	 * Creates an instance of {@link CassandraTemplate} initialized with the given {@link CqlSession} and a default
	 * {@link MappingCassandraConverter}.
	 *
	 * @param session {@link CqlSession} used to interact with Cassandra; must not be {@literal null}.
	 * @see CassandraConverter
	 * @see CqlSession
	 */
	public CassandraTemplate(CqlSession session) {
		this(session, createConverter(session));
	}

	/**
	 * Creates an instance of {@link CassandraTemplate} initialized with the given {@link CqlSession} and
	 * {@link CassandraConverter}.
	 *
	 * @param session {@link CqlSession} used to interact with Cassandra; must not be {@literal null}.
	 * @param converter {@link CassandraConverter} used to convert between Java and Cassandra types; must not be
	 *          {@literal null}.
	 * @see CassandraConverter
	 * @see CqlSession
	 */
	public CassandraTemplate(CqlSession session, CassandraConverter converter) {
		this(new DefaultSessionFactory(session), converter);
	}

	/**
	 * Creates an instance of {@link CassandraTemplate} initialized with the given {@link SessionFactory} and
	 * {@link CassandraConverter}.
	 *
	 * @param sessionFactory {@link SessionFactory} used to interact with Cassandra; must not be {@literal null}.
	 * @param converter {@link CassandraConverter} used to convert between Java and Cassandra types; must not be
	 *          {@literal null}.
	 * @see CassandraConverter
	 * @see SessionFactory
	 */
	public CassandraTemplate(SessionFactory sessionFactory, CassandraConverter converter) {
		this(new CqlTemplate(sessionFactory), converter);
	}

	/**
	 * Creates an instance of {@link CassandraTemplate} initialized with the given {@link CqlOperations} and
	 * {@link CassandraConverter}.
	 *
	 * @param cqlOperations {@link CqlOperations} used to interact with Cassandra; must not be {@literal null}.
	 * @param converter {@link CassandraConverter} used to convert between Java and Cassandra types; must not be
	 *          {@literal null}.
	 * @see CassandraConverter
	 * @see CqlSession
	 */
	public CassandraTemplate(CqlOperations cqlOperations, CassandraConverter converter) {

		Assert.notNull(cqlOperations, "CqlOperations must not be null");
		Assert.notNull(converter, "CassandraConverter must not be null");

		this.converter = converter;
		this.eventDelegate = new EntityLifecycleEventDelegate();
		this.cqlOperations = cqlOperations;
		this.statementFactory = new StatementFactory(converter);
		this.entityOperations = new EntityOperations(converter);
		this.queryOperations = new QueryOperations(converter, this.statementFactory, this.eventDelegate);
	}

	@Override
	public CassandraBatchOperations batchOps(BatchType batchType) {
		return new CassandraBatchTemplate(this, batchType);
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
	public CqlOperations getCqlOperations() {
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

	@Override
	public CqlIdentifier getTableName(Class<?> entityClass) {
		return queryOperations.getTableName(entityClass);
	}

	// -------------------------------------------------------------------------
	// Methods dealing with static CQL
	// -------------------------------------------------------------------------

	@Override
	public <T> List<T> select(String cql, Class<T> entityClass) {

		Assert.hasText(cql, "CQL must not be empty");

		return select(SimpleStatement.newInstance(cql), entityClass);
	}

	@Override
	public <T> @Nullable T selectOne(String cql, Class<T> entityClass) {

		Assert.hasText(cql, "CQL must not be empty");
		Assert.notNull(entityClass, "Entity type must not be null");

		return selectOne(SimpleStatement.newInstance(cql), entityClass);
	}

	@Override
	public <T> Stream<T> stream(String cql, Class<T> entityClass) throws DataAccessException {

		Assert.hasText(cql, "CQL must not be empty");
		Assert.notNull(entityClass, "Entity type must not be null");

		return stream(SimpleStatement.newInstance(cql), entityClass);
	}

	// -------------------------------------------------------------------------
	// Methods dealing with com.datastax.oss.driver.api.core.cql.Statement
	// -------------------------------------------------------------------------

	@Override
	public ResultSet execute(Statement<?> statement) {

		Assert.notNull(statement, "Statement must not be null");

		return doQueryForResultSet(statement);
	}

	@Override
	public <T> List<T> select(Statement<?> statement, Class<T> entityClass) {

		Assert.notNull(statement, "Statement must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		RowMapper<T> rowMapper = queryOperations.getRowMapper(entityClass, statement);
		return doQuery(statement, rowMapper);
	}

	@Override
	public <T> @Nullable T selectOne(Statement<?> statement, Class<T> entityClass) {

		List<T> result = select(statement, entityClass);
		return result.isEmpty() ? null : result.get(0);
	}

	@Override
	public <T> Slice<T> slice(Statement<?> statement, Class<T> entityClass) {

		Assert.notNull(statement, "Statement must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		RowMapper<T> rowMapper = queryOperations.getRowMapper(entityClass, statement);
		return doSlice(statement, rowMapper);
	}

	@Override
	public <T> Stream<T> stream(Statement<?> statement, Class<T> entityClass) throws DataAccessException {

		Assert.notNull(statement, "Statement must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		RowMapper<T> rowMapper = queryOperations.getRowMapper(entityClass, statement);
		return doStream(statement, rowMapper);
	}

	// -------------------------------------------------------------------------
	// Methods dealing with org.springframework.data.cassandra.core.query.Query
	// -------------------------------------------------------------------------

	@Override
	public <T> List<T> select(Query query, Class<T> entityClass) throws DataAccessException {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		return queryOperations.select(entityClass).matching(query).select(this::doQuery);
	}

	ResultSet doSelectResultSet(Query query, Class<?> entityClass, CqlIdentifier tableName) {

		CassandraPersistentEntity<?> entity = getRequiredPersistentEntity(entityClass);

		StatementBuilder<Select> select = getStatementFactory().select(query, entity, tableName);
		SimpleStatement statement = select.build();

		return queryForResultSet(statement);
	}

	ResultSet queryForResultSet(Statement<?> statement) {

		if (PreparedStatementDelegate.canPrepare(isUsePreparedStatements(), statement, log)) {

			PreparedStatementHandler statementHandler = createPreparedStatementHandler(statement);
			return getCqlOperations().query(statementHandler, statementHandler, resultSet -> resultSet);
		}

		return getCqlOperations().queryForResultSet(statement);
	}

	@Override
	public <T> @Nullable T selectOne(Query query, Class<T> entityClass) throws DataAccessException {

		List<T> result = select(query, entityClass);
		return result.isEmpty() ? null : result.get(0);
	}

	@Override
	public <T> Slice<T> slice(Query query, Class<T> entityClass) throws DataAccessException {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		return queryOperations.select(entityClass).matching(query).select(this::doSlice);
	}

	@Override
	public <T> Stream<T> stream(Query query, Class<T> entityClass) throws DataAccessException {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		return queryOperations.select(entityClass).matching(query).select(this::doStream);
	}

	@Override
	public boolean update(Query query, org.springframework.data.cassandra.core.query.Update update, Class<?> entityClass)
			throws DataAccessException {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(update, "Update must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		StatementBuilder<Update> updateStatement = getStatementFactory().update(query, update,
				getRequiredPersistentEntity(entityClass));

		return doExecute(updateStatement.build()).wasApplied();
	}

	WriteResult doUpdate(Query query, org.springframework.data.cassandra.core.query.Update update, Class<?> entityClass,
			CqlIdentifier tableName) {

		StatementBuilder<Update> updateStatement = getStatementFactory().update(query, update,
				getRequiredPersistentEntity(entityClass), tableName);

		return doExecute(updateStatement.build());
	}

	@Override
	public boolean delete(Query query, Class<?> entityClass) throws DataAccessException {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		WriteResult result = doDelete(query, entityClass, getTableName(entityClass));

		return result.wasApplied();
	}

	WriteResult doDelete(Query query, Class<?> entityClass, CqlIdentifier tableName) {

		StatementBuilder<Delete> delete = getStatementFactory().delete(query, getRequiredPersistentEntity(entityClass),
				tableName);
		SimpleStatement statement = delete.build();

		maybeEmitEvent(() -> new BeforeDeleteEvent<>(statement, entityClass, tableName));

		WriteResult writeResult = doExecute(statement);

		maybeEmitEvent(() -> new AfterDeleteEvent<>(statement, entityClass, tableName));

		return writeResult;
	}

	// -------------------------------------------------------------------------
	// Methods dealing with entities
	// -------------------------------------------------------------------------

	@Override
	public long count(Class<?> entityClass) {

		Assert.notNull(entityClass, "Entity type must not be null");

		return count(Query.empty(), entityClass);
	}

	@Override
	public long count(Query query, Class<?> entityClass) throws DataAccessException {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		return doCount(query, entityClass, getTableName(entityClass));
	}

	long doCount(Query query, Class<?> entityClass, CqlIdentifier tableName) {

		StatementBuilder<Select> countStatement = getStatementFactory().count(query,
				getRequiredPersistentEntity(entityClass), tableName);

		return doQueryForRequiredObject(countStatement.build(), Long.class);
	}

	@Override
	public boolean exists(Object id, Class<?> entityClass) {

		Assert.notNull(id, "Id must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		return queryOperations.select(entityClass).matchingId(id)
				.exists((statement) -> doExecute(statement, rs -> rs.one() != null));
	}

	@Override
	public boolean exists(Query query, Class<?> entityClass) throws DataAccessException {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		return doExists(query, entityClass, getTableName(entityClass));
	}

	boolean doExists(Query query, Class<?> entityClass, CqlIdentifier tableName) {

		return queryOperations.select(entityClass, tableName).matching(query)
				.exists((statement) -> doExecute(statement, rs -> rs.one() != null));
	}

	@Override
	public <T> @Nullable T selectOneById(Object id, Class<T> entityClass) {

		Assert.notNull(id, "Id must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		List<T> select = queryOperations.select(entityClass).matchingId(id).select(this::doQuery);

		return DataAccessUtils.singleResult(select);
	}

	@Override
	public <T> T insert(T entity) {
		return insert(entity, InsertOptions.empty()).getEntity();
	}

	@Override
	public <T> EntityWriteResult<T> insert(T entity, InsertOptions options) {

		Assert.notNull(entity, "Entity must not be null");
		Assert.notNull(options, "InsertOptions must not be null");

		return doInsert(entity, options, getTableName(entity.getClass()));
	}

	<T> EntityWriteResult<T> doInsert(T entity, WriteOptions options, CqlIdentifier tableName) {

		AdaptibleEntity<T> source = entityOperations.forEntity(maybeCallBeforeConvert(entity, tableName),
				getConverter().getConversionService());

		T entityToUse = source.isVersionedEntity() ? source.initializeVersionProperty() : source.getBean();

		StatementBuilder<RegularInsert> builder = getStatementFactory().insert(entityToUse, options,
				source.getPersistentEntity(), tableName);

		if (source.isVersionedEntity()) {

			builder.apply(Insert::ifNotExists);
			return doInsertVersioned(builder.build(), entityToUse, source, tableName);
		}

		return doInsert(builder.build(), entityToUse, tableName);
	}

	private <T> EntityWriteResult<T> doInsertVersioned(SimpleStatement insert, T entity, AdaptibleEntity<T> source,
			CqlIdentifier tableName) {

		return executeSave(entity, tableName, insert, result -> {

			if (!result.wasApplied()) {
				throw new OptimisticLockingFailureException(
						String.format("Cannot insert entity %s with version %s into table %s as it already exists", entity,
								source.getVersion(), tableName));
			}
		});
	}

	private <T> EntityWriteResult<T> doInsert(SimpleStatement insert, T entity, CqlIdentifier tableName) {
		return executeSave(entity, tableName, insert);
	}

	@Override
	public <T> T update(T entity) {
		return update(entity, UpdateOptions.empty()).getEntity();
	}

	@Override
	public <T> EntityWriteResult<T> update(T entity, UpdateOptions options) {

		Assert.notNull(entity, "Entity must not be null");
		Assert.notNull(options, "UpdateOptions must not be null");

		AdaptibleEntity<T> source = entityOperations.forEntity(entity, getConverter().getConversionService());
		CassandraPersistentEntity<?> persistentEntity = getRequiredPersistentEntity(entity.getClass());
		CqlIdentifier tableName = persistentEntity.getTableName();

		T entityToUpdate = maybeCallBeforeConvert(entity, tableName);

		return source.isVersionedEntity() ? doUpdateVersioned(entityToUpdate, options, tableName, persistentEntity)
				: doUpdate(entityToUpdate, options, tableName, persistentEntity);
	}

	private <T> EntityWriteResult<T> doUpdateVersioned(T entity, UpdateOptions options, CqlIdentifier tableName,
			CassandraPersistentEntity<?> persistentEntity) {

		AdaptibleEntity<T> source = entityOperations.forEntity(entity, getConverter().getConversionService());

		Number previousVersion = source.getVersion();
		T toSave = source.incrementVersion();

		StatementBuilder<Update> builder = getStatementFactory().update(toSave, options, persistentEntity, tableName);
		SimpleStatement update = source.appendVersionCondition(builder, previousVersion).build();

		return executeSave(toSave, tableName, update, result -> {

			if (!result.wasApplied()) {
				throw new OptimisticLockingFailureException(
						String.format("Cannot save entity %s with version %s to table %s; Has it been modified meanwhile", toSave,
								source.getVersion(), tableName));
			}
		});
	}

	private <T> EntityWriteResult<T> doUpdate(T entity, UpdateOptions options, CqlIdentifier tableName,
			CassandraPersistentEntity<?> persistentEntity) {

		StatementBuilder<Update> builder = getStatementFactory().update(entity, options, persistentEntity, tableName);

		return executeSave(entity, tableName, builder.build());
	}

	@Override
	public void delete(Object entity) {
		delete(entity, QueryOptions.empty());
	}

	@Override
	public WriteResult delete(Object entity, QueryOptions options) {

		Assert.notNull(entity, "Entity must not be null");
		Assert.notNull(options, "QueryOptions must not be null");

		AdaptibleEntity<Object> source = entityOperations.forEntity(entity, getConverter().getConversionService());
		CassandraPersistentEntity<?> persistentEntity = getRequiredPersistentEntity(entity.getClass());
		CqlIdentifier tableName = persistentEntity.getTableName();

		StatementBuilder<Delete> builder = getStatementFactory().delete(entity, options, getConverter(), tableName);

		return source.isVersionedEntity()
				? doDeleteVersioned(source.appendVersionCondition(builder).build(), entity, source, tableName)
				: doDelete(builder.build(), entity, tableName);
	}

	private WriteResult doDeleteVersioned(SimpleStatement statement, Object entity, AdaptibleEntity<Object> source,
			CqlIdentifier tableName) {

		return executeDelete(entity, tableName, statement, result -> {

			if (!result.wasApplied()) {
				throw new OptimisticLockingFailureException(
						String.format("Cannot delete entity %s with version %s in table %s; Has it been modified meanwhile", entity,
								source.getVersion(), tableName));
			}
		});
	}

	private WriteResult doDelete(SimpleStatement delete, Object entity, CqlIdentifier tableName) {
		return executeDelete(entity, tableName, delete, result -> {});
	}

	@Override
	public boolean deleteById(Object id, Class<?> entityClass) {

		Assert.notNull(id, "Id must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		CassandraPersistentEntity<?> entity = getRequiredPersistentEntity(entityClass);
		CqlIdentifier tableName = entity.getTableName();

		StatementBuilder<Delete> delete = getStatementFactory().deleteById(id, entity, tableName);
		SimpleStatement statement = delete.build();

		maybeEmitEvent(() -> new BeforeDeleteEvent<>(statement, entityClass, tableName));

		boolean result = doExecute(statement).wasApplied();

		maybeEmitEvent(() -> new AfterDeleteEvent<>(statement, entityClass, tableName));

		return result;
	}

	@Override
	public void truncate(Class<?> entityClass) {

		Assert.notNull(entityClass, "Entity type must not be null");

		CassandraPersistentEntity<?> entity = getRequiredPersistentEntity(entityClass);
		CqlIdentifier tableName = entity.getTableName();

		Truncate truncate = QueryBuilder.truncate(entity.getKeyspace(), tableName);
		SimpleStatement statement = truncate.build();

		maybeEmitEvent(() -> new BeforeDeleteEvent<>(statement, entityClass, tableName));

		doExecute(statement);

		maybeEmitEvent(() -> new AfterDeleteEvent<>(statement, entityClass, tableName));
	}

	// -------------------------------------------------------------------------
	// Fluent API entry points
	// -------------------------------------------------------------------------

	@Override
	public <T> ExecutableSelect<T> query(Class<T> domainType) {
		return new ExecutableSelectOperationSupport(this, this.queryOperations).query(domainType);
	}

	@Override
	public UntypedSelect query(String cql) {
		return new ExecutableSelectOperationSupport(this, this.queryOperations).query(cql);
	}

	@Override
	public UntypedSelect query(Statement<?> statement) {
		return new ExecutableSelectOperationSupport(this, this.queryOperations).query(statement);
	}

	@Override
	public <T> ExecutableInsert<T> insert(Class<T> domainType) {
		return new ExecutableInsertOperationSupport(this).insert(domainType);
	}

	@Override
	public ExecutableUpdate update(Class<?> domainType) {
		return new ExecutableUpdateOperationSupport(this).update(domainType);
	}

	@Override
	public ExecutableDelete delete(Class<?> domainType) {
		return new ExecutableDeleteOperationSupport(this).delete(domainType);
	}

	// -------------------------------------------------------------------------
	// Implementation hooks and utility methods
	// -------------------------------------------------------------------------

	/**
	 * Create a new statement-based {@link PreparedStatementHandler} using the statement passed in.
	 * <p>
	 * This method allows for the creation to be overridden by subclasses.
	 *
	 * @param statement the statement to be prepared.
	 * @return the new {@link PreparedStatementHandler} to use.
	 * @since 3.3.3
	 */
	protected PreparedStatementHandler createPreparedStatementHandler(Statement<?> statement) {
		return new PreparedStatementHandler(statement);
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

	<T> List<T> doQuery(Statement<?> statement, RowMapper<T> rowMapper) {

		if (PreparedStatementDelegate.canPrepare(isUsePreparedStatements(), statement, log)) {

			PreparedStatementHandler statementHandler = createPreparedStatementHandler(statement);
			return getCqlOperations().query(statementHandler, statementHandler, rowMapper);
		}

		return getCqlOperations().query(statement, rowMapper);
	}

	<T> Slice<T> doSlice(Statement<?> statement, RowMapper<T> mapper) {

		ResultSet resultSet = doQueryForResultSet(statement);
		return EntityQueryUtils.readSlice(resultSet, mapper, 0, getEffectivePageSize(statement));
	}

	<T> Stream<T> doStream(Statement<?> statement, RowMapper<T> rowMapper) {

		if (PreparedStatementDelegate.canPrepare(isUsePreparedStatements(), statement, log)) {

			PreparedStatementHandler statementHandler = createPreparedStatementHandler(statement);
			return getCqlOperations().queryForStream(statementHandler, statementHandler, rowMapper);
		}

		return getCqlOperations().queryForStream(statement, rowMapper);
	}

	private <T> EntityWriteResult<T> executeSave(T entity, CqlIdentifier tableName, SimpleStatement statement) {
		return executeSave(entity, tableName, statement, ignore -> {});
	}

	private <T> EntityWriteResult<T> executeSave(T entity, CqlIdentifier tableName, SimpleStatement statement,
			Consumer<WriteResult> resultConsumer) {

		maybeEmitEvent(() -> new BeforeSaveEvent<>(entity, tableName, statement));
		T entityToSave = maybeCallBeforeSave(entity, tableName, statement);

		WriteResult result = doExecute(statement);
		resultConsumer.accept(result);

		maybeEmitEvent(() -> new AfterSaveEvent<>(entityToSave, tableName));

		return EntityWriteResult.of(result, entityToSave);
	}

	private WriteResult executeDelete(Object entity, CqlIdentifier tableName, SimpleStatement statement,
			Consumer<WriteResult> resultConsumer) {

		maybeEmitEvent(() -> new BeforeDeleteEvent<>(statement, entity.getClass(), tableName));

		WriteResult result = doExecute(statement);

		resultConsumer.accept(result);

		maybeEmitEvent(() -> new AfterDeleteEvent<>(statement, entity.getClass(), tableName));

		return result;
	}

	private <T> T doQueryForRequiredObject(Statement<?> statement, Class<T> resultType) {
		return DataAccessUtils.requiredSingleResult(doQuery(statement, SingleColumnRowMapper.newInstance(resultType)));
	}

	private WriteResult doExecute(SimpleStatement statement) {
		return doExecute(statement, WriteResult::of);
	}

	private ResultSet doQueryForResultSet(Statement<?> statement) {
		return doExecute(statement, Function.identity());
	}

	private <T> T doExecute(Statement<?> statement, Function<ResultSet, T> mappingFunction) {

		if (PreparedStatementDelegate.canPrepare(isUsePreparedStatements(), statement, log)) {

			PreparedStatementHandler statementHandler = createPreparedStatementHandler(statement);
			return getCqlOperations().query(statementHandler, statementHandler, mappingFunction::apply);
		}

		return mappingFunction.apply(getCqlOperations().queryForResultSet(statement));
	}

	private CassandraPersistentEntity<?> getRequiredPersistentEntity(Class<?> entityType) {
		return queryOperations.getRequiredPersistentEntity(entityType);
	}

	/**
	 * Create a new {@link MappingCassandraConverter} given the {@link CqlSession}.
	 *
	 * @param session the session to be used for
	 *          {@link com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry} and user type resolution.
	 * @return a new and initialized {@link MappingCassandraConverter}.
	 */
	static MappingCassandraConverter createConverter(CqlSession session) {
		return createConverter(new SimpleUserTypeResolver(session), session.getContext());
	}

	/**
	 * Create a new {@link MappingCassandraConverter}.
	 *
	 * @param userTypeResolver the user type resolver to be used.
	 * @param context the driver context to be used for codec registry access.
	 * @return a new and initialized {@link MappingCassandraConverter}.
	 */
	static MappingCassandraConverter createConverter(UserTypeResolver userTypeResolver, DriverContext context) {

		MappingCassandraConverter converter = new MappingCassandraConverter();
		converter.setUserTypeResolver(userTypeResolver);
		converter.setCodecRegistry(context.getCodecRegistry());

		converter.afterPropertiesSet();

		return converter;
	}

	static int getConfiguredPageSize(CqlSession session) {
		return getConfiguredPageSize(session.getContext());
	}

	static int getConfiguredPageSize(DriverContext context) {
		return context.getConfig().getDefaultProfile().getInt(DefaultDriverOption.REQUEST_PAGE_SIZE, 5000);
	}

	@SuppressWarnings("ConstantConditions")
	private int getEffectivePageSize(Statement<?> statement) {

		if (statement.getPageSize() > 0) {
			return statement.getPageSize();
		}

		if (getCqlOperations() instanceof CassandraAccessor accessor) {

			if (accessor.getPageSize() != -1) {
				return accessor.getPageSize();
			}
		}

		class GetConfiguredPageSize implements SessionCallback<Integer>, CqlProvider {
			@Override
			public Integer doInSession(CqlSession session) {
				return getConfiguredPageSize(session);
			}

			@Override
			public String getCql() {
				return QueryExtractorDelegate.getCql(statement);
			}
		}

		return getCqlOperations().execute(new GetConfiguredPageSize());
	}

	/**
	 * Utility class to prepare a {@link SimpleStatement} and bind values associated with the statement to a
	 * {@link BoundStatement}.
	 *
	 * @since 3.2
	 */
	public static class PreparedStatementHandler
			implements PreparedStatementCreator, PreparedStatementBinder, CqlProvider {

		private final SimpleStatement statement;

		public PreparedStatementHandler(Statement<?> statement) {
			this.statement = PreparedStatementDelegate.getStatementForPrepare(statement);
		}

		@Override
		public PreparedStatement createPreparedStatement(CqlSession session) throws DriverException {

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
