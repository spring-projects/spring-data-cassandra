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
package org.springframework.data.cassandra.core.cql;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import org.reactivestreams.Publisher;

import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.DataAccessUtils;
import org.springframework.data.cassandra.ReactiveResultSet;
import org.springframework.data.cassandra.ReactiveSession;
import org.springframework.data.cassandra.ReactiveSessionFactory;
import org.springframework.data.cassandra.core.cql.session.DefaultReactiveSessionFactory;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;

/**
 * <b>This is the central class in the CQL core package for reactive Cassandra data access.</b> It simplifies the use of
 * CQL and helps to avoid common errors. It executes core CQL workflow, leaving application code to provide CQL and
 * extract results. This class executes CQL queries or updates, initiating iteration over {@link ReactiveResultSet}s and
 * catching {@link DriverException} exceptions and translating them to the generic, more informative exception hierarchy
 * defined in the {@code org.springframework.dao} package.
 * <p>
 * Code using this class need only implement callback interfaces, giving them a clearly defined contract. The
 * {@link PreparedStatementCreator} callback interface creates a prepared statement given a Connection, providing CQL
 * and any necessary parameters. The {@link ResultSetExtractor} interface extracts values from a
 * {@link ReactiveResultSet}. See also {@link PreparedStatementBinder} and {@link RowMapper} for two popular alternative
 * callback interfaces.
 * <p>
 * Can be used within a service implementation via direct instantiation with a {@link ReactiveSessionFactory} reference,
 * or get prepared in an application context and given to services as bean reference. Note: The
 * {@link ReactiveSessionFactory} should always be configured as a bean in the application context, in the first case
 * given to the service directly, in the second case to the prepared template.
 * <p>
 * Because this class is parameterizable by the callback interfaces and the
 * {@link org.springframework.dao.support.PersistenceExceptionTranslator} interface, there should be no need to subclass
 * it.
 * <p>
 * All CQL operations performed by this class are logged at debug level, using
 * "org.springframework.data.cassandra.core.cql.ReactiveCqlTemplate" as log category.
 * <p>
 * <b>NOTE: An instance of this class is thread-safe once configured.</b>
 *
 * @author Mark Paluch
 * @author Tomasz Lelek
 * @since 2.0
 * @see PreparedStatementCreator
 * @see PreparedStatementBinder
 * @see PreparedStatementCallback
 * @see ResultSetExtractor
 * @see RowCallbackHandler
 * @see RowMapper
 * @see org.springframework.dao.support.PersistenceExceptionTranslator
 */
public class ReactiveCqlTemplate extends ReactiveCassandraAccessor implements ReactiveCqlOperations {

	/**
	 * If this variable is set to a value, it will be used for setting the {@code consistencyLevel} property on statements
	 * used for query processing.
	 */
	private @Nullable ConsistencyLevel consistencyLevel;

	/**
	 * If this variable is set to a value, it will be used for setting the {@code executionProfile} property on statements
	 * used for query processing.
	 */
	private ExecutionProfileResolver executionProfileResolver = ExecutionProfileResolver.none();

	/**
	 * If this variable is set to a value, it will be used for setting the {@code keyspace} property on statements used
	 * for query processing.
	 */
	private @Nullable CqlIdentifier keyspace;

	/**
	 * If this variable is set to a non-negative value, it will be used for setting the {@code pageSize} property on
	 * statements used for query processing.
	 */
	private int pageSize = -1;

	/**
	 * If this variable is set to a value, it will be used for setting the serial {@code consistencyLevel} property on
	 * statements used for query processing.
	 */
	private @Nullable ConsistencyLevel serialConsistencyLevel;

	/**
	 * Construct a new {@link ReactiveCqlTemplate Note: The {@link ReactiveSessionFactory} has to be set before using the
	 * instance.
	 *
	 * @see #setSessionFactory
	 */
	public ReactiveCqlTemplate() {}

	/**
	 * Construct a new {@link ReactiveCqlTemplate}, given a {@link ReactiveSession}.
	 *
	 * @param reactiveSession the {@link ReactiveSession}, must not be {@literal null}.
	 */
	public ReactiveCqlTemplate(ReactiveSession reactiveSession) {

		Assert.notNull(reactiveSession, "ReactiveSession must not be null");

		setSessionFactory(new DefaultReactiveSessionFactory(reactiveSession));
		afterPropertiesSet();
	}

	/**
	 * Construct a new {@link ReactiveCqlTemplate}, given a {@link ReactiveSessionFactory} to obtain
	 * {@link ReactiveSession}s from.
	 *
	 * @param reactiveSessionFactory the {@link ReactiveSessionFactory} to obtain {@link ReactiveSession}s from, must not
	 *          be {@literal null}.
	 */
	public ReactiveCqlTemplate(ReactiveSessionFactory reactiveSessionFactory) {

		setSessionFactory(reactiveSessionFactory);
		afterPropertiesSet();
	}

	/**
	 * Set the consistency level for this {@link ReactiveCqlTemplate}. Consistency level defines the number of nodes
	 * involved into query processing. Relaxed consistency level settings use fewer nodes but eventual consistency is more
	 * likely to occur while a higher consistency level involves more nodes to obtain results with a higher consistency
	 * guarantee.
	 *
	 * @see Statement#setConsistencyLevel(ConsistencyLevel)
	 * @see RetryPolicy
	 */
	public void setConsistencyLevel(@Nullable ConsistencyLevel consistencyLevel) {
		this.consistencyLevel = consistencyLevel;
	}

	/**
	 * @return the {@link ConsistencyLevel} specified for this {@link ReactiveCqlTemplate}.
	 */
	@Nullable
	public ConsistencyLevel getConsistencyLevel() {
		return consistencyLevel;
	}

	/**
	 * Set the driver execution profile for this template.
	 *
	 * @see Statement#setExecutionProfileName(String)
	 * @see ExecutionProfileResolver
	 * @since 3.0
	 */
	public void setExecutionProfile(String profileName) {
		setExecutionProfileResolver(ExecutionProfileResolver.from(profileName));

	}

	/**
	 * Set the {@link ExecutionProfileResolver} for this template.
	 *
	 * @see com.datastax.oss.driver.api.core.config.DriverExecutionProfile
	 * @see ExecutionProfileResolver
	 * @since 3.0
	 */
	public void setExecutionProfileResolver(ExecutionProfileResolver executionProfileResolver) {

		Assert.notNull(executionProfileResolver, "ExecutionProfileResolver must not be null");

		this.executionProfileResolver = executionProfileResolver;
	}

	/**
	 * @return the {@link ExecutionProfileResolver} specified for this {@link ReactiveCqlTemplate}.
	 * @since 3.0
	 */
	public ExecutionProfileResolver getExecutionProfileResolver() {
		return executionProfileResolver;
	}

	/**
	 * Set the fetch size for this template. This is important for processing large result sets: Setting this higher than
	 * the default value will increase processing speed at the cost of memory consumption; setting this lower can avoid
	 * transferring row data that will never be read by the application. Default is -1, indicating to use the CQL driver's
	 * default configuration (i.e. to not pass a specific fetch size setting on to the driver).
	 *
	 * @see com.datastax.oss.driver.api.core.cql.SimpleStatementBuilder#setPageSize(int)
	 * @deprecated since 3.0, use {@link #setPageSize(int)}
	 */
	@Deprecated
	public void setFetchSize(int fetchSize) {
		setPageSize(fetchSize);
	}

	/**
	 * @return the fetch size specified for this template.
	 * @deprecated since 3.0, use {@link #getPageSize()}.
	 */
	@Deprecated
	public int getFetchSize() {
		return getPageSize();
	}

	/**
	 * Set the {@link CqlIdentifier keyspace} to be applied on statement-level for this template. If not set, the default
	 * {@link CqlSession} keyspace will be used.
	 *
	 * @param keyspace the keyspace to apply, must not be {@literal null}.
	 * @see SimpleStatement#setKeyspace(CqlIdentifier)
	 * @see BatchStatement#setKeyspace(CqlIdentifier)
	 * @since 3.1
	 */
	public void setKeyspace(CqlIdentifier keyspace) {

		Assert.notNull(keyspace, "Keyspace must not be null");

		this.keyspace = keyspace;
	}

	/**
	 * @return the {@link CqlIdentifier keyspace} to be applied on statement-level for this template.
	 * @since 3.1
	 */
	@Nullable
	public CqlIdentifier getKeyspace() {
		return this.keyspace;
	}

	/**
	 * Set the page size for this template. This is important for processing large result sets: Setting this higher than
	 * the default value will increase processing speed at the cost of memory consumption; setting this lower can avoid
	 * transferring row data that will never be read by the application. Default is -1, indicating to use the CQL driver's
	 * default configuration (i.e. to not pass a specific page size setting on to the driver).
	 *
	 * @see com.datastax.oss.driver.api.core.cql.SimpleStatementBuilder#setPageSize(int)
	 */
	public void setPageSize(int fetchSize) {
		this.pageSize = fetchSize;
	}

	/**
	 * @return the page size specified for this template.
	 */
	public int getPageSize() {
		return this.pageSize;
	}

	/**
	 * Set the serial consistency level for this template.
	 *
	 * @since 3.0
	 * @see Statement#setSerialConsistencyLevel(ConsistencyLevel)
	 * @see ConsistencyLevel
	 */
	public void setSerialConsistencyLevel(@Nullable ConsistencyLevel consistencyLevel) {
		this.serialConsistencyLevel = consistencyLevel;
	}

	/**
	 * @return the serial {@link ConsistencyLevel} specified for this template.
	 * @since 3.0
	 */
	@Nullable
	public ConsistencyLevel getSerialConsistencyLevel() {
		return this.serialConsistencyLevel;
	}

	// -------------------------------------------------------------------------
	// Methods dealing with a plain org.springframework.data.cassandra.core.cql.ReactiveSession
	// -------------------------------------------------------------------------

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.ReactiveCqlOperations#execute(org.springframework.data.cassandra.core.cql.ReactiveSessionCallback)
	 */
	@Override
	public <T> Flux<T> execute(ReactiveSessionCallback<T> action) throws DataAccessException {

		Assert.notNull(action, "Callback object must not be null");

		return createFlux(action).onErrorMap(translateException("ReactiveSessionCallback", getCql(action)));
	}

	// -------------------------------------------------------------------------
	// Methods dealing with static CQL
	// -------------------------------------------------------------------------

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.ReactiveCqlOperations#execute(java.lang.String)
	 */
	@Override
	public Mono<Boolean> execute(String cql) throws DataAccessException {

		Assert.hasText(cql, "CQL must not be empty");

		return queryForResultSet(cql).map(ReactiveResultSet::wasApplied);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.ReactiveCqlOperations#query(java.lang.String, org.springframework.data.cassandra.core.cql.ReactiveResultSetExtractor)
	 */
	@Override
	public <T> Flux<T> query(String cql, ReactiveResultSetExtractor<T> resultSetExtractor) throws DataAccessException {

		Assert.hasText(cql, "CQL must not be empty");
		Assert.notNull(resultSetExtractor, "ReactiveResultSetExtractor must not be null");

		return query(SimpleStatement.newInstance(cql), resultSetExtractor);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.ReactiveCqlOperations#query(java.lang.String, org.springframework.data.cassandra.core.cql.RowMapper)
	 */
	@Override
	public <T> Flux<T> query(String cql, RowMapper<T> rowMapper) throws DataAccessException {
		return query(cql, new ReactiveRowMapperResultSetExtractor<>(rowMapper));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.ReactiveCqlOperations#queryForObject(java.lang.String, org.springframework.data.cassandra.core.cql.RowMapper)
	 */
	@Override
	public <T> Mono<T> queryForObject(String cql, RowMapper<T> rowMapper) throws DataAccessException {
		return query(cql, rowMapper).buffer(2).flatMap(list -> Mono.just(DataAccessUtils.requiredSingleResult(list)))
				.next();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.ReactiveCqlOperations#queryForObject(java.lang.String, java.lang.Class)
	 */
	@Override
	public <T> Mono<T> queryForObject(String cql, Class<T> requiredType) throws DataAccessException {
		return queryForObject(cql, getSingleColumnRowMapper(requiredType));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.ReactiveCqlOperations#queryForMap(java.lang.String)
	 */
	@Override
	public Mono<Map<String, Object>> queryForMap(String cql) throws DataAccessException {
		return queryForObject(cql, getColumnMapRowMapper());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.ReactiveCqlOperations#queryForFlux(java.lang.String, java.lang.Class)
	 */
	@Override
	public <T> Flux<T> queryForFlux(String cql, Class<T> elementType) throws DataAccessException {
		return query(cql, getSingleColumnRowMapper(elementType));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.ReactiveCqlOperations#queryForFlux(java.lang.String)
	 */
	@Override
	public Flux<Map<String, Object>> queryForFlux(String cql) throws DataAccessException {
		return query(cql, getColumnMapRowMapper());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.ReactiveCqlOperations#queryForResultSet(java.lang.String)
	 */
	@Override
	public Mono<ReactiveResultSet> queryForResultSet(String cql) throws DataAccessException {

		Assert.hasText(cql, "CQL must not be empty");

		return queryForResultSet(SimpleStatement.newInstance(cql));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.ReactiveCqlOperations#queryForRows(java.lang.String)
	 */
	@Override
	public Flux<Row> queryForRows(String cql) throws DataAccessException {
		return queryForResultSet(cql).flatMapMany(ReactiveResultSet::rows)
				.onErrorMap(translateException("QueryForRows", cql));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.ReactiveCqlOperations#execute(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<Boolean> execute(Publisher<String> statementPublisher) throws DataAccessException {

		Assert.notNull(statementPublisher, "CQL Publisher must not be null");

		return Flux.from(statementPublisher).flatMap(this::execute);
	}

	// -------------------------------------------------------------------------
	// Methods dealing with com.datastax.oss.driver.api.core.cql.Statement
	// -------------------------------------------------------------------------

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.ReactiveCqlOperations#execute(com.datastax.oss.driver.api.core.cql.Statement)
	 */
	@Override
	public Mono<Boolean> execute(Statement<?> statement) throws DataAccessException {

		Assert.notNull(statement, "CQL Statement must not be null");

		return queryForResultSet(statement).map(ReactiveResultSet::wasApplied);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.ReactiveCqlOperations#query(com.datastax.oss.driver.api.core.cql.Statement, org.springframework.data.cassandra.core.cql.ReactiveResultSetExtractor)
	 */
	@Override
	public <T> Flux<T> query(Statement<?> statement, ReactiveResultSetExtractor<T> rse) throws DataAccessException {

		Assert.notNull(statement, "CQL Statement must not be null");
		Assert.notNull(rse, "ReactiveResultSetExtractor must not be null");

		return createFlux(statement, (session, stmt) -> {

			if (logger.isDebugEnabled()) {
				logger.debug("Executing statement [{}]", QueryExtractorDelegate.getCql(statement));
			}

			return session.execute(applyStatementSettings(statement)).flatMapMany(rse::extractData);
		}).onErrorMap(translateException("Query", statement.toString()));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.ReactiveCqlOperations#query(com.datastax.oss.driver.api.core.cql.Statement, org.springframework.data.cassandra.core.cql.RowMapper)
	 */
	@Override
	public <T> Flux<T> query(Statement<?> statement, RowMapper<T> rowMapper) throws DataAccessException {
		return query(statement, new ReactiveRowMapperResultSetExtractor<>(rowMapper));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.ReactiveCqlOperations#queryForObject(com.datastax.oss.driver.api.core.cql.Statement, org.springframework.data.cassandra.core.cql.RowMapper)
	 */
	@Override
	public <T> Mono<T> queryForObject(Statement<?> statement, RowMapper<T> rowMapper) throws DataAccessException {
		return query(statement, rowMapper).buffer(2).flatMap(list -> Mono.just(DataAccessUtils.requiredSingleResult(list)))
				.next();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.ReactiveCqlOperations#queryForObject(com.datastax.oss.driver.api.core.cql.Statement, java.lang.Class)
	 */
	@Override
	public <T> Mono<T> queryForObject(Statement<?> statement, Class<T> requiredType) throws DataAccessException {
		return queryForObject(statement, getSingleColumnRowMapper(requiredType));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.ReactiveCqlOperations#queryForMap(com.datastax.oss.driver.api.core.cql.Statement)
	 */
	@Override
	public Mono<Map<String, Object>> queryForMap(Statement<?> statement) throws DataAccessException {
		return queryForObject(statement, getColumnMapRowMapper());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.ReactiveCqlOperations#queryForFlux(com.datastax.oss.driver.api.core.cql.Statement, java.lang.Class)
	 */
	@Override
	public <T> Flux<T> queryForFlux(Statement<?> statement, Class<T> elementType) throws DataAccessException {
		return query(statement, getSingleColumnRowMapper(elementType));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.ReactiveCqlOperations#queryForFlux(com.datastax.oss.driver.api.core.cql.Statement)
	 */
	@Override
	public Flux<Map<String, Object>> queryForFlux(Statement<?> statement) throws DataAccessException {
		return query(statement, getColumnMapRowMapper());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.ReactiveCqlOperations#queryForResultSet(com.datastax.oss.driver.api.core.cql.Statement)
	 */
	@Override
	public Mono<ReactiveResultSet> queryForResultSet(Statement<?> statement) throws DataAccessException {

		Assert.notNull(statement, "CQL Statement must not be null");

		return createMono(statement, (session, executedStatement) -> {

			if (logger.isDebugEnabled()) {
				logger.debug("Executing statement [{}]", QueryExtractorDelegate.getCql(statement));
			}

			return session.execute(applyStatementSettings(executedStatement));
		}).onErrorMap(translateException("QueryForResultSet", statement.toString()));
	}

	@Override
	public Flux<Row> queryForRows(Statement<?> statement) throws DataAccessException {
		return queryForResultSet(statement).flatMapMany(ReactiveResultSet::rows)
				.onErrorMap(translateException("QueryForRows", statement.toString()));
	}

	// -------------------------------------------------------------------------
	// Methods dealing with com.datastax.oss.driver.api.core.cql.PreparedStatement
	// -------------------------------------------------------------------------

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.ReactiveCqlOperations#execute(org.springframework.data.cassandra.core.cql.ReactivePreparedStatementCreator, org.springframework.data.cassandra.core.cql.ReactivePreparedStatementCallback)
	 */
	@Override
	public <T> Flux<T> execute(ReactivePreparedStatementCreator psc, ReactivePreparedStatementCallback<T> action)
			throws DataAccessException {

		Assert.notNull(psc, "ReactivePreparedStatementCreator must not be null");
		Assert.notNull(action, "ReactivePreparedStatementCallback object must not be null");

		return createFlux(session -> {

			logger.debug("Preparing statement [{}] using {}", getCql(psc), psc);

			return psc.createPreparedStatement(session).flatMapMany(ps -> action.doInPreparedStatement(session, ps));
		}).onErrorMap(translateException("ReactivePreparedStatementCallback", getCql(psc)));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.ReactiveCqlOperations#execute(java.lang.String, org.springframework.data.cassandra.core.cql.ReactivePreparedStatementCallback)
	 */
	@Override
	public <T> Flux<T> execute(String cql, ReactivePreparedStatementCallback<T> action) throws DataAccessException {
		return execute(newReactivePreparedStatementCreator(cql), action);
	}

	/**
	 * Query using a prepared statement, reading the {@link ReactiveResultSet} with a {@link ReactiveResultSetExtractor}.
	 *
	 * @param psc object that can create a {@link PreparedStatement} given a {@link ReactiveSession}
	 * @param preparedStatementBinder object that knows how to set values on the prepared statement. If this is
	 *          {@literal null}, the CQL will be assumed to contain no bind parameters.
	 * @param rse object that will extract results
	 * @return an arbitrary result object, as returned by the {@link ReactiveResultSetExtractor}
	 * @throws DataAccessException if there is any problem
	 */
	public <T> Flux<T> query(ReactivePreparedStatementCreator psc,
			@Nullable PreparedStatementBinder preparedStatementBinder, ReactiveResultSetExtractor<T> rse)
			throws DataAccessException {

		Assert.notNull(psc, "ReactivePreparedStatementCreator must not be null");
		Assert.notNull(rse, "ReactiveResultSetExtractor object must not be null");

		return execute(psc, (session, preparedStatement) -> Mono.just(preparedStatement).flatMapMany(pps -> {

			if (logger.isDebugEnabled()) {
				logger.debug("Executing prepared statement [{}]", QueryExtractorDelegate.getCql(preparedStatement));
			}

			BoundStatement boundStatement = (preparedStatementBinder != null
					? preparedStatementBinder.bindValues(preparedStatement)
					: preparedStatement.bind());

			return session.execute(applyStatementSettings(boundStatement));
		}).flatMap(rse::extractData)).onErrorMap(translateException("Query", getCql(psc)));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.ReactiveCqlOperations#query(org.springframework.data.cassandra.core.cql.ReactivePreparedStatementCreator, org.springframework.data.cassandra.core.cql.ReactiveResultSetExtractor)
	 */
	@Override
	public <T> Flux<T> query(ReactivePreparedStatementCreator psc, ReactiveResultSetExtractor<T> rse)
			throws DataAccessException {
		return query(psc, null, rse);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.ReactiveCqlOperations#query(java.lang.String, org.springframework.data.cassandra.core.cql.PreparedStatementBinder, org.springframework.data.cassandra.core.cql.ReactiveResultSetExtractor)
	 */
	@Override
	public <T> Flux<T> query(String cql, @Nullable PreparedStatementBinder psb, ReactiveResultSetExtractor<T> rse)
			throws DataAccessException {
		return query(newReactivePreparedStatementCreator(cql), psb, rse);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.ReactiveCqlOperations#query(java.lang.String, org.springframework.data.cassandra.core.cql.ReactiveResultSetExtractor, java.lang.Object[])
	 */
	@Override
	public <T> Flux<T> query(String cql, ReactiveResultSetExtractor<T> rse, Object... args) throws DataAccessException {
		return query(newReactivePreparedStatementCreator(cql), newArgPreparedStatementBinder(args), rse);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.ReactiveCqlOperations#query(org.springframework.data.cassandra.core.cql.ReactivePreparedStatementCreator, org.springframework.data.cassandra.core.cql.RowMapper)
	 */
	@Override
	public <T> Flux<T> query(ReactivePreparedStatementCreator psc, RowMapper<T> rowMapper) throws DataAccessException {
		return query(psc, null, new ReactiveRowMapperResultSetExtractor<>(rowMapper));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.ReactiveCqlOperations#query(java.lang.String, org.springframework.data.cassandra.core.cql.PreparedStatementBinder, org.springframework.data.cassandra.core.cql.RowMapper)
	 */
	@Override
	public <T> Flux<T> query(String cql, @Nullable PreparedStatementBinder psb, RowMapper<T> rowMapper)
			throws DataAccessException {
		return query(cql, psb, new ReactiveRowMapperResultSetExtractor<>(rowMapper));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.ReactiveCqlOperations#query(org.springframework.data.cassandra.core.cql.ReactivePreparedStatementCreator, org.springframework.data.cassandra.core.cql.PreparedStatementBinder, org.springframework.data.cassandra.core.cql.RowMapper)
	 */
	@Override
	public <T> Flux<T> query(ReactivePreparedStatementCreator psc, @Nullable PreparedStatementBinder psb,
			RowMapper<T> rowMapper) throws DataAccessException {

		return query(psc, psb, new ReactiveRowMapperResultSetExtractor<>(rowMapper));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.ReactiveCqlOperations#query(java.lang.String, org.springframework.data.cassandra.core.cql.RowMapper, java.lang.Object[])
	 */
	@Override
	public <T> Flux<T> query(String cql, RowMapper<T> rowMapper, Object... args) throws DataAccessException {
		return query(cql, newArgPreparedStatementBinder(args), rowMapper);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.ReactiveCqlOperations#queryForObject(java.lang.String, org.springframework.data.cassandra.core.cql.RowMapper, java.lang.Object[])
	 */
	@Override
	public <T> Mono<T> queryForObject(String cql, RowMapper<T> rowMapper, Object... args) throws DataAccessException {
		return query(cql, rowMapper, args).buffer(2).flatMap(list -> Mono.just(DataAccessUtils.requiredSingleResult(list)))
				.next();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.ReactiveCqlOperations#queryForObject(java.lang.String, java.lang.Class, java.lang.Object[])
	 */
	@Override
	public <T> Mono<T> queryForObject(String cql, Class<T> requiredType, Object... args) throws DataAccessException {
		return queryForObject(cql, getSingleColumnRowMapper(requiredType), args);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.ReactiveCqlOperations#queryForMap(java.lang.String, java.lang.Object[])
	 */
	@Override
	public Mono<Map<String, Object>> queryForMap(String cql, Object... args) throws DataAccessException {
		return queryForObject(cql, getColumnMapRowMapper(), args);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.ReactiveCqlOperations#queryForFlux(java.lang.String, java.lang.Class, java.lang.Object[])
	 */
	@Override
	public <T> Flux<T> queryForFlux(String cql, Class<T> elementType, Object... args) throws DataAccessException {
		return query(cql, getSingleColumnRowMapper(elementType), args);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.ReactiveCqlOperations#queryForFlux(java.lang.String, java.lang.Object[])
	 */
	@Override
	public Flux<Map<String, Object>> queryForFlux(String cql, Object... args) throws DataAccessException {
		return query(cql, getColumnMapRowMapper(), args);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.ReactiveCqlOperations#queryForResultSet(java.lang.String, java.lang.Object[])
	 */
	@Override
	public Mono<ReactiveResultSet> queryForResultSet(String cql, Object... args) throws DataAccessException {

		Assert.hasText(cql, "CQL must not be empty");

		return query(newReactivePreparedStatementCreator(cql), newArgPreparedStatementBinder(args), Mono::just).next();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.ReactiveCqlOperations#queryForRows(java.lang.String, java.lang.Object[])
	 */
	@Override
	public Flux<Row> queryForRows(String cql, Object... args) throws DataAccessException {
		return queryForResultSet(cql, args).flatMapMany(ReactiveResultSet::rows)
				.onErrorMap(translateException("QueryForRows", cql));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.ReactiveCqlOperations#execute(org.springframework.data.cassandra.core.cql.ReactivePreparedStatementCreator)
	 */
	@Override
	public Mono<Boolean> execute(ReactivePreparedStatementCreator psc) throws DataAccessException {
		return query(psc, resultSet -> Mono.just(resultSet.wasApplied())).last();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.ReactiveCqlOperations#execute(java.lang.String, org.springframework.data.cassandra.core.cql.PreparedStatementBinder)
	 */
	@Override
	public Mono<Boolean> execute(String cql, @Nullable PreparedStatementBinder psb) throws DataAccessException {
		return query(newReactivePreparedStatementCreator(cql), psb, resultSet -> Mono.just(resultSet.wasApplied())).next();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.ReactiveCqlOperations#execute(java.lang.String, java.lang.Object[])
	 */
	@Override
	public Mono<Boolean> execute(String cql, Object... args) throws DataAccessException {
		return execute(cql, newArgPreparedStatementBinder(args));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.ReactiveCqlOperations#execute(java.lang.String, org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<Boolean> execute(String cql, Publisher<Object[]> args) throws DataAccessException {

		Assert.notNull(args, "Args Publisher must not be null");

		return execute(newReactivePreparedStatementCreator(cql), (session, ps) -> Flux.from(args).flatMap(objects -> {

			if (logger.isDebugEnabled()) {
				logger.debug("Executing prepared CQL statement [{}]", cql);
			}

			BoundStatement boundStatement = newArgPreparedStatementBinder(objects).bindValues(ps);

			return session.execute(applyStatementSettings(boundStatement));

		}).map(ReactiveResultSet::wasApplied));
	}

	// -------------------------------------------------------------------------
	// Implementation hooks and helper methods
	// -------------------------------------------------------------------------

	/**
	 * Create a new CQL-based {@link ReactivePreparedStatementCreator} using the CQL passed in. By default, we'll create
	 * an {@link SimpleReactivePreparedStatementCreator}. This method allows for the creation to be overridden by
	 * subclasses.
	 *
	 * @param cql static CQL to execute, must not be empty or {@literal null}.
	 * @return the new {@link ReactivePreparedStatementCreator} to use
	 * @since 2.0.8
	 */
	protected ReactivePreparedStatementCreator newReactivePreparedStatementCreator(String cql) {
		return new SimpleReactivePreparedStatementCreator(
				(SimpleStatement) applyStatementSettings(SimpleStatement.newInstance(cql)));
	}

	/**
	 * Create a reusable {@link Flux} given a {@link ReactiveStatementCallback} without exception translation.
	 *
	 * @param callback must not be {@literal null}.
	 * @return a reusable {@link Flux} wrapping the {@link ReactiveStatementCallback}.
	 */
	protected <T> Flux<T> createFlux(Statement<?> statement, ReactiveStatementCallback<T> callback) {

		Assert.notNull(callback, "ReactiveStatementCallback must not be null");

		applyStatementSettings(statement);

		return getSession().flatMapMany(session -> callback.doInStatement(session, statement));
	}

	/**
	 * Create a reusable {@link Mono} given a {@link ReactiveStatementCallback} without exception translation.
	 *
	 * @param callback must not be {@literal null}.
	 * @return a reusable {@link Mono} wrapping the {@link ReactiveStatementCallback}.
	 */
	protected <T> Mono<T> createMono(Statement<?> statement, ReactiveStatementCallback<T> callback) {

		Assert.notNull(callback, "ReactiveStatementCallback must not be null");

		applyStatementSettings(statement);

		return getSession().flatMap(session -> Mono.from(callback.doInStatement(session, statement)));
	}

	/**
	 * Create a reusable {@link Flux} given a {@link ReactiveSessionCallback} without exception translation.
	 *
	 * @param callback must not be {@literal null}.
	 * @return a reusable {@link Flux} wrapping the {@link ReactiveSessionCallback}.
	 */
	protected <T> Flux<T> createFlux(ReactiveSessionCallback<T> callback) {

		Assert.notNull(callback, "ReactiveStatementCallback must not be null");

		return getSession().flatMapMany(callback::doInSession);
	}

	/**
	 * Exception translation {@link Function} intended for {@link Mono#otherwise(Function)} usage.
	 *
	 * @param task readable text describing the task being attempted
	 * @param cql CQL query or update that caused the problem (may be {@literal null})
	 * @return the exception translation {@link Function}
	 * @see CqlProvider
	 */
	protected Function<Throwable, Throwable> translateException(String task, @Nullable String cql) {
		return throwable -> throwable instanceof DriverException ? translate(task, cql, (DriverException) throwable)
				: throwable;
	}

	/**
	 * Create a new RowMapper for reading columns as key-value pairs.
	 *
	 * @return the RowMapper to use
	 * @see ColumnMapRowMapper
	 */
	protected RowMapper<Map<String, Object>> getColumnMapRowMapper() {
		return new ColumnMapRowMapper();
	}

	/**
	 * Create a new RowMapper for reading result objects from a single column.
	 *
	 * @param requiredType the type that each result object is expected to match
	 * @return the RowMapper to use
	 * @see SingleColumnRowMapper
	 */
	protected <T> RowMapper<T> getSingleColumnRowMapper(Class<T> requiredType) {
		return SingleColumnRowMapper.newInstance(requiredType);
	}

	/**
	 * Prepare the given CQL Statement applying statement settings such as page size and consistency level.
	 *
	 * @param stmt the CQL Statement to prepare
	 * @see #setConsistencyLevel(ConsistencyLevel)
	 * @see #setSerialConsistencyLevel(ConsistencyLevel)
	 * @see #setPageSize(int)
	 * @see #setExecutionProfile(String)
	 * @see #setExecutionProfileResolver(ExecutionProfileResolver)
	 */
	protected Statement<?> applyStatementSettings(Statement<?> statement) {

		Statement<?> statementToUse = statement;
		ConsistencyLevel consistencyLevel = getConsistencyLevel();
		ConsistencyLevel serialConsistencyLevel = getSerialConsistencyLevel();
		CqlIdentifier keyspace = getKeyspace();
		int pageSize = getPageSize();

		if (consistencyLevel != null) {
			statementToUse = statementToUse.setConsistencyLevel(consistencyLevel);
		}

		if (serialConsistencyLevel != null) {
			statementToUse = statementToUse.setSerialConsistencyLevel(serialConsistencyLevel);
		}

		if (pageSize > -1) {
			statementToUse = statementToUse.setPageSize(pageSize);
		}

		if (keyspace != null) {
			if (statementToUse instanceof BatchStatement) {
				statementToUse = ((BatchStatement) statementToUse).setKeyspace(keyspace);
			}
			if (statementToUse instanceof SimpleStatement) {
				statementToUse = ((SimpleStatement) statementToUse).setKeyspace(keyspace);
			}
		}

		statementToUse = getExecutionProfileResolver().apply(statementToUse);

		return statementToUse;
	}

	/**
	 * Create a new arg-based PreparedStatementSetter using the args passed in.
	 * <p>
	 * By default, we'll create an {@link ArgumentPreparedStatementBinder}. This method allows for the creation to be
	 * overridden by subclasses.
	 *
	 * @param args object array with arguments
	 * @return the new {@link PreparedStatementBinder} to use
	 */
	protected PreparedStatementBinder newArgPreparedStatementBinder(Object[] args) {
		return new ArgumentPreparedStatementBinder(args);
	}

	private Mono<ReactiveSession> getSession() {

		ReactiveSessionFactory sessionFactory = getSessionFactory();

		Assert.state(sessionFactory != null, "SessionFactory is null");

		return sessionFactory.getSession();
	}

	/**
	 * Determine CQL from potential provider object.
	 *
	 * @param cqlProvider object that's potentially a {@link CqlProvider}
	 * @return the CQL string, or {@literal null}
	 * @see CqlProvider
	 */
	@Nullable
	private static String getCql(@Nullable Object cqlProvider) {
		return QueryExtractorDelegate.getCql(cqlProvider);
	}

	static class SimpleReactivePreparedStatementCreator implements ReactivePreparedStatementCreator, CqlProvider {

		private final SimpleStatement statement;

		SimpleReactivePreparedStatementCreator(SimpleStatement statement) {
			this.statement = statement;
		}

		@Override
		public Mono<PreparedStatement> createPreparedStatement(ReactiveSession session) throws DriverException {
			return session.prepare(this.statement);
		}

		@Override
		public String getCql() {
			return this.statement.getQuery();
		}
	}
}
