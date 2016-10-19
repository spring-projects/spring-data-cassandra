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
package org.springframework.cassandra.core;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.StreamSupport;

import org.springframework.cassandra.support.CassandraAccessor;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.DataAccessUtils;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.util.Assert;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SettableListenableFuture;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;

/**
 * <b>This is the central class in the CQL core package for asynchronous Cassandra data access.</b> It simplifies the
 * use of CQL and helps to avoid common errors. It executes core CQL workflow, leaving application code to provide CQL
 * and extract results. This class executes CQL queries or updates, initiating iteration over {@link ResultSet}s and
 * catching {@link DriverException} exceptions and translating them to the generic, more informative exception hierarchy
 * defined in the {@code org.springframework.dao} package.
 * <p>
 * Code using this class need only implement callback interfaces, giving them a clearly defined contract. The
 * {@link PreparedStatementCreator} callback interface creates a prepared statement given a Connection, providing CQL
 * and any necessary parameters. The {@link ResultSetExtractor} interface extracts values from a {@link ResultSet}. See
 * also {@link PreparedStatementBinder} and {@link RowMapper} for two popular alternative callback interfaces.
 * <p>
 * Can be used within a service implementation via direct instantiation with a {@link Session} reference, or get
 * prepared in an application context and given to services as bean reference. Note: The {@link Session} should always
 * be configured as a bean in the application context, in the first case given to the service directly, in the second
 * case to the prepared template.
 * <p>
 * Because this class is parameterizable by the callback interfaces and the
 * {@link org.springframework.dao.support.PersistenceExceptionTranslator} interface, there should be no need to subclass
 * it.
 * <p>
 * All CQL operations performed by this class are logged at debug level, using
 * "org.springframework.cassandra.core.CqlTemplate" as log category.
 * <p>
 * <b>NOTE: An instance of this class is thread-safe once configured.</b>
 *
 * @author Mark Paluch
 * @see ListenableFuture
 * @see PreparedStatementCreator
 * @see PreparedStatementBinder
 * @see PreparedStatementCallback
 * @see ResultSetExtractor
 * @see RowCallbackHandler
 * @see RowMapper
 * @see org.springframework.dao.support.PersistenceExceptionTranslator
 */
public class AsyncCqlTemplate extends CassandraAccessor implements AsyncCqlOperations {

	/**
	 * Placeholder for default values.
	 */
	private final static Statement DEFAULTS = QueryBuilder.select().from("DEFAULT");

	/**
	 * If this variable is set to a non-negative value, it will be used for setting the {@code fetchSize} property on
	 * statements used for query processing.
	 */
	private int fetchSize = -1;

	/**
	 * If this variable is set to a value, it will be used for setting the {@code retryPolicy} property on statements used
	 * for query processing.
	 */
	private com.datastax.driver.core.policies.RetryPolicy retryPolicy;

	/**
	 * If this variable is set to a value, it will be used for setting the {@code consistencyLevel} property on statements
	 * used for query processing.
	 */
	private com.datastax.driver.core.ConsistencyLevel consistencyLevel;

	/**
	 * Construct a new {@link AsyncCqlTemplate}. Note: The {@link Session} has to be set before using the instance.
	 *
	 * @see #setSession(Session)
	 */
	public AsyncCqlTemplate() {}

	/**
	 * Construct a new {@link AsyncCqlTemplate}, given a {@link Session}.
	 *
	 * @param session the active Cassandra {@link Session}.
	 */
	public AsyncCqlTemplate(Session session) {

		Assert.notNull(session, "Session must not be null");

		setSession(session);
	}

	/**
	 * Set the fetch size for this {@link AsyncCqlTemplate}. This is important for processing large result sets: Setting
	 * this higher than the default value will increase processing speed at the cost of memory consumption; setting this
	 * lower can avoid transferring row data that will never be read by the application. Default is -1, indicating to use
	 * the CQL driver's default configuration (i.e. to not pass a specific fetch size setting on to the driver).
	 *
	 * @see Statement#setFetchSize(int)
	 */
	public void setFetchSize(int fetchSize) {
		this.fetchSize = fetchSize;
	}

	/**
	 * @return the fetch size specified for this {@link AsyncCqlTemplate}.
	 */
	public int getFetchSize() {
		return this.fetchSize;
	}

	/**
	 * Set the retry policy for this {@link AsyncCqlTemplate}. This is important for defining behavior when a request
	 * fails.
	 *
	 * @see Statement#setRetryPolicy(com.datastax.driver.core.policies.RetryPolicy)
	 * @see com.datastax.driver.core.policies.RetryPolicy
	 */
	public void setRetryPolicy(com.datastax.driver.core.policies.RetryPolicy retryPolicy) {
		this.retryPolicy = retryPolicy;
	}

	/**
	 * @return the {@link com.datastax.driver.core.policies.RetryPolicy} specified for this {@link AsyncCqlTemplate}.
	 */
	public com.datastax.driver.core.policies.RetryPolicy getRetryPolicy() {
		return retryPolicy;
	}

	/**
	 * Set the consistency level for this {@link AsyncCqlTemplate}. Consistency level defines the number of nodes involved
	 * into query processing. Relaxed consistency level settings use fewer nodes but eventual consistency is more likely
	 * to occur while a higher consistency level involves more nodes to obtain results with a higher consistency
	 * guarantee.
	 *
	 * @see Statement#setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel)
	 * @see com.datastax.driver.core.policies.RetryPolicy
	 */
	public void setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel consistencyLevel) {
		this.consistencyLevel = consistencyLevel;
	}

	/**
	 * @return the {@link com.datastax.driver.core.ConsistencyLevel} specified for this {@link AsyncCqlTemplate}.
	 */
	public com.datastax.driver.core.ConsistencyLevel getConsistencyLevel() {
		return consistencyLevel;
	}

	// -------------------------------------------------------------------------
	// Methods dealing with a plain com.datastax.driver.core.Session
	// -------------------------------------------------------------------------

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.AsyncCqlOperations#execute(org.springframework.cassandra.core.AsyncSessionCallback)
	 */
	@Override
	public <T> ListenableFuture<T> execute(AsyncSessionCallback<T> action) throws DataAccessException {

		Assert.notNull(action, "Callback object must not be null");

		try {
			return action.doInSession(getSession());
		} catch (DriverException e) {
			throw translateException("SessionCallback", getCql(action), e);
		}
	}

	// -------------------------------------------------------------------------
	// Methods dealing with static CQL
	// -------------------------------------------------------------------------

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.AsyncCqlOperations#execute(java.lang.String)
	 */
	@Override
	public ListenableFuture<Boolean> execute(String cql) throws DataAccessException {

		Assert.hasText(cql, "CQL must not be empty");

		return new MappingListenableFutureAdapter<>(queryForResultSet(cql), ResultSet::wasApplied);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.AsyncCqlOperations#query(java.lang.String, org.springframework.cassandra.core.ResultSetExtractor)
	 */
	@Override
	public <T> ListenableFuture<T> query(String cql, ResultSetExtractor<T> rse) throws DataAccessException {

		Assert.hasText(cql, "CQL must not be empty");
		Assert.notNull(rse, "ResultSetExtractor must not be null");

		try {

			if (logger.isDebugEnabled()) {
				logger.debug("Executing CQL Statement [{}]", cql);
			}

			SimpleStatement simpleStatement = new SimpleStatement(cql);

			applyStatementSettings(simpleStatement);

			return new ExceptionTranslatingListenableFutureAdapter<>(new MappingListenableFutureAdapter<>(
					new GuavaListenableFutureAdapter<>(getSession().executeAsync(simpleStatement),
							ex -> translateExceptionIfPossible("Query", cql, ex)),
					rse::extractData), getExceptionTranslator());
		} catch (DriverException e) {
			throw translateException("Query", cql, e);
		}
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.AsyncCqlOperations#query(java.lang.String, org.springframework.cassandra.core.RowCallbackHandler)
	 */
	@Override
	public ListenableFuture<Void> query(String cql, RowCallbackHandler rch) throws DataAccessException {
		return new ExceptionTranslatingListenableFutureAdapter<>(
				new MappingListenableFutureAdapter<>(query(cql, new RowCallbackHandlerResultSetExtractor(rch)), o -> null),
				getExceptionTranslator());
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.AsyncCqlOperations#query(java.lang.String, org.springframework.cassandra.core.RowMapper)
	 */
	@Override
	public <T> ListenableFuture<List<T>> query(String cql, RowMapper<T> rowMapper) throws DataAccessException {
		return query(cql, new RowMapperResultSetExtractor<>(rowMapper));
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.AsyncCqlOperations#queryForObject(java.lang.String, org.springframework.cassandra.core.RowMapper)
	 */
	@Override
	public <T> ListenableFuture<T> queryForObject(String cql, RowMapper<T> rowMapper) throws DataAccessException {
		return new ExceptionTranslatingListenableFutureAdapter<>(
				new MappingListenableFutureAdapter<>(query(cql, rowMapper), DataAccessUtils::requiredSingleResult),
				getExceptionTranslator());
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.AsyncCqlOperations#queryForObject(java.lang.String, java.lang.Class)
	 */
	@Override
	public <T> ListenableFuture<T> queryForObject(String cql, Class<T> requiredType) throws DataAccessException {
		return queryForObject(cql, getSingleColumnRowMapper(requiredType));
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.AsyncCqlOperations#queryForMap(java.lang.String)
	 */
	@Override
	public ListenableFuture<Map<String, Object>> queryForMap(String cql) throws DataAccessException {
		return queryForObject(cql, getColumnMapRowMapper());
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.AsyncCqlOperations#queryForList(java.lang.String, java.lang.Class)
	 */
	@Override
	public <T> ListenableFuture<List<T>> queryForList(String cql, Class<T> elementType) throws DataAccessException {
		return query(cql, getSingleColumnRowMapper(elementType));
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.AsyncCqlOperations#queryForList(java.lang.String)
	 */
	@Override
	public ListenableFuture<List<Map<String, Object>>> queryForList(String cql) throws DataAccessException {
		return query(cql, getColumnMapRowMapper());
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.AsyncCqlOperations#queryForResultSet(java.lang.String)
	 */
	@Override
	public ListenableFuture<ResultSet> queryForResultSet(String cql) throws DataAccessException {
		return query(cql, rs -> rs);
	}

	// -------------------------------------------------------------------------
	// Methods dealing with com.datastax.driver.core.Statement
	// -------------------------------------------------------------------------

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.AsyncCqlOperations#execute(com.datastax.driver.core.Statement)
	 */
	@Override
	public ListenableFuture<Boolean> execute(Statement statement) throws DataAccessException {

		Assert.notNull(statement, "CQL Statement must not be null");

		return new MappingListenableFutureAdapter<>(queryForResultSet(statement), ResultSet::wasApplied);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.AsyncCqlOperations#query(com.datastax.driver.core.Statement, org.springframework.cassandra.core.ResultSetExtractor)
	 */
	@Override
	public <T> ListenableFuture<T> query(Statement statement, ResultSetExtractor<T> rse) throws DataAccessException {

		Assert.notNull(statement, "CQL Statement must not be null");
		Assert.notNull(rse, "ResultSetExtractor must not be null");

		try {

			if (logger.isDebugEnabled()) {
				logger.debug("Executing CQL Statement [{}]", statement);
			}

			applyStatementSettings(statement);

			return new ExceptionTranslatingListenableFutureAdapter<>(
					new MappingListenableFutureAdapter<>(new GuavaListenableFutureAdapter<>(getSession().executeAsync(statement),
							ex -> translateExceptionIfPossible("Query", statement.toString(), ex)), rse::extractData),
					getExceptionTranslator());
		} catch (DriverException e) {
			throw translateException("Query", statement.toString(), e);
		}
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.AsyncCqlOperations#query(com.datastax.driver.core.Statement, org.springframework.cassandra.core.RowCallbackHandler)
	 */
	@Override
	public ListenableFuture<Void> query(Statement statement, RowCallbackHandler rch) throws DataAccessException {
		return new ExceptionTranslatingListenableFutureAdapter<>(new MappingListenableFutureAdapter<>(
				query(statement, new RowCallbackHandlerResultSetExtractor(rch)), o -> null), getExceptionTranslator());
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.AsyncCqlOperations#query(com.datastax.driver.core.Statement, org.springframework.cassandra.core.RowMapper)
	 */
	@Override
	public <T> ListenableFuture<List<T>> query(Statement statement, RowMapper<T> rowMapper) throws DataAccessException {
		return query(statement, new RowMapperResultSetExtractor<>(rowMapper));
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.AsyncCqlOperations#queryForObject(com.datastax.driver.core.Statement, org.springframework.cassandra.core.RowMapper)
	 */
	@Override
	public <T> ListenableFuture<T> queryForObject(Statement statement, RowMapper<T> rowMapper)
			throws DataAccessException {
		return new ExceptionTranslatingListenableFutureAdapter<>(
				new MappingListenableFutureAdapter<>(query(statement, rowMapper), DataAccessUtils::requiredSingleResult),
				getExceptionTranslator());
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.AsyncCqlOperations#queryForObject(com.datastax.driver.core.Statement, java.lang.Class)
	 */
	@Override
	public <T> ListenableFuture<T> queryForObject(Statement statement, Class<T> requiredType) throws DataAccessException {
		return queryForObject(statement, getSingleColumnRowMapper(requiredType));
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.AsyncCqlOperations#queryForMap(com.datastax.driver.core.Statement)
	 */
	@Override
	public ListenableFuture<Map<String, Object>> queryForMap(Statement statement) throws DataAccessException {
		return queryForObject(statement, getColumnMapRowMapper());
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.AsyncCqlOperations#queryForList(com.datastax.driver.core.Statement, java.lang.Class)
	 */
	@Override
	public <T> ListenableFuture<List<T>> queryForList(Statement statement, Class<T> elementType)
			throws DataAccessException {
		return query(statement, getSingleColumnRowMapper(elementType));
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.AsyncCqlOperations#queryForList(com.datastax.driver.core.Statement)
	 */
	@Override
	public ListenableFuture<List<Map<String, Object>>> queryForList(Statement statement) throws DataAccessException {
		return query(statement, getColumnMapRowMapper());
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.AsyncCqlOperations#queryForResultSet(com.datastax.driver.core.Statement)
	 */
	@Override
	public ListenableFuture<ResultSet> queryForResultSet(Statement statement) throws DataAccessException {
		return query(statement, rs -> rs);
	}

	// -------------------------------------------------------------------------
	// Methods dealing with prepared statements
	// -------------------------------------------------------------------------

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.AsyncCqlOperations#execute(org.springframework.cassandra.core.AsyncPreparedStatementCreator, org.springframework.cassandra.core.PreparedStatementCallback)
	 */
	@Override
	public <T> ListenableFuture<T> execute(AsyncPreparedStatementCreator psc, PreparedStatementCallback<T> action)
			throws DataAccessException {

		Assert.notNull(psc, "PreparedStatementCreator must not be null");
		Assert.notNull(action, "PreparedStatementCallback object must not be null");

		try {

			if (logger.isDebugEnabled()) {
				logger.debug("Preparing statement [{}] using {}", getCql(psc), psc);
			}

			return new ExceptionTranslatingListenableFutureAdapter<>(
					new MappingListenableFutureAdapter<>(psc.createPreparedStatement(getSession()), preparedStatement -> {

						try {
							applyStatementSettings(preparedStatement);
							return action.doInPreparedStatement(preparedStatement);
						} catch (DriverException e) {
							throw translateException("PreparedStatementCallback", preparedStatement.toString(), e);
						}
					}), getExceptionTranslator());

		} catch (DriverException e) {
			throw translateException("PreparedStatementCallback", getCql(psc), e);
		}
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.AsyncCqlOperations#query(org.springframework.cassandra.core.AsyncPreparedStatementCreator, org.springframework.cassandra.core.PreparedStatementBinder, org.springframework.cassandra.core.ResultSetExtractor)
	 */
	@Override
	public <T> ListenableFuture<T> query(AsyncPreparedStatementCreator psc, PreparedStatementBinder psb,
			ResultSetExtractor<T> rse) throws DataAccessException {

		Assert.notNull(psc, "AsyncPreparedStatementCreator must not be null");
		Assert.notNull(rse, "ResultSetExtractor object must not be null");

		try {

			if (logger.isDebugEnabled()) {
				logger.debug("Preparing statement [{}] using {}", getCql(psc), psc);
			}

			Session session = getSession();

			PersistenceExceptionTranslator exceptionTranslator = ex -> translateExceptionIfPossible("Query", getCql(psc), ex);

			ListenableFuture<BoundStatement> psFuture = new MappingListenableFutureAdapter<>(
					psc.createPreparedStatement(session), ps -> {

						if (logger.isDebugEnabled()) {
							logger.debug("Executing prepared statement [{}]", ps);
						}

						BoundStatement boundStatement = psb != null ? psb.bindValues(ps) : ps.bind();

						applyStatementSettings(boundStatement);

						return boundStatement;
					});

			SettableListenableFuture<T> settableListenableFuture = new SettableListenableFuture<T>();
			psFuture.addCallback(boundStatement -> {

				Futures.addCallback(session.executeAsync(boundStatement), new FutureCallback<ResultSet>() {
					@Override
					public void onSuccess(ResultSet result) {
						try {
							settableListenableFuture.set(rse.extractData(result));
						} catch (DriverException e) {
							settableListenableFuture.setException(exceptionTranslator.translateExceptionIfPossible(e));
						}
					}

					@Override
					public void onFailure(Throwable ex) {

						if (ex instanceof DriverException) {
							settableListenableFuture
									.setException(exceptionTranslator.translateExceptionIfPossible((DriverException) ex));
						} else {
							settableListenableFuture.setException(ex);
						}
					}
				});

			}, ex -> {
				if (ex instanceof DriverException) {
					settableListenableFuture.setException(exceptionTranslator.translateExceptionIfPossible((DriverException) ex));
				} else {
					settableListenableFuture.setException(ex);
				}
			});

			return settableListenableFuture;

		} catch (DriverException e) {
			throw translateException("Query", getCql(psc), e);
		}
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.AsyncCqlOperations#execute(java.lang.String, org.springframework.cassandra.core.PreparedStatementCallback)
	 */
	@Override
	public <T> ListenableFuture<T> execute(String cql, PreparedStatementCallback<T> action) throws DataAccessException {
		return execute(newAsyncPreparedStatementCreator(cql), action);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.AsyncCqlOperations#query(org.springframework.cassandra.core.AsyncPreparedStatementCreator, org.springframework.cassandra.core.ResultSetExtractor)
	 */
	@Override
	public <T> ListenableFuture<T> query(AsyncPreparedStatementCreator psc, ResultSetExtractor<T> rse)
			throws DataAccessException {
		return query(psc, null, rse);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.AsyncCqlOperations#query(java.lang.String, org.springframework.cassandra.core.PreparedStatementBinder, org.springframework.cassandra.core.ResultSetExtractor)
	 */
	@Override
	public <T> ListenableFuture<T> query(String cql, PreparedStatementBinder psb, ResultSetExtractor<T> rse)
			throws DataAccessException {
		return query(newAsyncPreparedStatementCreator(cql), psb, rse);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.AsyncCqlOperations#query(java.lang.String, org.springframework.cassandra.core.ResultSetExtractor, java.lang.Object[])
	 */
	@Override
	public <T> ListenableFuture<T> query(String cql, ResultSetExtractor<T> rse, Object... args)
			throws DataAccessException {
		return query(cql, newArgPreparedStatementBinder(args), rse);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.AsyncCqlOperations#query(org.springframework.cassandra.core.AsyncPreparedStatementCreator, org.springframework.cassandra.core.RowCallbackHandler)
	 */
	@Override
	public ListenableFuture<Void> query(AsyncPreparedStatementCreator psc, RowCallbackHandler rch)
			throws DataAccessException {
		return new ExceptionTranslatingListenableFutureAdapter<>(
				new MappingListenableFutureAdapter<>(query(psc, new RowCallbackHandlerResultSetExtractor(rch)), o -> null),
				getExceptionTranslator());
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.AsyncCqlOperations#query(java.lang.String, org.springframework.cassandra.core.PreparedStatementBinder, org.springframework.cassandra.core.RowCallbackHandler)
	 */
	@Override
	public ListenableFuture<Void> query(String cql, PreparedStatementBinder psb, RowCallbackHandler rch)
			throws DataAccessException {
		return new ExceptionTranslatingListenableFutureAdapter<>(
				new MappingListenableFutureAdapter<>(query(cql, psb, new RowCallbackHandlerResultSetExtractor(rch)), o -> null),
				getExceptionTranslator());
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.AsyncCqlOperations#query(org.springframework.cassandra.core.AsyncPreparedStatementCreator, org.springframework.cassandra.core.PreparedStatementBinder, org.springframework.cassandra.core.RowCallbackHandler)
	 */
	@Override
	public ListenableFuture<Void> query(AsyncPreparedStatementCreator psc, PreparedStatementBinder psb,
			RowCallbackHandler rch) throws DataAccessException {
		return new ExceptionTranslatingListenableFutureAdapter<>(
				new MappingListenableFutureAdapter<>(query(psc, psb, new RowCallbackHandlerResultSetExtractor(rch)), o -> null),
				getExceptionTranslator());
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.AsyncCqlOperations#query(java.lang.String, org.springframework.cassandra.core.RowCallbackHandler, java.lang.Object[])
	 */
	@Override
	public ListenableFuture<Void> query(String cql, RowCallbackHandler rch, Object... args) throws DataAccessException {
		return new ExceptionTranslatingListenableFutureAdapter<>(
				new MappingListenableFutureAdapter<>(query(newAsyncPreparedStatementCreator(cql),
						newArgPreparedStatementBinder(args), new RowCallbackHandlerResultSetExtractor(rch)), o -> null),
				getExceptionTranslator());
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.AsyncCqlOperations#query(org.springframework.cassandra.core.AsyncPreparedStatementCreator, org.springframework.cassandra.core.RowMapper)
	 */
	@Override
	public <T> ListenableFuture<List<T>> query(AsyncPreparedStatementCreator psc, RowMapper<T> rowMapper)
			throws DataAccessException {
		return query(psc, new RowMapperResultSetExtractor<>(rowMapper));
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.AsyncCqlOperations#query(java.lang.String, org.springframework.cassandra.core.PreparedStatementBinder, org.springframework.cassandra.core.RowMapper)
	 */
	@Override
	public <T> ListenableFuture<List<T>> query(String cql, PreparedStatementBinder psb, RowMapper<T> rowMapper)
			throws DataAccessException {
		return query(cql, psb, new RowMapperResultSetExtractor<>(rowMapper));
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.AsyncCqlOperations#query(org.springframework.cassandra.core.AsyncPreparedStatementCreator, org.springframework.cassandra.core.PreparedStatementBinder, org.springframework.cassandra.core.RowMapper)
	 */
	@Override
	public <T> ListenableFuture<List<T>> query(AsyncPreparedStatementCreator psc, PreparedStatementBinder psb,
			RowMapper<T> rowMapper) throws DataAccessException {
		return query(psc, psb, new RowMapperResultSetExtractor<>(rowMapper));
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.AsyncCqlOperations#query(java.lang.String, org.springframework.cassandra.core.RowMapper, java.lang.Object[])
	 */
	@Override
	public <T> ListenableFuture<List<T>> query(String cql, RowMapper<T> rowMapper, Object... args)
			throws DataAccessException {
		return query(cql, newArgPreparedStatementBinder(args), new RowMapperResultSetExtractor<>(rowMapper));
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.AsyncCqlOperations#queryForObject(java.lang.String, org.springframework.cassandra.core.RowMapper, java.lang.Object[])
	 */
	@Override
	public <T> ListenableFuture<T> queryForObject(String cql, RowMapper<T> rowMapper, Object... args)
			throws DataAccessException {
		return new ExceptionTranslatingListenableFutureAdapter<>(new MappingListenableFutureAdapter<>(
				query(cql, newArgPreparedStatementBinder(args), new RowMapperResultSetExtractor<>(rowMapper, 1)),
				DataAccessUtils::requiredSingleResult), getExceptionTranslator());
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.AsyncCqlOperations#queryForObject(java.lang.String, java.lang.Class, java.lang.Object[])
	 */
	@Override
	public <T> ListenableFuture<T> queryForObject(String cql, Class<T> requiredType, Object... args)
			throws DataAccessException {
		return queryForObject(cql, getSingleColumnRowMapper(requiredType), args);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.AsyncCqlOperations#queryForMap(java.lang.String, java.lang.Object[])
	 */
	@Override
	public ListenableFuture<Map<String, Object>> queryForMap(String cql, Object... args) throws DataAccessException {
		return queryForObject(cql, getColumnMapRowMapper(), args);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.AsyncCqlOperations#queryForList(java.lang.String, java.lang.Class, java.lang.Object[])
	 */
	@Override
	public <T> ListenableFuture<List<T>> queryForList(String cql, Class<T> elementType, Object... args)
			throws DataAccessException {
		return query(cql, getSingleColumnRowMapper(elementType), args);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.AsyncCqlOperations#queryForList(java.lang.String, java.lang.Object[])
	 */
	@Override
	public ListenableFuture<List<Map<String, Object>>> queryForList(String cql, Object... args)
			throws DataAccessException {
		return query(cql, getColumnMapRowMapper(), args);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.AsyncCqlOperations#queryForResultSet(java.lang.String, java.lang.Object[])
	 */
	@Override
	public ListenableFuture<ResultSet> queryForResultSet(String cql, Object... args) throws DataAccessException {
		return query(cql, rs -> rs, args);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.AsyncCqlOperations#execute(org.springframework.cassandra.core.AsyncPreparedStatementCreator)
	 */
	@Override
	public ListenableFuture<Boolean> execute(AsyncPreparedStatementCreator psc) throws DataAccessException {
		return query(psc, ResultSet::wasApplied);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.AsyncCqlOperations#execute(java.lang.String, org.springframework.cassandra.core.PreparedStatementBinder)
	 */
	@Override
	public ListenableFuture<Boolean> execute(String cql, PreparedStatementBinder psb) throws DataAccessException {
		return query(newAsyncPreparedStatementCreator(cql), psb, ResultSet::wasApplied);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.AsyncCqlOperations#execute(java.lang.String, java.lang.Object[])
	 */
	@Override
	public ListenableFuture<Boolean> execute(String cql, Object... args) throws DataAccessException {
		return execute(cql, newArgPreparedStatementBinder(args));
	}

	// -------------------------------------------------------------------------
	// Implementation hooks and helper methods
	// -------------------------------------------------------------------------

	/**
	 * Translate the given {@link DriverException} into a generic {@link DataAccessException}.
	 *
	 * @param task readable text describing the task being attempted
	 * @param cql CQL query or update that caused the problem (may be {@code null})
	 * @param ex the offending {@code RuntimeException}.
	 * @return the translated {@link DataAccessException} or {@literal null} if translation not possible.
	 * @see CqlProvider
	 */
	@SuppressWarnings("ThrowableResultOfMethodCallIgnored")
	protected DataAccessException translateExceptionIfPossible(String task, String cql, RuntimeException ex) {

		if (ex instanceof DriverException) {
			return translate(task, cql, (DriverException) ex);
		}

		return null;
	}

	/**
	 * Translate the given {@link DriverException} into a generic {@link DataAccessException}.
	 *
	 * @param task readable text describing the task being attempted
	 * @param cql CQL query or update that caused the problem (may be {@code null})
	 * @param ex the offending {@code RuntimeException}.
	 * @return the exception translation {@link Function}
	 * @see CqlProvider
	 */
	@SuppressWarnings("ThrowableResultOfMethodCallIgnored")
	protected DataAccessException translateException(String task, String cql, DriverException ex) {
		return translate(task, cql, ex);
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
	 * Prepare the given CQL Statement (or {@link com.datastax.driver.core.PreparedStatement}), applying statement
	 * settings such as fetch size, retry policy, and consistency level.
	 *
	 * @param stmt the CQL Statement to prepare
	 * @see #setFetchSize(int)
	 * @see #setRetryPolicy(RetryPolicy)
	 * @see #setConsistencyLevel(ConsistencyLevel)
	 */
	protected void applyStatementSettings(Statement stmt) {

		int fetchSize = getFetchSize();
		if (fetchSize != -1 && stmt.getFetchSize() == DEFAULTS.getFetchSize()) {
			stmt.setFetchSize(fetchSize);
		}

		RetryPolicy retryPolicy = getRetryPolicy();
		if (retryPolicy != null && stmt.getRetryPolicy() == DEFAULTS.getRetryPolicy()) {
			stmt.setRetryPolicy(retryPolicy);
		}

		ConsistencyLevel consistencyLevel = getConsistencyLevel();
		if (consistencyLevel != null && stmt.getConsistencyLevel() == DEFAULTS.getConsistencyLevel()) {
			stmt.setConsistencyLevel(consistencyLevel);
		}
	}

	/**
	 * Prepare the given CQL Statement (or {@link com.datastax.driver.core.PreparedStatement}), applying statement
	 * settings such as retry policy and consistency level.
	 *
	 * @param stmt the CQL Statement to prepare
	 * @see #setRetryPolicy(RetryPolicy)
	 * @see #setConsistencyLevel(ConsistencyLevel)
	 */
	protected void applyStatementSettings(PreparedStatement stmt) {

		RetryPolicy retryPolicy = getRetryPolicy();
		if (retryPolicy != null) {
			stmt.setRetryPolicy(retryPolicy);
		}

		ConsistencyLevel consistencyLevel = getConsistencyLevel();
		if (consistencyLevel != null) {
			stmt.setConsistencyLevel(consistencyLevel);
		}
	}

	/**
	 * Create a new arg-based PreparedStatementSetter using the args passed in. By default, we'll create an
	 * {@link ArgumentPreparedStatementBinder}. This method allows for the creation to be overridden by subclasses.
	 *
	 * @param args object array with arguments
	 * @return the new {@link PreparedStatementBinder} to use
	 */
	protected PreparedStatementBinder newArgPreparedStatementBinder(Object[] args) {
		return new ArgumentPreparedStatementBinder(args);
	}

	/**
	 * Create a new CQL-based AsyncPreparedStatementCreator using the CQL passed in. By default, we'll create an
	 * {@link SimpleAsyncPreparedStatementCreator}. This method allows for the creation to be overridden by subclasses.
	 *
	 * @param cql static CQL to execute, must not be empty or {@literal null}.
	 * @return the new {@link AsyncPreparedStatementCreator} to use
	 */
	protected AsyncPreparedStatementCreator newAsyncPreparedStatementCreator(String cql) {
		return new SimpleAsyncPreparedStatementCreator(cql,
				ex -> translateExceptionIfPossible("PrepareStatement", cql, ex));
	}

	/**
	 * Determine CQL from potential provider object.
	 *
	 * @param cqlProvider object that's potentially a {@link CqlProvider}
	 * @return the CQL string, or {@code null}
	 * @see CqlProvider
	 */
	private static String getCql(Object cqlProvider) {

		if (cqlProvider instanceof CqlProvider) {
			return ((CqlProvider) cqlProvider).getCql();
		} else {
			return null;
		}
	}

	private static class SimpleAsyncPreparedStatementCreator implements AsyncPreparedStatementCreator, CqlProvider {

		private final String cql;
		private final PersistenceExceptionTranslator persistenceExceptionTranslator;

		SimpleAsyncPreparedStatementCreator(String cql, PersistenceExceptionTranslator persistenceExceptionTranslator) {

			Assert.notNull(cql, "CQL must not be null");

			this.cql = cql;
			this.persistenceExceptionTranslator = persistenceExceptionTranslator;
		}

		@Override
		public ListenableFuture<PreparedStatement> createPreparedStatement(Session session) throws DriverException {

			return new GuavaListenableFutureAdapter<>(session.prepareAsync(cql), persistenceExceptionTranslator);
		}

		@Override
		public String getCql() {
			return cql;
		}
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

	/**
	 * Adapter to enable use of a {@link RowCallbackHandler} inside a {@link ResultSetExtractor}.
	 */
	private static class RowCallbackHandlerResultSetExtractor implements ResultSetExtractor<Object> {

		private final RowCallbackHandler rch;

		public RowCallbackHandlerResultSetExtractor(RowCallbackHandler rch) {
			this.rch = rch;
		}

		@Override
		public Object extractData(ResultSet rs) {

			StreamSupport.stream(rs.spliterator(), false).forEach(rch::processRow);
			return null;
		}
	}
}
