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
package org.springframework.data.cassandra.core.cql;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.DataAccessUtils;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.cassandra.SessionFactory;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SettableListenableFuture;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverException;
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
 * "org.springframework.data.cassandra.core.cqlTemplate" as log category.
 * <p>
 * <b>NOTE: An instance of this class is thread-safe once configured.</b>
 *
 * @author Mark Paluch
 * @author John Blum
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
	 * Create a new, uninitialized {@link AsyncCqlTemplate}. Note: The {@link SessionFactory} has to be set before using
	 * the instance.
	 *
	 * @see #setSessionFactory(SessionFactory)
	 */
	public AsyncCqlTemplate() {}

	/**
	 * Create a new {@link AsyncCqlTemplate} with the given {@link Session}.
	 *
	 * @param session the active Cassandra {@link Session}, must not be {@literal null}.
	 * @throws IllegalStateException if {@link Session} is {@literal null}.
	 * @see com.datastax.driver.core.Session
	 */
	public AsyncCqlTemplate(Session session) {

		Assert.notNull(session, "Session must not be null");

		setSession(session);
	}

	/**
	 * Constructs a new {@link AsyncCqlTemplate} with the given {@link SessionFactory}.
	 *
	 * @param sessionFactory the active Cassandra {@link SessionFactory}.
	 * @since 2.0
	 * @see SessionFactory
	 */
	public AsyncCqlTemplate(SessionFactory sessionFactory) {

		Assert.notNull(sessionFactory, "SessionFactory must not be null");

		setSessionFactory(sessionFactory);
	}

	// -------------------------------------------------------------------------
	// Methods dealing with a plain com.datastax.driver.core.Session
	// -------------------------------------------------------------------------

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.AsyncCqlOperations#execute(org.springframework.data.cassandra.core.cql.AsyncSessionCallback)
	 */
	@Override
	public <T> ListenableFuture<T> execute(AsyncSessionCallback<T> action) throws DataAccessException {

		Assert.notNull(action, "Callback object must not be null");

		try {
			return action.doInSession(getCurrentSession());
		} catch (DriverException e) {
			throw translateException("SessionCallback", toCql(action), e);
		}
	}

	// -------------------------------------------------------------------------
	// Methods dealing with static CQL
	// -------------------------------------------------------------------------

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.AsyncCqlOperations#execute(java.lang.String)
	 */
	@Override
	public ListenableFuture<Boolean> execute(String cql) throws DataAccessException {

		Assert.hasText(cql, "CQL must not be empty");

		return new MappingListenableFutureAdapter<>(queryForResultSet(cql), ResultSet::wasApplied);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.AsyncCqlOperations#query(java.lang.String, org.springframework.data.cassandra.core.cql.ResultSetExtractor)
	 */
	@Override
	public <T> ListenableFuture<T> query(String cql, ResultSetExtractor<T> resultSetExtractor)
			throws DataAccessException {

		Assert.hasText(cql, "CQL must not be empty");
		Assert.notNull(resultSetExtractor, "ResultSetExtractor must not be null");

		try {
			if (logger.isDebugEnabled()) {
				logger.debug("Executing CQL Statement [{}]", cql);
			}

			SimpleStatement simpleStatement = applyStatementSettings(new SimpleStatement(cql));

			ResultSetFuture results = getCurrentSession().executeAsync(simpleStatement);

			return new ExceptionTranslatingListenableFutureAdapter<>(new MappingListenableFutureAdapter<>(
					new GuavaListenableFutureAdapter<>(results, ex -> translateExceptionIfPossible("Query", cql, ex)),
					resultSetExtractor::extractData), getExceptionTranslator());
		} catch (DriverException e) {
			throw translateException("Query", cql, e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.AsyncCqlOperations#query(java.lang.String, org.springframework.data.cassandra.core.cql.RowCallbackHandler)
	 */
	@Override
	public ListenableFuture<Void> query(String cql, RowCallbackHandler rowCallbackHandler) throws DataAccessException {

		ListenableFuture<?> results = query(cql, newResultSetExtractor(rowCallbackHandler));

		return new ExceptionTranslatingListenableFutureAdapter<>(new MappingListenableFutureAdapter<>(results, o -> null),
				getExceptionTranslator());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.AsyncCqlOperations#query(java.lang.String, org.springframework.data.cassandra.core.cql.RowMapper)
	 */
	@Override
	public <T> ListenableFuture<List<T>> query(String cql, RowMapper<T> rowMapper) throws DataAccessException {
		return query(cql, newResultSetExtractor(rowMapper));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.AsyncCqlOperations#queryForList(java.lang.String)
	 */
	@Override
	public ListenableFuture<List<Map<String, Object>>> queryForList(String cql) throws DataAccessException {
		return query(cql, newResultSetExtractor(newColumnMapRowMapper()));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.AsyncCqlOperations#queryForList(java.lang.String, java.lang.Class)
	 */
	@Override
	public <T> ListenableFuture<List<T>> queryForList(String cql, Class<T> elementType) throws DataAccessException {
		return query(cql, newResultSetExtractor(newSingleColumnRowMapper(elementType)));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.AsyncCqlOperations#queryForMap(java.lang.String)
	 */
	@Override
	public ListenableFuture<Map<String, Object>> queryForMap(String cql) throws DataAccessException {
		return queryForObject(cql, newColumnMapRowMapper());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.AsyncCqlOperations#queryForObject(java.lang.String, java.lang.Class)
	 */
	@Override
	public <T> ListenableFuture<T> queryForObject(String cql, Class<T> requiredType) throws DataAccessException {
		return queryForObject(cql, newSingleColumnRowMapper(requiredType));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.AsyncCqlOperations#queryForObject(java.lang.String, org.springframework.data.cassandra.core.cql.RowMapper)
	 */
	@Override
	public <T> ListenableFuture<T> queryForObject(String cql, RowMapper<T> rowMapper) throws DataAccessException {

		ListenableFuture<List<T>> results = query(cql, newResultSetExtractor(rowMapper));

		return new ExceptionTranslatingListenableFutureAdapter<>(
				new MappingListenableFutureAdapter<>(results, DataAccessUtils::requiredSingleResult), getExceptionTranslator());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.AsyncCqlOperations#queryForResultSet(java.lang.String)
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
	 * @see org.springframework.data.cassandra.core.cql.AsyncCqlOperations#execute(com.datastax.driver.core.Statement)
	 */
	@Override
	public ListenableFuture<Boolean> execute(Statement statement) throws DataAccessException {

		Assert.notNull(statement, "CQL Statement must not be null");

		return new MappingListenableFutureAdapter<>(queryForResultSet(statement), ResultSet::wasApplied);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.AsyncCqlOperations#query(com.datastax.driver.core.Statement, org.springframework.data.cassandra.core.cql.ResultSetExtractor)
	 */
	@Override
	public <T> ListenableFuture<T> query(Statement statement, ResultSetExtractor<T> resultSetExtractor)
			throws DataAccessException {

		Assert.notNull(statement, "CQL Statement must not be null");
		Assert.notNull(resultSetExtractor, "ResultSetExtractor must not be null");

		try {
			if (logger.isDebugEnabled()) {
				logger.debug("Executing CQL Statement [{}]", statement);
			}

			ResultSetFuture results = getCurrentSession().executeAsync(applyStatementSettings(statement));

			return new ExceptionTranslatingListenableFutureAdapter<>(
					new MappingListenableFutureAdapter<>(
							new GuavaListenableFutureAdapter<>(results,
									ex -> translateExceptionIfPossible("Query", statement.toString(), ex)),
							resultSetExtractor::extractData),
					getExceptionTranslator());
		} catch (DriverException e) {
			throw translateException("Query", statement.toString(), e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.AsyncCqlOperations#query(com.datastax.driver.core.Statement, org.springframework.data.cassandra.core.cql.RowCallbackHandler)
	 */
	@Override
	public ListenableFuture<Void> query(Statement statement, RowCallbackHandler rowCallbackHandler)
			throws DataAccessException {

		ListenableFuture<?> result = query(statement, newResultSetExtractor(rowCallbackHandler));

		return new ExceptionTranslatingListenableFutureAdapter<>(new MappingListenableFutureAdapter<>(result, o -> null),
				getExceptionTranslator());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.AsyncCqlOperations#query(com.datastax.driver.core.Statement, org.springframework.data.cassandra.core.cql.RowMapper)
	 */
	@Override
	public <T> ListenableFuture<List<T>> query(Statement statement, RowMapper<T> rowMapper) throws DataAccessException {
		return query(statement, newResultSetExtractor(rowMapper));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.AsyncCqlOperations#queryForList(com.datastax.driver.core.Statement)
	 */
	@Override
	public ListenableFuture<List<Map<String, Object>>> queryForList(Statement statement) throws DataAccessException {
		return query(statement, newResultSetExtractor(newColumnMapRowMapper()));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.AsyncCqlOperations#queryForList(com.datastax.driver.core.Statement, java.lang.Class)
	 */
	@Override
	public <T> ListenableFuture<List<T>> queryForList(Statement statement, Class<T> elementType)
			throws DataAccessException {
		return query(statement, newResultSetExtractor(newSingleColumnRowMapper(elementType)));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.AsyncCqlOperations#queryForMap(com.datastax.driver.core.Statement)
	 */
	@Override
	public ListenableFuture<Map<String, Object>> queryForMap(Statement statement) throws DataAccessException {
		return queryForObject(statement, newColumnMapRowMapper());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.AsyncCqlOperations#queryForObject(com.datastax.driver.core.Statement, java.lang.Class)
	 */
	@Override
	public <T> ListenableFuture<T> queryForObject(Statement statement, Class<T> requiredType) throws DataAccessException {
		return queryForObject(statement, newSingleColumnRowMapper(requiredType));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.AsyncCqlOperations#queryForObject(com.datastax.driver.core.Statement, org.springframework.data.cassandra.core.cql.RowMapper)
	 */
	@Override
	public <T> ListenableFuture<T> queryForObject(Statement statement, RowMapper<T> rowMapper)
			throws DataAccessException {

		ListenableFuture<List<T>> results = query(statement, newResultSetExtractor(rowMapper));

		return new ExceptionTranslatingListenableFutureAdapter<>(
				new MappingListenableFutureAdapter<>(results, DataAccessUtils::requiredSingleResult), getExceptionTranslator());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.AsyncCqlOperations#queryForResultSet(com.datastax.driver.core.Statement)
	 */
	@Override
	public ListenableFuture<ResultSet> queryForResultSet(Statement statement) throws DataAccessException {
		return query(statement, rs -> rs);
	}

	// -------------------------------------------------------------------------
	// Methods dealing with com.datastax.driver.core.PreparedStatement
	// -------------------------------------------------------------------------

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.AsyncCqlOperations#execute(org.springframework.data.cassandra.core.cql.AsyncPreparedStatementCreator)
	 */
	@Override
	public ListenableFuture<Boolean> execute(AsyncPreparedStatementCreator preparedStatementCreator)
			throws DataAccessException {

		return query(preparedStatementCreator, ResultSet::wasApplied);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.AsyncCqlOperations#execute(java.lang.String, java.lang.Object[])
	 */
	@Override
	public ListenableFuture<Boolean> execute(String cql, Object... args) throws DataAccessException {
		return execute(cql, newPreparedStatementBinder(args));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.AsyncCqlOperations#execute(java.lang.String, org.springframework.data.cassandra.core.cql.@Nullablender)
	 */
	@Override
	public ListenableFuture<Boolean> execute(String cql, @Nullable PreparedStatementBinder psb)
			throws DataAccessException {

		return query(newAsyncPreparedStatementCreator(cql), psb, ResultSet::wasApplied);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.AsyncCqlOperations#execute(java.lang.String, org.springframework.data.cassandra.core.cql.PreparedStatementCallback)
	 */
	@Override
	public <T> ListenableFuture<T> execute(String cql, PreparedStatementCallback<T> action) throws DataAccessException {
		return execute(newAsyncPreparedStatementCreator(cql), action);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.AsyncCqlOperations#execute(org.springframework.data.cassandra.core.cql.AsyncPreparedStatementCreator, org.springframework.data.cassandra.core.cql.PreparedStatementCallback)
	 */
	@Override
	public <T> ListenableFuture<T> execute(AsyncPreparedStatementCreator preparedStatementCreator,
			PreparedStatementCallback<T> action) throws DataAccessException {

		Assert.notNull(preparedStatementCreator, "PreparedStatementCreator must not be null");
		Assert.notNull(action, "PreparedStatementCallback object must not be null");

		PersistenceExceptionTranslator exceptionTranslator = ex -> translateExceptionIfPossible("PreparedStatementCallback",
				toCql(preparedStatementCreator), ex);
		try {
			if (logger.isDebugEnabled()) {
				logger.debug("Preparing statement [{}] using {}", toCql(preparedStatementCreator), preparedStatementCreator);
			}

			Session currentSession = getCurrentSession();
			return new ExceptionTranslatingListenableFutureAdapter<>(new MappingListenableFutureAdapter<>(
					preparedStatementCreator.createPreparedStatement(currentSession), preparedStatement -> {
						try {
							return action.doInPreparedStatement(currentSession, applyStatementSettings(preparedStatement));
						} catch (DriverException e) {
							throw translateException(exceptionTranslator, e);
						}
					}), getExceptionTranslator());

		} catch (DriverException e) {
			throw translateException(exceptionTranslator, e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.AsyncCqlOperations#query(org.springframework.data.cassandra.core.cql.AsyncPreparedStatementCreator, org.springframework.data.cassandra.core.cql.ResultSetExtractor)
	 */
	@Override
	public <T> ListenableFuture<T> query(AsyncPreparedStatementCreator preparedStatementCreator,
			ResultSetExtractor<T> resultSetExtractor) throws DataAccessException {

		return query(preparedStatementCreator, null, resultSetExtractor);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.AsyncCqlOperations#query(org.springframework.data.cassandra.core.cql.AsyncPreparedStatementCreator, org.springframework.data.cassandra.core.cql.RowCallbackHandler)
	 */
	@Override
	public ListenableFuture<Void> query(AsyncPreparedStatementCreator preparedStatementCreator,
			RowCallbackHandler rowCallbackHandler) throws DataAccessException {

		ListenableFuture<?> results = query(preparedStatementCreator, null, newResultSetExtractor(rowCallbackHandler));

		return new ExceptionTranslatingListenableFutureAdapter<>(new MappingListenableFutureAdapter<>(results, o -> null),
				getExceptionTranslator());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.AsyncCqlOperations#query(org.springframework.data.cassandra.core.cql.AsyncPreparedStatementCreator, org.springframework.data.cassandra.core.cql.RowMapper)
	 */
	@Override
	public <T> ListenableFuture<List<T>> query(AsyncPreparedStatementCreator preparedStatementCreator,
			RowMapper<T> rowMapper) throws DataAccessException {

		return query(preparedStatementCreator, null, newResultSetExtractor(rowMapper));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.AsyncCqlOperations#query(org.springframework.data.cassandra.core.cql.AsyncPreparedStatementCreator, org.springframework.data.cassandra.core.cql.PreparedStatementBinder, org.springframework.data.cassandra.core.cql.ResultSetExtractor)
	 */
	@Override
	public <T> ListenableFuture<T> query(AsyncPreparedStatementCreator preparedStatementCreator,
			@Nullable PreparedStatementBinder psb, ResultSetExtractor<T> resultSetExtractor)
			throws DataAccessException {

		Assert.notNull(preparedStatementCreator, "AsyncPreparedStatementCreator must not be null");
		Assert.notNull(resultSetExtractor, "ResultSetExtractor object must not be null");

		PersistenceExceptionTranslator exceptionTranslator = ex -> translateExceptionIfPossible("Query",
				toCql(preparedStatementCreator), ex);

		try {

			if (logger.isDebugEnabled()) {
				logger.debug("Preparing statement [{}] using {}", toCql(preparedStatementCreator), preparedStatementCreator);
			}

			Session session = getCurrentSession();

			ListenableFuture<BoundStatement> statementFuture = new MappingListenableFutureAdapter<>(
					preparedStatementCreator.createPreparedStatement(session), preparedStatement -> {
						if (logger.isDebugEnabled()) {
							logger.debug("Executing prepared statement [{}]", preparedStatement);
						}

						return applyStatementSettings(psb != null ? psb.bindValues(preparedStatement) : preparedStatement.bind());
					});

			SettableListenableFuture<T> settableListenableFuture = new SettableListenableFuture<>();

			statementFuture.addCallback(
					boundStatement -> Futures.addCallback(session.executeAsync(boundStatement), new FutureCallback<ResultSet>() {
						@Override
						public void onSuccess(@Nullable ResultSet result) {
							try {
								settableListenableFuture.set(result != null ? resultSetExtractor.extractData(result) : null);
							} catch (DriverException e) {
								settableListenableFuture.setException(translateException(exceptionTranslator, e));
							}
						}

						@Override
						public void onFailure(Throwable ex) {
							if (ex instanceof DriverException) {
								settableListenableFuture.setException(translateException(exceptionTranslator, (DriverException) ex));
							} else {
								settableListenableFuture.setException(ex);
							}
						}
					}), ex -> {
						if (ex instanceof DriverException) {
							settableListenableFuture.setException(translateException(exceptionTranslator, (DriverException) ex));
						} else {
							settableListenableFuture.setException(ex);
						}
					});

			return settableListenableFuture;

		} catch (DriverException e) {
			throw translateException(exceptionTranslator, e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.AsyncCqlOperations#query(org.springframework.data.cassandra.core.cql.AsyncPreparedStatementCreator, org.springframework.data.cassandra.core.cql.PreparedStatementBinder, org.springframework.data.cassandra.core.cql.RowCallbackHandler)
	 */
	@Override
	public ListenableFuture<Void> query(AsyncPreparedStatementCreator preparedStatementCreator,
			@Nullable PreparedStatementBinder psb, RowCallbackHandler rowCallbackHandler)
			throws DataAccessException {

		ListenableFuture<?> results = query(preparedStatementCreator, psb,
				newResultSetExtractor(rowCallbackHandler));

		return new ExceptionTranslatingListenableFutureAdapter<>(new MappingListenableFutureAdapter<>(results, o -> null),
				getExceptionTranslator());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.AsyncCqlOperations#query(org.springframework.data.cassandra.core.cql.AsyncPreparedStatementCreator, org.springframework.data.cassandra.core.cql.PreparedStatementBinder, org.springframework.data.cassandra.core.cql.RowMapper)
	 */
	@Override
	public <T> ListenableFuture<List<T>> query(AsyncPreparedStatementCreator preparedStatementCreator,
			@Nullable PreparedStatementBinder psb, RowMapper<T> rowMapper) throws DataAccessException {
		return query(preparedStatementCreator, psb, newResultSetExtractor(rowMapper));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.AsyncCqlOperations#query(java.lang.String, org.springframework.data.cassandra.core.cql.ResultSetExtractor, java.lang.Object[])
	 */
	@Override
	public <T> ListenableFuture<T> query(String cql, ResultSetExtractor<T> resultSetExtractor, Object... args)
			throws DataAccessException {
		return query(newAsyncPreparedStatementCreator(cql), newPreparedStatementBinder(args), resultSetExtractor);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.AsyncCqlOperations#query(java.lang.String, org.springframework.data.cassandra.core.cql.RowCallbackHandler, java.lang.Object[])
	 */
	@Override
	public ListenableFuture<Void> query(String cql, RowCallbackHandler rowCallbackHandler, Object... args)
			throws DataAccessException {

		ListenableFuture<?> results = query(newAsyncPreparedStatementCreator(cql), newPreparedStatementBinder(args),
				newResultSetExtractor(rowCallbackHandler));

		return new ExceptionTranslatingListenableFutureAdapter<>(new MappingListenableFutureAdapter<>(results, o -> null),
				getExceptionTranslator());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.AsyncCqlOperations#query(java.lang.String, org.springframework.data.cassandra.core.cql.RowMapper, java.lang.Object[])
	 */
	@Override
	public <T> ListenableFuture<List<T>> query(String cql, RowMapper<T> rowMapper, Object... args)
			throws DataAccessException {
		return query(newAsyncPreparedStatementCreator(cql), newPreparedStatementBinder(args),
				newResultSetExtractor(rowMapper));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.AsyncCqlOperations#query(java.lang.String, org.springframework.data.cassandra.core.cql.PreparedStatementBinder, org.springframework.data.cassandra.core.cql.ResultSetExtractor)
	 */
	@Override
	public <T> ListenableFuture<T> query(String cql, @Nullable PreparedStatementBinder psb,
			ResultSetExtractor<T> resultSetExtractor) throws DataAccessException {

		return query(newAsyncPreparedStatementCreator(cql), psb, resultSetExtractor);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.AsyncCqlOperations#query(java.lang.String, org.springframework.data.cassandra.core.cql.PreparedStatementBinder, org.springframework.data.cassandra.core.cql.RowCallbackHandler)
	 */
	@Override
	public ListenableFuture<Void> query(String cql, @Nullable PreparedStatementBinder psb,
			RowCallbackHandler rowCallbackHandler) throws DataAccessException {

		ListenableFuture<?> results = query(newAsyncPreparedStatementCreator(cql), psb,
				newResultSetExtractor(rowCallbackHandler));

		return new ExceptionTranslatingListenableFutureAdapter<>(new MappingListenableFutureAdapter<>(results, o -> null),
				getExceptionTranslator());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.AsyncCqlOperations#query(java.lang.String, org.springframework.data.cassandra.core.cql.PreparedStatementBinder, org.springframework.data.cassandra.core.cql.RowMapper)
	 */
	@Override
	public <T> ListenableFuture<List<T>> query(String cql, @Nullable PreparedStatementBinder psb,
			RowMapper<T> rowMapper) throws DataAccessException {

		return query(newAsyncPreparedStatementCreator(cql), psb, newResultSetExtractor(rowMapper));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.AsyncCqlOperations#queryForList(java.lang.String, java.lang.Object[])
	 */
	@Override
	public ListenableFuture<List<Map<String, Object>>> queryForList(String cql, Object... args)
			throws DataAccessException {

		return query(newAsyncPreparedStatementCreator(cql), newPreparedStatementBinder(args),
				newResultSetExtractor(newColumnMapRowMapper()));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.AsyncCqlOperations#queryForList(java.lang.String, java.lang.Class, java.lang.Object[])
	 */
	@Override
	public <T> ListenableFuture<List<T>> queryForList(String cql, Class<T> elementType, Object... args)
			throws DataAccessException {

		return query(newAsyncPreparedStatementCreator(cql), newPreparedStatementBinder(args),
				newResultSetExtractor(newSingleColumnRowMapper(elementType)));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.AsyncCqlOperations#queryForMap(java.lang.String, java.lang.Object[])
	 */
	@Override
	public ListenableFuture<Map<String, Object>> queryForMap(String cql, Object... args) throws DataAccessException {
		return queryForObject(cql, newColumnMapRowMapper(), args);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.AsyncCqlOperations#queryForObject(java.lang.String, java.lang.Class, java.lang.Object[])
	 */
	@Override
	public <T> ListenableFuture<T> queryForObject(String cql, Class<T> requiredType, Object... args)
			throws DataAccessException {

		return queryForObject(cql, newSingleColumnRowMapper(requiredType), args);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.AsyncCqlOperations#queryForObject(java.lang.String, org.springframework.data.cassandra.core.cql.RowMapper, java.lang.Object[])
	 */
	@Override
	public <T> ListenableFuture<T> queryForObject(String cql, RowMapper<T> rowMapper, Object... args)
			throws DataAccessException {

		ListenableFuture<List<T>> results = query(newAsyncPreparedStatementCreator(cql), newPreparedStatementBinder(args),
				newResultSetExtractor(rowMapper, 1));

		return new ExceptionTranslatingListenableFutureAdapter<>(
				new MappingListenableFutureAdapter<>(results, DataAccessUtils::requiredSingleResult), getExceptionTranslator());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.AsyncCqlOperations#queryForResultSet(java.lang.String, java.lang.Object[])
	 */
	@Override
	public ListenableFuture<ResultSet> queryForResultSet(String cql, Object... args) throws DataAccessException {
		return query(cql, rs -> rs, args);
	}

	// -------------------------------------------------------------------------
	// Implementation hooks and helper methods
	// -------------------------------------------------------------------------

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
	 * Translate the given {@link DriverException} into a generic {@link DataAccessException}.
	 *
	 * @param task readable text describing the task being attempted
	 * @param cql CQL query or update that caused the problem (may be {@literal null})
	 * @param ex the offending {@code RuntimeException}.
	 * @return the exception translation {@link Function}
	 * @see CqlProvider
	 */
	protected DataAccessException translateException(String task, @Nullable String cql, DriverException ex) {
		return translate(task, cql, ex);
	}

	/**
	 * Translate the given {@link DriverException} into a generic {@link DataAccessException}.
	 *
	 * @param task readable text describing the task being attempted
	 * @param cql CQL query or update that caused the problem (may be {@literal null})
	 * @param ex the offending {@code RuntimeException}.
	 * @return the translated {@link DataAccessException} or {@literal null} if translation not possible.
	 * @see CqlProvider
	 */
	@Nullable
	protected DataAccessException translateExceptionIfPossible(String task, @Nullable String cql, RuntimeException ex) {
		return (ex instanceof DriverException ? translate(task, cql, (DriverException) ex) : null);
	}

	private Session getCurrentSession() {

		SessionFactory sessionFactory = getSessionFactory();

		Assert.state(sessionFactory != null, "SessionFactory is null");

		return sessionFactory.getSession();
	}

	private static RuntimeException translateException(PersistenceExceptionTranslator exceptionTranslator,
			DriverException e) {

		DataAccessException translated = exceptionTranslator.translateExceptionIfPossible(e);
		return translated == null ? e : translated;
	}

	private static class SimpleAsyncPreparedStatementCreator implements AsyncPreparedStatementCreator, CqlProvider {

		private final PersistenceExceptionTranslator persistenceExceptionTranslator;

		private final String cql;

		private SimpleAsyncPreparedStatementCreator(String cql,
				PersistenceExceptionTranslator persistenceExceptionTranslator) {

			Assert.hasText(cql, "CQL must not be empty");

			this.cql = cql;
			this.persistenceExceptionTranslator = persistenceExceptionTranslator;
		}

		@Override
		public String getCql() {
			return this.cql;
		}

		@Override
		public ListenableFuture<PreparedStatement> createPreparedStatement(Session session) throws DriverException {

			return new GuavaListenableFutureAdapter<>(session.prepareAsync(getCql()), this.persistenceExceptionTranslator);
		}
	}

	private static class MappingListenableFutureAdapter<T, S>
			extends org.springframework.util.concurrent.ListenableFutureAdapter<T, S> {

		private final Function<S, T> mapper;

		private MappingListenableFutureAdapter(ListenableFuture<S> adaptee, Function<S, T> mapper) {
			super(adaptee);
			this.mapper = mapper;
		}

		@Override
		protected T adapt(S adapteeResult) throws ExecutionException {
			return mapper.apply(adapteeResult);
		}
	}
}
