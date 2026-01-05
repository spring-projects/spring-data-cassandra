/*
 * Copyright 2016-present the original author or authors.
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

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import org.jspecify.annotations.Nullable;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.DataAccessUtils;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.cassandra.SessionFactory;
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;

/**
 * <b>This is the central class in the CQL core package for asynchronous Cassandra data access.</b> It simplifies the
 * use of CQL and helps to avoid common errors. It executes core CQL workflow, leaving application code to provide CQL
 * and extract results. This class executes CQL queries or updates, initiating iteration over {@link ResultSet}s and
 * catching {@link DriverException} exceptions and translating them to the generic, more informative exception hierarchy
 * defined in the {@code org.springframework.dao} package.
 * <p>
 * Code using this class need only implement callback interfaces, giving them a clearly defined contract. The
 * {@link PreparedStatementCreator} callback interface creates a prepared statement given a Connection, providing CQL
 * and any necessary parameters. The {@link AsyncResultSetExtractor} interface extracts values from a {@link ResultSet}.
 * See also {@link PreparedStatementBinder} and {@link RowMapper} for two popular alternative callback interfaces.
 * <p>
 * Can be used within a service implementation via direct instantiation with a {@link CqlSession} reference, or get
 * prepared in an application context and given to services as bean reference. Note: The {@link CqlSession} should
 * always be configured as a bean in the application context, in the first case given to the service directly, in the
 * second case to the prepared template.
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
 * @since 2.0
 * @see CompletableFuture
 * @see PreparedStatementCreator
 * @see PreparedStatementBinder
 * @see PreparedStatementCallback
 * @see AsyncResultSetExtractor
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
	 * Create a new {@link AsyncCqlTemplate} with the given {@link CqlSession}.
	 *
	 * @param session the active Cassandra {@link CqlSession}, must not be {@literal null}.
	 * @throws IllegalStateException if {@link CqlSession} is {@literal null}.
	 */
	public AsyncCqlTemplate(CqlSession session) {

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
	// Methods dealing with a plain com.datastax.oss.driver.api.core.CqlSession
	// -------------------------------------------------------------------------

	@Override
	public <T extends @Nullable Object> CompletableFuture<T> execute(AsyncSessionCallback<T> action)
			throws DataAccessException {

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

	@Override
	public CompletableFuture<Boolean> execute(String cql) throws DataAccessException {

		Assert.hasText(cql, "CQL must not be empty");

		return queryForResultSet(cql).thenApply(AsyncResultSet::wasApplied);
	}

	@Override
	public <T extends @Nullable Object> CompletableFuture<T> query(String cql,
			AsyncResultSetExtractor<T> resultSetExtractor)
			throws DataAccessException {

		Assert.hasText(cql, "CQL must not be empty");
		Assert.notNull(resultSetExtractor, "AsyncResultSetExtractor must not be null");

		try {
			if (logger.isDebugEnabled()) {
				logger.debug(String.format("Executing CQL statement [%s]", cql));
			}

			CompletionStage<T> results = getCurrentSession().executeAsync(applyStatementSettings(newStatement(cql)))
					.thenApply(resultSetExtractor::extractData) //
					.thenCompose(Function.identity());

			return results.exceptionallyCompose(exceptionComposition(ex -> translateExceptionIfPossible("Query", cql, ex)))
					.toCompletableFuture();
		} catch (DriverException e) {
			throw translateException("Query", cql, e);
		}
	}

	@Override
	public CompletableFuture<Void> query(String cql, RowCallbackHandler rowCallbackHandler) throws DataAccessException {

		CompletableFuture<?> results = query(cql, newAsyncResultSetExtractor(rowCallbackHandler));

		return results.thenApply(o -> null);
	}

	@Override
	public <T extends @Nullable Object> CompletableFuture<List<T>> query(String cql, RowMapper<T> rowMapper)
			throws DataAccessException {
		return query(cql, newAsyncResultSetExtractor(rowMapper));
	}

	@Override
	public CompletableFuture<List<Map<String, Object>>> queryForList(String cql) throws DataAccessException {
		return query(cql, newAsyncResultSetExtractor(newColumnMapRowMapper()));
	}

	@Override
	public <T extends @Nullable Object> CompletableFuture<List<T>> queryForList(String cql, Class<T> elementType)
			throws DataAccessException {
		return query(cql, newAsyncResultSetExtractor(newSingleColumnRowMapper(elementType)));
	}

	@Override
	public CompletableFuture<Map<String, Object>> queryForMap(String cql) throws DataAccessException {
		return queryForObject(cql, newColumnMapRowMapper());
	}

	@Override
	public <T extends @Nullable Object> CompletableFuture<T> queryForObject(String cql, Class<T> requiredType)
			throws DataAccessException {
		return queryForObject(cql, newSingleColumnRowMapper(requiredType));
	}

	@Override
	public <T extends @Nullable Object> CompletableFuture<T> queryForObject(String cql, RowMapper<T> rowMapper)
			throws DataAccessException {

		CompletableFuture<List<T>> results = query(cql, newAsyncResultSetExtractor(rowMapper));

		return results.thenApply(DataAccessUtils::nullableSingleResult);
	}

	@Override
	public CompletableFuture<AsyncResultSet> queryForResultSet(String cql) throws DataAccessException {
		return query(cql, AsyncCqlTemplate::toResultSet);
	}

	// -------------------------------------------------------------------------
	// Methods dealing with com.datastax.oss.driver.api.core.cql.Statement
	// -------------------------------------------------------------------------

	@Override
	public CompletableFuture<Boolean> execute(Statement<?> statement) throws DataAccessException {

		Assert.notNull(statement, "CQL Statement must not be null");

		return queryForResultSet(statement).thenApply(AsyncResultSet::wasApplied);
	}

	@Override
	public <T extends @Nullable Object> CompletableFuture<T> query(Statement<?> statement,
			AsyncResultSetExtractor<T> resultSetExtractor)
			throws DataAccessException {

		Assert.notNull(statement, "CQL Statement must not be null");
		Assert.notNull(resultSetExtractor, "AsyncResultSetExtractor must not be null");

		try {
			if (logger.isDebugEnabled()) {
				logger.debug(String.format("Executing statement [%s]", toCql(statement)));
			}

			CompletionStage<T> results = getCurrentSession() //
					.executeAsync(applyStatementSettings(statement)) //
					.thenApply(resultSetExtractor::extractData) //
					.thenCompose(Function.identity());

			return results
					.exceptionallyCompose(exceptionComposition(ex -> translateExceptionIfPossible("Query", toCql(statement), ex)))
					.toCompletableFuture();
		} catch (DriverException e) {
			throw translateException("Query", toCql(statement), e);
		}
	}

	@Override
	public CompletableFuture<Void> query(Statement<?> statement, RowCallbackHandler rowCallbackHandler)
			throws DataAccessException {

		CompletableFuture<?> result = query(statement, newAsyncResultSetExtractor(rowCallbackHandler));

		return result.thenApply(it -> null);
	}

	@Override
	public <T extends @Nullable Object> CompletableFuture<List<T>> query(Statement<?> statement, RowMapper<T> rowMapper)
			throws DataAccessException {
		return query(statement, newAsyncResultSetExtractor(rowMapper));
	}

	@Override
	public CompletableFuture<List<Map<String, Object>>> queryForList(Statement<?> statement) throws DataAccessException {
		return query(statement, newAsyncResultSetExtractor(newColumnMapRowMapper()));
	}

	@Override
	public <T extends @Nullable Object> CompletableFuture<List<T>> queryForList(Statement<?> statement,
			Class<T> elementType)
			throws DataAccessException {
		return query(statement, newAsyncResultSetExtractor(newSingleColumnRowMapper(elementType)));
	}

	@Override
	public CompletableFuture<Map<String, Object>> queryForMap(Statement<?> statement) throws DataAccessException {
		return queryForObject(statement, newColumnMapRowMapper());
	}

	@Override
	public <T extends @Nullable Object> CompletableFuture<T> queryForObject(Statement<?> statement, Class<T> requiredType)
			throws DataAccessException {
		return queryForObject(statement, newSingleColumnRowMapper(requiredType));
	}

	@Override
	public <T extends @Nullable Object> CompletableFuture<T> queryForObject(Statement<?> statement,
			RowMapper<T> rowMapper)
			throws DataAccessException {

		CompletableFuture<List<T>> results = query(statement, newAsyncResultSetExtractor(rowMapper));

		return results.thenApply(DataAccessUtils::nullableSingleResult);
	}

	@Override
	public CompletableFuture<AsyncResultSet> queryForResultSet(Statement<?> statement) throws DataAccessException {
		return query(statement, AsyncCqlTemplate::toResultSet);
	}

	// -------------------------------------------------------------------------
	// Methods dealing with com.datastax.driver.core.PreparedStatement
	// -------------------------------------------------------------------------

	@Override
	public CompletableFuture<Boolean> execute(AsyncPreparedStatementCreator preparedStatementCreator)
			throws DataAccessException {

		return query(preparedStatementCreator, AsyncCqlTemplate::toResultSet).thenApply(AsyncResultSet::wasApplied);
	}

	@Override
	public CompletableFuture<Boolean> execute(String cql, Object... args) throws DataAccessException {
		return execute(cql, newPreparedStatementBinder(args));
	}

	@Override
	public CompletableFuture<Boolean> execute(String cql, @Nullable PreparedStatementBinder psb)
			throws DataAccessException {

		return query(newAsyncPreparedStatementCreator(cql), psb, AsyncCqlTemplate::toResultSet)
				.thenApply(AsyncResultSet::wasApplied);
	}

	@Override
	public <T extends @Nullable Object> CompletableFuture<T> execute(String cql, PreparedStatementCallback<T> action)
			throws DataAccessException {
		return execute(newAsyncPreparedStatementCreator(cql), action);
	}

	@Override
	public <T extends @Nullable Object> CompletableFuture<T> execute(
			AsyncPreparedStatementCreator preparedStatementCreator,
			PreparedStatementCallback<T> action) throws DataAccessException {

		Assert.notNull(preparedStatementCreator, "PreparedStatementCreator must not be null");
		Assert.notNull(action, "PreparedStatementCallback object must not be null");

		PersistenceExceptionTranslator exceptionTranslator = ex -> translateExceptionIfPossible("PreparedStatementCallback",
				toCql(preparedStatementCreator), ex);
		try {
			if (logger.isDebugEnabled()) {
				logger.debug(String.format("Preparing statement [%s] using %s", toCql(preparedStatementCreator),
						preparedStatementCreator));
			}

			CqlSession currentSession = getCurrentSession();

			return preparedStatementCreator.createPreparedStatement(currentSession).thenApply(preparedStatement -> {
				try {
					return action.doInPreparedStatement(currentSession, preparedStatement);
				} catch (DriverException e) {
					throw translateException(exceptionTranslator, e);
				}
			}).toCompletableFuture()
					.exceptionallyCompose(exceptionComposition(exceptionTranslator::translateExceptionIfPossible));

		} catch (DriverException e) {
			throw translateException(exceptionTranslator, e);
		}
	}

	@Override
	public <T extends @Nullable Object> CompletableFuture<T> query(AsyncPreparedStatementCreator preparedStatementCreator,
			AsyncResultSetExtractor<T> resultSetExtractor) throws DataAccessException {

		return query(preparedStatementCreator, null, resultSetExtractor);
	}

	@Override
	public CompletableFuture<Void> query(AsyncPreparedStatementCreator preparedStatementCreator,
			RowCallbackHandler rowCallbackHandler) throws DataAccessException {

		CompletableFuture<?> results = query(preparedStatementCreator, null,
				newAsyncResultSetExtractor(rowCallbackHandler));

		return results.thenApply(o -> null);
	}

	@Override
	public <T extends @Nullable Object> CompletableFuture<List<T>> query(
			AsyncPreparedStatementCreator preparedStatementCreator,
			RowMapper<T> rowMapper) throws DataAccessException {

		return query(preparedStatementCreator, null, newAsyncResultSetExtractor(rowMapper));
	}

	@Override
	public <T extends @Nullable Object> CompletableFuture<T> query(AsyncPreparedStatementCreator preparedStatementCreator,
			@Nullable PreparedStatementBinder psb, AsyncResultSetExtractor<T> resultSetExtractor) throws DataAccessException {

		Assert.notNull(preparedStatementCreator, "AsyncPreparedStatementCreator must not be null");
		Assert.notNull(resultSetExtractor, "AsyncResultSetExtractor object must not be null");

		PersistenceExceptionTranslator exceptionTranslator = ex -> translateExceptionIfPossible("Query",
				toCql(preparedStatementCreator), ex);

		try {

			if (logger.isDebugEnabled()) {
				logger.debug(String.format("Preparing statement [%s] using %s", toCql(preparedStatementCreator),
						preparedStatementCreator));
			}

			CqlSession session = getCurrentSession();

			CompletionStage<Statement<?>> statementFuture = preparedStatementCreator.createPreparedStatement(session)
					.thenApply(preparedStatement -> {
						if (logger.isDebugEnabled()) {
							logger.debug(String.format("Executing prepared statement [%s]", toCql(preparedStatement)));
						}

						return applyStatementSettings(psb != null ? psb.bindValues(preparedStatement) : preparedStatement.bind());
					});

			CompletableFuture<T> result = statementFuture.toCompletableFuture() //
					.thenCompose(session::executeAsync) //
					.thenApply(resultSetExtractor::extractData) //
					.thenCompose(Function.identity());

			return result.exceptionallyCompose(exceptionComposition(this::translateExceptionIfPossible));
		} catch (DriverException e) {
			throw translateException(exceptionTranslator, e);
		}
	}

	@Override
	public CompletableFuture<Void> query(AsyncPreparedStatementCreator preparedStatementCreator,
			@Nullable PreparedStatementBinder psb, RowCallbackHandler rowCallbackHandler) throws DataAccessException {

		CompletableFuture<?> results = query(preparedStatementCreator, psb, newAsyncResultSetExtractor(rowCallbackHandler));

		return results.thenApply(o -> null);
	}

	@Override
	public <T extends @Nullable Object> CompletableFuture<List<T>> query(
			AsyncPreparedStatementCreator preparedStatementCreator,
			@Nullable PreparedStatementBinder psb, RowMapper<T> rowMapper) throws DataAccessException {
		return query(preparedStatementCreator, psb, newAsyncResultSetExtractor(rowMapper));
	}

	@Override
	public <T extends @Nullable Object> CompletableFuture<T> query(String cql,
			AsyncResultSetExtractor<T> resultSetExtractor, Object... args)
			throws DataAccessException {
		return query(newAsyncPreparedStatementCreator(cql), newPreparedStatementBinder(args), resultSetExtractor);
	}

	@Override
	public CompletableFuture<Void> query(String cql, RowCallbackHandler rowCallbackHandler, Object... args)
			throws DataAccessException {

		CompletableFuture<?> results = query(newAsyncPreparedStatementCreator(cql), newPreparedStatementBinder(args),
				newAsyncResultSetExtractor(rowCallbackHandler));

		return results.thenApply(o -> null);
	}

	@Override
	public <T extends @Nullable Object> CompletableFuture<List<T>> query(String cql, RowMapper<T> rowMapper,
			Object... args)
			throws DataAccessException {
		return query(newAsyncPreparedStatementCreator(cql), newPreparedStatementBinder(args),
				newAsyncResultSetExtractor(rowMapper));
	}

	@Override
	public <T extends @Nullable Object> CompletableFuture<T> query(String cql, @Nullable PreparedStatementBinder psb,
			AsyncResultSetExtractor<T> resultSetExtractor) throws DataAccessException {

		return query(newAsyncPreparedStatementCreator(cql), psb, resultSetExtractor);
	}

	@Override
	public CompletableFuture<Void> query(String cql, @Nullable PreparedStatementBinder psb,
			RowCallbackHandler rowCallbackHandler) throws DataAccessException {

		CompletableFuture<?> results = query(newAsyncPreparedStatementCreator(cql), psb,
				newAsyncResultSetExtractor(rowCallbackHandler));

		return results.thenApply(o -> null);
	}

	@Override
	public <T extends @Nullable Object> CompletableFuture<List<T>> query(String cql,
			@Nullable PreparedStatementBinder psb, RowMapper<T> rowMapper)
			throws DataAccessException {

		return query(newAsyncPreparedStatementCreator(cql), psb, newAsyncResultSetExtractor(rowMapper));
	}

	@Override
	public CompletableFuture<List<Map<String, Object>>> queryForList(String cql, Object... args)
			throws DataAccessException {

		return query(newAsyncPreparedStatementCreator(cql), newPreparedStatementBinder(args),
				newAsyncResultSetExtractor(newColumnMapRowMapper()));
	}

	@Override
	public <T extends @Nullable Object> CompletableFuture<List<T>> queryForList(String cql, Class<T> elementType,
			Object... args)
			throws DataAccessException {

		return query(newAsyncPreparedStatementCreator(cql), newPreparedStatementBinder(args),
				newAsyncResultSetExtractor(newSingleColumnRowMapper(elementType)));
	}

	@Override
	public CompletableFuture<Map<String, Object>> queryForMap(String cql, Object... args) throws DataAccessException {
		return queryForObject(cql, newColumnMapRowMapper(), args);
	}

	@Override
	public <T extends @Nullable Object> CompletableFuture<T> queryForObject(String cql, Class<T> requiredType,
			Object... args)
			throws DataAccessException {

		return queryForObject(cql, newSingleColumnRowMapper(requiredType), args);
	}

	@Override
	public <T extends @Nullable Object> CompletableFuture<T> queryForObject(String cql, RowMapper<T> rowMapper,
			Object... args)
			throws DataAccessException {

		CompletableFuture<List<T>> results = query(newAsyncPreparedStatementCreator(cql), newPreparedStatementBinder(args),
				newAsyncResultSetExtractor(rowMapper));

		return results.thenApply(DataAccessUtils::nullableSingleResult);
	}

	@Override
	public CompletableFuture<AsyncResultSet> queryForResultSet(String cql, Object... args) throws DataAccessException {
		return query(cql, AsyncCqlTemplate::toResultSet, args);
	}

	// -------------------------------------------------------------------------
	// Implementation hooks and helper methods
	// -------------------------------------------------------------------------

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
		return translate(task, cql, ex);
	}

	/**
	 * Create a new CQL-based {@link AsyncPreparedStatementCreator} using the CQL passed in. By default, we'll create an
	 * {@link SimpleAsyncPreparedStatementCreator}. This method allows for the creation to be overridden by subclasses.
	 *
	 * @param cql static CQL to execute, must not be empty or {@literal null}.
	 * @return the new {@link AsyncPreparedStatementCreator} to use
	 */
	protected AsyncPreparedStatementCreator newAsyncPreparedStatementCreator(String cql) {
		return new SimpleAsyncPreparedStatementCreator(
				(SimpleStatement) applyStatementSettings(SimpleStatement.newInstance(cql)),
				ex -> translateExceptionIfPossible("PrepareStatement", cql, ex));
	}

	/**
	 * Constructs a new instance of the {@link ResultSetExtractor} adapting the given {@link RowCallbackHandler}.
	 *
	 * @param rowCallbackHandler {@link RowCallbackHandler} to adapt as a {@link ResultSetExtractor}.
	 * @return a {@link ResultSetExtractor} implementation adapting an instance of the {@link RowCallbackHandler}.
	 * @see AsyncRowCallbackHandlerResultSetExtractor
	 * @see ResultSetExtractor
	 * @see RowCallbackHandler
	 */
	protected AsyncRowCallbackHandlerResultSetExtractor newAsyncResultSetExtractor(
			RowCallbackHandler rowCallbackHandler) {
		return new AsyncRowCallbackHandlerResultSetExtractor(rowCallbackHandler);
	}

	/**
	 * Constructs a new instance of the {@link ResultSetExtractor} adapting the given {@link RowMapper}.
	 *
	 * @param rowMapper {@link RowMapper} to adapt as a {@link ResultSetExtractor}.
	 * @return a {@link ResultSetExtractor} implementation adapting an instance of the {@link RowMapper}.
	 * @see ResultSetExtractor
	 * @see RowMapper
	 * @see RowMapperResultSetExtractor
	 */
	protected <T extends @Nullable Object> AsyncRowMapperResultSetExtractor<T> newAsyncResultSetExtractor(
			RowMapper<T> rowMapper) {
		return new AsyncRowMapperResultSetExtractor<>(rowMapper);
	}

	private CqlSession getCurrentSession() {

		SessionFactory sessionFactory = getSessionFactory();

		Assert.state(sessionFactory != null, "SessionFactory is null");

		return sessionFactory.getSession();
	}

	private static <T extends @Nullable Object> Function<Throwable, CompletionStage<T>> exceptionComposition(
			Function<RuntimeException, Exception> exceptionTranslator) {
		return throwable -> {

			Throwable toTranslate = throwable;
			Throwable translated = null;
			if (toTranslate instanceof ExecutionException e) {
				toTranslate = e.getCause();
			}

			if (toTranslate instanceof CompletionException e) {
				toTranslate = e.getCause();
			}

			if (toTranslate instanceof RuntimeException e) {
				translated = exceptionTranslator.apply(e);
			}

			return CompletableFuture.failedFuture(translated != null ? translated : toTranslate);
		};
	}

	private static CompletableFuture<AsyncResultSet> toResultSet(AsyncResultSet resultSet) {
		return CompletableFuture.completedFuture(resultSet);
	}

	private static RuntimeException translateException(PersistenceExceptionTranslator exceptionTranslator,
			DriverException e) {

		DataAccessException translated = exceptionTranslator.translateExceptionIfPossible(e);
		return translated == null ? e : translated;
	}

	private static class SimpleAsyncPreparedStatementCreator implements AsyncPreparedStatementCreator, CqlProvider {

		private final PersistenceExceptionTranslator exceptionTranslator;

		private final SimpleStatement statement;

		private SimpleAsyncPreparedStatementCreator(SimpleStatement statement,
				PersistenceExceptionTranslator exceptionTranslator) {

			this.statement = statement;
			this.exceptionTranslator = exceptionTranslator;
		}

		@Override
		public CompletableFuture<PreparedStatement> createPreparedStatement(CqlSession session) throws DriverException {
			return session.prepareAsync(this.statement)
					.exceptionallyCompose(exceptionComposition(exceptionTranslator::translateExceptionIfPossible))
					.toCompletableFuture();
		}

		@Override
		public String getCql() {
			return this.statement.getQuery();
		}
	}

	/**
	 * Adapter to enable use of a {@link RowCallbackHandler} inside a {@link ResultSetExtractor}.
	 */
	protected static class AsyncRowCallbackHandlerResultSetExtractor implements AsyncResultSetExtractor<Void> {

		private final RowCallbackHandler rowCallbackHandler;

		protected AsyncRowCallbackHandlerResultSetExtractor(RowCallbackHandler rowCallbackHandler) {
			this.rowCallbackHandler = rowCallbackHandler;
		}

		@Override
		public CompletableFuture<Void> extractData(AsyncResultSet resultSet) {
			return AsyncResultStream.from(resultSet).forEach(rowCallbackHandler::processRow);
		}
	}
}
