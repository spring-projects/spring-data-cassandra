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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.DataAccessUtils;
import org.springframework.data.cassandra.SessionFactory;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverException;

/**
 * <b>This is the central class in the CQL core package.</b> It simplifies the use of CQL and helps to avoid common
 * errors. It executes core CQL workflow, leaving application code to provide CQL and extract results. This class
 * executes CQL queries or updates, initiating iteration over {@link ResultSet}s and catching {@link DriverException}
 * exceptions and translating them to the generic, more informative exception hierarchy defined in the
 * {@code org.springframework.dao} package.
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
 * @author David Webb
 * @author Matthew Adams
 * @author Ryan Scheidter
 * @author Antoine Toulme
 * @author John Blum
 * @author Mark Paluch
 * @see PreparedStatementCreator
 * @see PreparedStatementBinder
 * @see PreparedStatementCallback
 * @see ResultSetExtractor
 * @see RowCallbackHandler
 * @see RowMapper
 * @see org.springframework.dao.support.PersistenceExceptionTranslator
 */
public class CqlTemplate extends CassandraAccessor implements CqlOperations {

	/**
	 * Create a new, uninitialized {@link CqlTemplate}. Note: The {@link SessionFactory} has to be set before using the
	 * instance.
	 *
	 * @see #setSessionFactory(SessionFactory)
	 */
	public CqlTemplate() {}

	/**
	 * Create a new {@link CqlTemplate} initialized with the given {@link Session}.
	 *
	 * @param session the active Cassandra {@link Session}.
	 * @throws IllegalStateException if {@link Session} is {@literal null}.
	 * @see com.datastax.driver.core.Session
	 */
	public CqlTemplate(Session session) {

		Assert.notNull(session, "Session must not be null");

		setSession(session);
	}

	/**
	 * Constructs a new {@link CqlTemplate} with the given {@link SessionFactory}.
	 *
	 * @param sessionFactory the active Cassandra {@link SessionFactory}, must not be {@literal null}.
	 * @see SessionFactory
	 */
	public CqlTemplate(SessionFactory sessionFactory) {

		Assert.notNull(sessionFactory, "SessionFactory must not be null");

		setSessionFactory(sessionFactory);
	}

	// -------------------------------------------------------------------------
	// Methods dealing with a plain com.datastax.driver.core.Session
	// -------------------------------------------------------------------------

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#execute(org.springframework.data.cassandra.core.cql.SessionCallback)
	 */
	@Override
	public <T> T execute(SessionCallback<T> action) throws DataAccessException {

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
	 * @see org.springframework.data.cassandra.core.cqlOperations#execute(java.lang.String)
	 */
	@Override
	public boolean execute(String cql) throws DataAccessException {

		Assert.hasText(cql, "CQL must not be empty");

		return queryForResultSet(cql).wasApplied();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#query(java.lang.String, org.springframework.data.cassandra.core.cql.ResultSetExtractor)
	 */
	@Override
	@Nullable
	public <T> T query(String cql, ResultSetExtractor<T> resultSetExtractor) throws DataAccessException {

		Assert.hasText(cql, "CQL must not be empty");
		Assert.notNull(resultSetExtractor, "ResultSetExtractor must not be null");

		try {
			if (logger.isDebugEnabled()) {
				logger.debug("Executing CQL Statement [{}]", cql);
			}

			SimpleStatement statement = applyStatementSettings(new SimpleStatement(cql));

			ResultSet results = getCurrentSession().execute(statement);

			return resultSetExtractor.extractData(results);
		} catch (DriverException e) {
			throw translateException("Query", cql, e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#query(java.lang.String, org.springframework.data.cassandra.core.cql.RowCallbackHandler)
	 */
	@Override
	public void query(String cql, RowCallbackHandler rowCallbackHandler) throws DataAccessException {
		query(cql, newResultSetExtractor(rowCallbackHandler));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#query(java.lang.String, org.springframework.data.cassandra.core.cql.RowMapper)
	 */
	@Override
	public <T> List<T> query(String cql, RowMapper<T> rowMapper) throws DataAccessException {
		// noinspection ConstantConditions
		return query(cql, newResultSetExtractor(rowMapper));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#queryForList(java.lang.String)
	 */
	@Override
	public List<Map<String, Object>> queryForList(String cql) throws DataAccessException {
		// noinspection ConstantConditions
		return query(cql, newResultSetExtractor(newColumnMapRowMapper()));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#queryForList(java.lang.String, java.lang.Class)
	 */
	@Override
	public <T> List<T> queryForList(String cql, Class<T> elementType) throws DataAccessException {
		// noinspection ConstantConditions
		return query(cql, newResultSetExtractor(newSingleColumnRowMapper(elementType)));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#queryForMap(java.lang.String)
	 */
	@Override
	public Map<String, Object> queryForMap(String cql) throws DataAccessException {
		return queryForObject(cql, newColumnMapRowMapper());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#queryForObject(java.lang.String, java.lang.Class)
	 */
	@Override
	public <T> T queryForObject(String cql, Class<T> requiredType) throws DataAccessException {
		return queryForObject(cql, newSingleColumnRowMapper(requiredType));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#queryForObject(java.lang.String, org.springframework.data.cassandra.core.cql.RowMapper)
	 */
	@Override
	public <T> T queryForObject(String cql, RowMapper<T> rowMapper) throws DataAccessException {
		return DataAccessUtils.requiredSingleResult(query(cql, newResultSetExtractor(rowMapper)));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#queryForResultSet(java.lang.String)
	 */
	@Override
	public ResultSet queryForResultSet(String cql) throws DataAccessException {
		// noinspection ConstantConditions
		return query(cql, rs -> rs);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#queryForRows(java.lang.String)
	 */
	@Override
	public Iterable<Row> queryForRows(String cql) throws DataAccessException {
		return () -> queryForResultSet(cql).iterator();
	}

	// -------------------------------------------------------------------------
	// Methods dealing with com.datastax.driver.core.Statement
	// -------------------------------------------------------------------------

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#execute(com.datastax.driver.core.Statement)
	 */
	@Override
	public boolean execute(Statement statement) throws DataAccessException {

		Assert.notNull(statement, "CQL Statement must not be null");

		return queryForResultSet(statement).wasApplied();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#query(com.datastax.driver.core.Statement, org.springframework.data.cassandra.core.cql.ResultSetExtractor)
	 */
	@Override
	public <T> T query(Statement statement, ResultSetExtractor<T> resultSetExtractor) throws DataAccessException {

		Assert.notNull(statement, "CQL Statement must not be null");
		Assert.notNull(resultSetExtractor, "ResultSetExtractor must not be null");

		try {
			if (logger.isDebugEnabled()) {
				logger.debug("Executing CQL Statement [{}]", statement);
			}

			return resultSetExtractor.extractData(getCurrentSession().execute(applyStatementSettings(statement)));
		} catch (DriverException e) {
			throw translateException("Query", statement.toString(), e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#query(com.datastax.driver.core.Statement, org.springframework.data.cassandra.core.cql.RowCallbackHandler)
	 */
	@Override
	public void query(Statement statement, RowCallbackHandler rowCallbackHandler) throws DataAccessException {
		query(statement, newResultSetExtractor(rowCallbackHandler));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#query(com.datastax.driver.core.Statement, org.springframework.data.cassandra.core.cql.RowMapper)
	 */
	@Override
	public <T> List<T> query(Statement statement, RowMapper<T> rowMapper) throws DataAccessException {
		// noinspection ConstantConditions
		return query(statement, newResultSetExtractor(rowMapper));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#queryForList(com.datastax.driver.core.Statement)
	 */
	@Override
	public List<Map<String, Object>> queryForList(Statement statement) throws DataAccessException {
		// noinspection ConstantConditions
		return query(statement, newResultSetExtractor(newColumnMapRowMapper()));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#queryForList(com.datastax.driver.core.Statement, java.lang.Class)
	 */
	@Override
	public <T> List<T> queryForList(Statement statement, Class<T> elementType) throws DataAccessException {
		// noinspection ConstantConditions
		return query(statement, newResultSetExtractor(newSingleColumnRowMapper(elementType)));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#queryForMap(com.datastax.driver.core.Statement)
	 */
	@Override
	public Map<String, Object> queryForMap(Statement statement) throws DataAccessException {
		// noinspection ConstantConditions
		return queryForObject(statement, newColumnMapRowMapper());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#queryForObject(com.datastax.driver.core.Statement, java.lang.Class)
	 */
	@Override
	public <T> T queryForObject(Statement statement, Class<T> requiredType) throws DataAccessException {
		return queryForObject(statement, newSingleColumnRowMapper(requiredType));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#queryForObject(com.datastax.driver.core.Statement, org.springframework.data.cassandra.core.cql.RowMapper)
	 */
	@Override
	public <T> T queryForObject(Statement statement, RowMapper<T> rowMapper) throws DataAccessException {
		return DataAccessUtils.requiredSingleResult(query(statement, newResultSetExtractor(rowMapper)));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#queryForResultSet(com.datastax.driver.core.Statement)
	 */
	@Override
	public ResultSet queryForResultSet(Statement statement) throws DataAccessException {
		// noinspection ConstantConditions
		return query(statement, rs -> rs);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#queryForRows(com.datastax.driver.core.Statement)
	 */
	@Override
	public Iterable<Row> queryForRows(Statement statement) throws DataAccessException {
		return () -> queryForResultSet(statement).iterator();
	}

	// -------------------------------------------------------------------------
	// Methods dealing with com.datastax.driver.core.PreparedStatement
	// -------------------------------------------------------------------------

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#execute(java.lang.String, java.lang.Object[])
	 */
	@Override
	public boolean execute(String cql, Object... args) throws DataAccessException {
		return execute(cql, newPreparedStatementBinder(args));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#execute(java.lang.String, org.springframework.data.cassandra.core.cql.PreparedStatementBinder)
	 */
	@Override
	public boolean execute(String cql, @Nullable PreparedStatementBinder psb) throws DataAccessException {
		// noinspection ConstantConditions
		return query(new SimplePreparedStatementCreator(cql), psb, ResultSet::wasApplied);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#execute(java.lang.String, org.springframework.data.cassandra.core.cql.PreparedStatementCallback)
	 */
	@Nullable
	@Override
	public <T> T execute(String cql, PreparedStatementCallback<T> action) throws DataAccessException {
		return execute(newPreparedStatementCreator(cql), action);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#execute(org.springframework.data.cassandra.core.cql.PreparedStatementCreator)
	 */
	@Override
	public boolean execute(PreparedStatementCreator preparedStatementCreator) throws DataAccessException {
		// noinspection ConstantConditions
		return query(preparedStatementCreator, ResultSet::wasApplied);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#execute(org.springframework.data.cassandra.core.cql.PreparedStatementCreator, org.springframework.data.cassandra.core.cql.PreparedStatementCallback)
	 */
	@Override
	@Nullable
	public <T> T execute(PreparedStatementCreator preparedStatementCreator, PreparedStatementCallback<T> action)
			throws DataAccessException {

		Assert.notNull(preparedStatementCreator, "PreparedStatementCreator must not be null");
		Assert.notNull(action, "PreparedStatementCallback object must not be null");

		try {
			if (logger.isDebugEnabled()) {
				logger.debug("Preparing statement [{}] using {}", toCql(preparedStatementCreator), preparedStatementCreator);
			}

			Session session = getCurrentSession();

			return action.doInPreparedStatement(session,
					applyStatementSettings(preparedStatementCreator.createPreparedStatement(session)));

		} catch (DriverException e) {
			throw translateException("PreparedStatementCallback", toCql(preparedStatementCreator), e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#query(org.springframework.data.cassandra.core.cql.PreparedStatementCreator, org.springframework.data.cassandra.core.cql.ResultSetExtractor)
	 */
	@Override
	public <T> T query(PreparedStatementCreator preparedStatementCreator, ResultSetExtractor<T> resultSetExtractor)
			throws DataAccessException {

		return query(preparedStatementCreator, null, resultSetExtractor);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#query(org.springframework.data.cassandra.core.cql.PreparedStatementCreator, org.springframework.data.cassandra.core.cql.RowCallbackHandler)
	 */
	@Override
	public void query(PreparedStatementCreator preparedStatementCreator, RowCallbackHandler rowCallbackHandler)
			throws DataAccessException {

		query(preparedStatementCreator, null, newResultSetExtractor(rowCallbackHandler));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#query(org.springframework.data.cassandra.core.cql.PreparedStatementCreator, org.springframework.data.cassandra.core.cql.RowMapper)
	 */
	@Override
	public <T> List<T> query(PreparedStatementCreator preparedStatementCreator, RowMapper<T> rowMapper)
			throws DataAccessException {
		// noinspection ConstantConditions
		return query(preparedStatementCreator, null, newResultSetExtractor(rowMapper));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#query(org.springframework.data.cassandra.core.cql.PreparedStatementCreator, org.springframework.data.cassandra.core.cql.PreparedStatementBinder, org.springframework.data.cassandra.core.cql.ResultSetExtractor)
	 */
	@Nullable
	@Override
	public <T> T query(PreparedStatementCreator preparedStatementCreator, @Nullable PreparedStatementBinder psb,
			ResultSetExtractor<T> resultSetExtractor) throws DataAccessException {

		Assert.notNull(preparedStatementCreator, "PreparedStatementCreator must not be null");
		Assert.notNull(resultSetExtractor, "ResultSetExtractor object must not be null");

		try {
			if (logger.isDebugEnabled()) {
				logger.debug("Preparing statement [{}] using {}", toCql(preparedStatementCreator), preparedStatementCreator);
			}

			Session session = getCurrentSession();

			PreparedStatement preparedStatement = preparedStatementCreator.createPreparedStatement(session);

			if (logger.isDebugEnabled()) {
				logger.debug("Executing prepared statement [{}]", preparedStatement);
			}

			BoundStatement boundStatement = applyStatementSettings(
					psb != null ? psb.bindValues(preparedStatement) : preparedStatement.bind());

			ResultSet results = session.execute(boundStatement);

			return resultSetExtractor.extractData(results);

		} catch (DriverException e) {
			throw translateException("Query", toCql(preparedStatementCreator), e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#query(org.springframework.data.cassandra.core.cql.PreparedStatementCreator, org.springframework.data.cassandra.core.cql.PreparedStatementBinder, org.springframework.data.cassandra.core.cql.RowCallbackHandler)
	 */
	@Override
	public void query(PreparedStatementCreator preparedStatementCreator, @Nullable PreparedStatementBinder psb,
			RowCallbackHandler rowCallbackHandler) throws DataAccessException {

		query(preparedStatementCreator, psb, newResultSetExtractor(rowCallbackHandler));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#query(org.springframework.data.cassandra.core.cql.PreparedStatementCreator, org.springframework.data.cassandra.core.cql.PreparedStatementBinder, org.springframework.data.cassandra.core.cql.RowMapper)
	 */
	@Override
	public <T> List<T> query(PreparedStatementCreator preparedStatementCreator,
			@Nullable PreparedStatementBinder psb, RowMapper<T> rowMapper) throws DataAccessException {
		// noinspection ConstantConditions
		return query(preparedStatementCreator, psb, newResultSetExtractor(rowMapper));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#query(java.lang.String, org.springframework.data.cassandra.core.cql.ResultSetExtractor, java.lang.Object[])
	 */
	@Override
	public <T> T query(String cql, ResultSetExtractor<T> resultSetExtractor, Object... args) throws DataAccessException {

		return query(newPreparedStatementCreator(cql), newPreparedStatementBinder(args), resultSetExtractor);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#query(java.lang.String, org.springframework.data.cassandra.core.cql.RowCallbackHandler, java.lang.Object[])
	 */
	@Override
	public void query(String cql, RowCallbackHandler rowCallbackHandler, Object... args) throws DataAccessException {
		query(newPreparedStatementCreator(cql), newPreparedStatementBinder(args),
				newResultSetExtractor(rowCallbackHandler));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#query(java.lang.String, org.springframework.data.cassandra.core.cql.RowMapper, java.lang.Object[])
	 */
	@Override
	public <T> List<T> query(String cql, RowMapper<T> rowMapper, Object... args) throws DataAccessException {
		// noinspection ConstantConditions
		return query(newPreparedStatementCreator(cql), newPreparedStatementBinder(args), newResultSetExtractor(rowMapper));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#query(java.lang.String, org.springframework.data.cassandra.core.cql.PreparedStatementBinder, org.springframework.data.cassandra.core.cql.ResultSetExtractor)
	 */
	@Override
	public <T> T query(String cql, @Nullable PreparedStatementBinder psb,
			ResultSetExtractor<T> resultSetExtractor) throws DataAccessException {

		return query(newPreparedStatementCreator(cql), psb, resultSetExtractor);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#query(java.lang.String, org.springframework.data.cassandra.core.cql.PreparedStatementBinder, org.springframework.data.cassandra.core.cql.RowCallbackHandler)
	 */
	@Override
	public void query(String cql, @Nullable PreparedStatementBinder psb, RowCallbackHandler rowCallbackHandler)
			throws DataAccessException {
		query(newPreparedStatementCreator(cql), psb, newResultSetExtractor(rowCallbackHandler));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#query(java.lang.String, org.springframework.data.cassandra.core.cql.PreparedStatementBinder, org.springframework.data.cassandra.core.cql.RowMapper)
	 */
	@Override
	public <T> List<T> query(String cql, @Nullable PreparedStatementBinder psb, RowMapper<T> rowMapper)
			throws DataAccessException {
		// noinspection ConstantConditions
		return query(newPreparedStatementCreator(cql), psb, newResultSetExtractor(rowMapper));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#queryForList(java.lang.String, java.lang.Object[])
	 */
	@Override
	public List<Map<String, Object>> queryForList(String cql, Object... args) throws DataAccessException {
		// noinspection ConstantConditions
		return query(newPreparedStatementCreator(cql), newPreparedStatementBinder(args),
				newResultSetExtractor(newColumnMapRowMapper()));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#queryForList(java.lang.String, java.lang.Class, java.lang.Object[])
	 */
	@Override
	public <T> List<T> queryForList(String cql, Class<T> elementType, Object... args) throws DataAccessException {
		// noinspection ConstantConditions
		return query(newPreparedStatementCreator(cql), newPreparedStatementBinder(args),
				newResultSetExtractor(newSingleColumnRowMapper(elementType)));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#queryForMap(java.lang.String, java.lang.Object[])
	 */
	@Override
	public Map<String, Object> queryForMap(String cql, Object... args) throws DataAccessException {
		return queryForObject(cql, newColumnMapRowMapper(), args);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#queryForObject(java.lang.String, java.lang.Class, java.lang.Object[])
	 */
	@Override
	public <T> T queryForObject(String cql, Class<T> requiredType, Object... args) throws DataAccessException {
		return queryForObject(cql, newSingleColumnRowMapper(requiredType), args);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#queryForObject(java.lang.String, org.springframework.data.cassandra.core.cql.RowMapper, java.lang.Object[])
	 */
	@Override
	public <T> T queryForObject(String cql, RowMapper<T> rowMapper, Object... args) throws DataAccessException {
		return DataAccessUtils.requiredSingleResult(
				query(newPreparedStatementCreator(cql), newPreparedStatementBinder(args), newResultSetExtractor(rowMapper, 1)));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#queryForResultSet(java.lang.String, java.lang.Object[])
	 */
	@Override
	public ResultSet queryForResultSet(String cql, Object... args) throws DataAccessException {
		// noinspection ConstantConditions
		return query(newPreparedStatementCreator(cql), newPreparedStatementBinder(args), rs -> rs);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#queryForRows(java.lang.String, java.lang.Object[])
	 */
	@Override
	public Iterable<Row> queryForRows(String cql, Object... args) throws DataAccessException {
		return () -> queryForResultSet(cql, args).iterator();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#describeRing()
	 */
	@Override
	public List<RingMember> describeRing() throws DataAccessException {
		return (List<RingMember>) describeRing(RingMemberHostMapper.INSTANCE);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#describeRing(org.springframework.data.cassandra.core.cql.HostMapper)
	 */
	@Override
	public <T> Collection<T> describeRing(HostMapper<T> hostMapper) throws DataAccessException {

		Assert.notNull(hostMapper, "HostMapper must not be null");

		return hostMapper.mapHosts(getHosts());
	}

	/* (non-Javadoc) */
	private Set<Host> getHosts() {
		return getCurrentSession().getCluster().getMetadata().getAllHosts();
	}

	/* (non-Javadoc) */
	protected PreparedStatementCreator newPreparedStatementCreator(String cql) {
		return new SimplePreparedStatementCreator(cql);
	}

	// -------------------------------------------------------------------------
	// Implementation hooks and helper methods
	// -------------------------------------------------------------------------

	/**
	 * Translate the given {@link DriverException} into a generic {@link DataAccessException}.
	 *
	 * @param task readable text describing the task being attempted
	 * @param cql CQL query or update that caused the problem (may be {@literal null})
	 * @param driverException the offending {@code RuntimeException}.
	 * @return the exception translation {@link Function}
	 * @see CqlProvider
	 */
	protected DataAccessException translateException(String task, @Nullable String cql, DriverException driverException) {
		return translate(task, cql, driverException);
	}

	private Session getCurrentSession() {

		SessionFactory sessionFactory = getSessionFactory();

		Assert.state(sessionFactory != null, "SessionFactory is null");

		return sessionFactory.getSession();
	}

	private class SimplePreparedStatementCreator implements PreparedStatementCreator, CqlProvider {

		private final String cql;

		SimplePreparedStatementCreator(String cql) {

			Assert.notNull(cql, "CQL must not be null");

			this.cql = cql;
		}

		@Override
		public PreparedStatement createPreparedStatement(Session session) throws DriverException {
			return session.prepare(cql);
		}

		@Override
		public String getCql() {
			return cql;
		}
	}
}
