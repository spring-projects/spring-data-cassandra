/*
 * Copyright 2016-2020 the original author or authors.
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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.StreamSupport;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Node;

import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.DataAccessUtils;
import org.springframework.data.cassandra.SessionFactory;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * <b>This is the central class in the CQL core package.</b> It simplifies the use of CQL and helps to avoid common
 * errors. It executes core CQL workflow, leaving application code to provide CQL and extract results. This class
 * executes CQL queries or updates, initiating iteration over {@link ResultSet}s and catching {@link RuntimeException}
 * exceptions and translating them to the generic, more informative exception hierarchy defined in the
 * {@code org.springframework.dao} package.
 * <p>
 * Code using this class need only implement callback interfaces, giving them a clearly defined contract. The
 * {@link PreparedStatementCreator} callback interface creates a prepared statement given a Connection, providing CQL
 * and any necessary parameters. The {@link ResultSetExtractor} interface extracts values from a {@link ResultSet}. See
 * also {@link PreparedStatementBinder} and {@link RowMapper} for two popular alternative callback interfaces.
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
 * @author David Webb
 * @author Matthew Adams
 * @author Ryan Scheidter
 * @author Antoine Toulme
 * @author John Blum
 * @author Mark Paluch
 * @author Mike Barlotta
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
	 * Create a new {@link CqlTemplate} initialized with the given {@link CqlSession}.
	 *
	 * @param session the active Cassandra {@link CqlSession}.
	 * @throws IllegalStateException if {@link CqlSession} is {@literal null}.
	 */
	public CqlTemplate(CqlSession session) {

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
	// Methods dealing with a plain com.datastax.oss.driver.api.core.CqlSession
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

			Statement<?> statement = applyStatementSettings(newStatement(cql));

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
	// Methods dealing with com.datastax.oss.driver.api.core.cql.Statement
	// -------------------------------------------------------------------------

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#execute(com.datastax.oss.driver.api.core.cql.Statement)
	 */
	@Override
	public boolean execute(Statement<?> statement) throws DataAccessException {

		Assert.notNull(statement, "CQL Statement must not be null");

		return queryForResultSet(statement).wasApplied();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#query(com.datastax.oss.driver.api.core.cql.Statement, org.springframework.data.cassandra.core.cql.ResultSetExtractor)
	 */
	@Override
	public <T> T query(Statement<?> statement, ResultSetExtractor<T> resultSetExtractor) throws DataAccessException {

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
	 * @see org.springframework.data.cassandra.core.cqlOperations#query(com.datastax.oss.driver.api.core.cql.Statement, org.springframework.data.cassandra.core.cql.RowCallbackHandler)
	 */
	@Override
	public void query(Statement<?> statement, RowCallbackHandler rowCallbackHandler) throws DataAccessException {
		query(statement, newResultSetExtractor(rowCallbackHandler));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#query(com.datastax.oss.driver.api.core.cql.Statement, org.springframework.data.cassandra.core.cql.RowMapper)
	 */
	@Override
	public <T> List<T> query(Statement<?> statement, RowMapper<T> rowMapper) throws DataAccessException {
		// noinspection ConstantConditions
		return query(statement, newResultSetExtractor(rowMapper));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#queryForList(com.datastax.oss.driver.api.core.cql.Statement)
	 */
	@Override
	public List<Map<String, Object>> queryForList(Statement<?> statement) throws DataAccessException {
		// noinspection ConstantConditions
		return query(statement, newResultSetExtractor(newColumnMapRowMapper()));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#queryForList(com.datastax.oss.driver.api.core.cql.Statement, java.lang.Class)
	 */
	@Override
	public <T> List<T> queryForList(Statement<?> statement, Class<T> elementType) throws DataAccessException {
		// noinspection ConstantConditions
		return query(statement, newResultSetExtractor(newSingleColumnRowMapper(elementType)));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#queryForMap(com.datastax.oss.driver.api.core.cql.Statement)
	 */
	@Override
	public Map<String, Object> queryForMap(Statement<?> statement) throws DataAccessException {
		// noinspection ConstantConditions
		return queryForObject(statement, newColumnMapRowMapper());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#queryForObject(com.datastax.oss.driver.api.core.cql.Statement, java.lang.Class)
	 */
	@Override
	public <T> T queryForObject(Statement<?> statement, Class<T> requiredType) throws DataAccessException {
		return queryForObject(statement, newSingleColumnRowMapper(requiredType));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#queryForObject(com.datastax.oss.driver.api.core.cql.Statement, org.springframework.data.cassandra.core.cql.RowMapper)
	 */
	@Override
	public <T> T queryForObject(Statement<?> statement, RowMapper<T> rowMapper) throws DataAccessException {
		return DataAccessUtils.requiredSingleResult(query(statement, newResultSetExtractor(rowMapper)));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#queryForResultSet(com.datastax.oss.driver.api.core.cql.Statement)
	 */
	@Override
	public ResultSet queryForResultSet(Statement<?> statement) throws DataAccessException {
		// noinspection ConstantConditions
		return query(statement, rs -> rs);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlOperations#queryForRows(com.datastax.oss.driver.api.core.cql.Statement)
	 */
	@Override
	public Iterable<Row> queryForRows(Statement<?> statement) throws DataAccessException {
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
		return query(newPreparedStatementCreator(cql), psb, ResultSet::wasApplied);
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

			CqlSession session = getCurrentSession();

			return action.doInPreparedStatement(session, preparedStatementCreator.createPreparedStatement(session));

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

			CqlSession session = getCurrentSession();

			PreparedStatement preparedStatement = preparedStatementCreator.createPreparedStatement(session);

			if (logger.isDebugEnabled()) {
				logger.debug("Executing prepared statement [{}]", preparedStatement);
			}

			Statement<?> boundStatement = applyStatementSettings(
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
	public <T> List<T> query(PreparedStatementCreator preparedStatementCreator, @Nullable PreparedStatementBinder psb,
			RowMapper<T> rowMapper) throws DataAccessException {
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
	public <T> T query(String cql, @Nullable PreparedStatementBinder psb, ResultSetExtractor<T> resultSetExtractor)
			throws DataAccessException {

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
	private Collection<Node> getHosts() {
		return getCurrentSession().getMetadata().getNodes().values();
	}

	// -------------------------------------------------------------------------
	// Implementation hooks and helper methods
	// -------------------------------------------------------------------------

	/**
	 * Translate the given {@link RuntimeException} into a generic {@link DataAccessException}.
	 *
	 * @param task readable text describing the task being attempted
	 * @param cql CQL query or update that caused the problem (may be {@literal null})
	 * @param RuntimeException the offending {@code RuntimeException}.
	 * @return the exception translation {@link Function}
	 * @see CqlProvider
	 */
	protected DataAccessException translateException(String task, @Nullable String cql,
			RuntimeException RuntimeException) {
		return translate(task, cql, RuntimeException);
	}

	/**
	 * Create a new CQL-based {@link PreparedStatementCreator} using the CQL passed in. By default, we'll create an
	 * {@link SimplePreparedStatementCreator}. This method allows for the creation to be overridden by subclasses.
	 *
	 * @param cql static CQL to execute, must not be empty or {@literal null}.
	 * @return the new {@link PreparedStatementCreator} to use
	 */
	protected PreparedStatementCreator newPreparedStatementCreator(String cql) {
		return new SimplePreparedStatementCreator(cql);
	}

	/**
	 * Constructs a new instance of the {@link ResultSetExtractor} initialized with and adapting the given
	 * {@link RowCallbackHandler}.
	 *
	 * @param rowCallbackHandler {@link RowCallbackHandler} to adapt as a {@link ResultSetExtractor}.
	 * @return a {@link ResultSetExtractor} implementation adapting an instance of the {@link RowCallbackHandler}.
	 * @see AsyncCqlTemplate.AsyncRowCallbackHandlerResultSetExtractor
	 * @see ResultSetExtractor
	 * @see RowCallbackHandler
	 */
	protected RowCallbackHandlerResultSetExtractor newResultSetExtractor(RowCallbackHandler rowCallbackHandler) {
		return new RowCallbackHandlerResultSetExtractor(rowCallbackHandler);
	}

	/**
	 * Constructs a new instance of the {@link ResultSetExtractor} initialized with and adapting the given
	 * {@link RowMapper}.
	 *
	 * @param rowMapper {@link RowMapper} to adapt as a {@link ResultSetExtractor}.
	 * @return a {@link ResultSetExtractor} implementation adapting an instance of the {@link RowMapper}.
	 * @see ResultSetExtractor
	 * @see RowMapper
	 * @see RowMapperResultSetExtractor
	 */
	protected <T> RowMapperResultSetExtractor<T> newResultSetExtractor(RowMapper<T> rowMapper) {
		return new RowMapperResultSetExtractor<>(rowMapper);
	}

	/**
	 * Constructs a new instance of the {@link ResultSetExtractor} initialized with and adapting the given
	 * {@link RowMapper}.
	 *
	 * @param rowMapper {@link RowMapper} to adapt as a {@link ResultSetExtractor}.
	 * @param rowsExpected number of expected rows in the {@link ResultSet}.
	 * @return a {@link ResultSetExtractor} implementation adapting an instance of the {@link RowMapper}.
	 * @see ResultSetExtractor
	 * @see RowMapper
	 * @see RowMapperResultSetExtractor
	 */
	protected <T> RowMapperResultSetExtractor<T> newResultSetExtractor(RowMapper<T> rowMapper, int rowsExpected) {
		return new RowMapperResultSetExtractor<>(rowMapper, rowsExpected);
	}

	private CqlSession getCurrentSession() {

		SessionFactory sessionFactory = getSessionFactory();

		Assert.state(sessionFactory != null, "SessionFactory is null");

		return sessionFactory.getSession();
	}

	/**
	 * Adapter to enable use of a {@link RowCallbackHandler} inside a {@link ResultSetExtractor}.
	 */
	protected static class RowCallbackHandlerResultSetExtractor implements ResultSetExtractor<Object> {

		private final RowCallbackHandler rowCallbackHandler;

		protected RowCallbackHandlerResultSetExtractor(RowCallbackHandler rowCallbackHandler) {
			this.rowCallbackHandler = rowCallbackHandler;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.ResultSetExtractor#extractData(com.datastax.driver.core.ResultSet)
		 */
		@Override
		@Nullable
		public Object extractData(ResultSet resultSet) {

			StreamSupport.stream(resultSet.spliterator(), false).forEach(rowCallbackHandler::processRow);

			return null;
		}
	}

}
