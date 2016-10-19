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

import static org.springframework.cassandra.core.cql.CqlIdentifier.cqlId;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.StreamSupport;

import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.Update;
import org.springframework.cassandra.support.CassandraAccessor;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.DataAccessUtils;
import org.springframework.util.Assert;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.querybuilder.QueryBuilder;

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
 * "org.springframework.cassandra.core.CqlTemplate" as log category.
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
	private RetryPolicy retryPolicy;

	/**
	 * If this variable is set to a value, it will be used for setting the {@code consistencyLevel} property on statements
	 * used for query processing.
	 */
	private com.datastax.driver.core.ConsistencyLevel consistencyLevel;

	/**
	 * Construct a new {@link CqlTemplate}. Note: The {@link Session} has to be set before using the instance.
	 *
	 * @see #setSession(Session)
	 */
	public CqlTemplate() {}

	/**
	 * Construct a new {@link CqlTemplate}, given a {@link Session}.
	 *
	 * @param session the active Cassandra {@link Session}.
	 */
	public CqlTemplate(Session session) {

		Assert.notNull(session, "Session must not be null");

		setSession(session);
	}

	/**
	 * Set the fetch size for this {@link CqlTemplate}. This is important for processing large result sets: Setting this
	 * higher than the default value will increase processing speed at the cost of memory consumption; setting this lower
	 * can avoid transferring row data that will never be read by the application. Default is -1, indicating to use the
	 * CQL driver's default configuration (i.e. to not pass a specific fetch size setting on to the driver).
	 *
	 * @see Statement#setFetchSize(int)
	 */
	public void setFetchSize(int fetchSize) {
		this.fetchSize = fetchSize;
	}

	/**
	 * @return the fetch size specified for this {@link CqlTemplate}.
	 */
	public int getFetchSize() {
		return this.fetchSize;
	}

	/**
	 * Set the retry policy for this {@link CqlTemplate}. This is important for defining behavior when a request
	 * fails.
	 *
	 * @see Statement#setRetryPolicy(RetryPolicy)
	 * @see RetryPolicy
	 */
	public void setRetryPolicy(RetryPolicy retryPolicy) {
		this.retryPolicy = retryPolicy;
	}

	/**
	 * @return the {@link RetryPolicy} specified for this {@link CqlTemplate}.
	 */
	public RetryPolicy getRetryPolicy() {
		return retryPolicy;
	}

	/**
	 * Set the consistency level for this {@link CqlTemplate}. Consistency level defines the number of nodes
	 * involved into query processing. Relaxed consistency level settings use fewer nodes but eventual consistency is more
	 * likely to occur while a higher consistency level involves more nodes to obtain results with a higher consistency
	 * guarantee.
	 *
	 * @see Statement#setConsistencyLevel(ConsistencyLevel)
	 * @see RetryPolicy
	 */
	public void setConsistencyLevel(ConsistencyLevel consistencyLevel) {
		this.consistencyLevel = consistencyLevel;
	}

	/**
	 * @return the {@link ConsistencyLevel} specified for this {@link CqlTemplate}.
	 */
	public ConsistencyLevel getConsistencyLevel() {
		return consistencyLevel;
	}

	// -------------------------------------------------------------------------
	// Methods dealing with a plain com.datastax.driver.core.Session
	// -------------------------------------------------------------------------

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.CqlOperationsNG#execute(org.springframework.cassandra.core.SessionCallback)
	 */
	@Override
	public <T> T execute(SessionCallback<T> action) throws DataAccessException {

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
	 * @see org.springframework.cassandra.core.CqlOperationsNG#execute(java.lang.String)
	 */
	@Override
	public boolean execute(String cql) throws DataAccessException {

		Assert.hasText(cql, "CQL must not be empty");

		return queryForResultSet(cql).wasApplied();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.CqlOperationsNG#query(java.lang.String, org.springframework.cassandra.core.ResultSetExtractor)
	 */
	@Override
	public <T> T query(String cql, ResultSetExtractor<T> rse) throws DataAccessException {

		Assert.hasText(cql, "CQL must not be empty");
		Assert.notNull(rse, "ResultSetExtractor must not be null");

		try {

			if (logger.isDebugEnabled()) {
				logger.debug("Executing CQL Statement [{}]", cql);
			}

			SimpleStatement simpleStatement = new SimpleStatement(cql);

			applyStatementSettings(simpleStatement);

			return rse.extractData(getSession().execute(simpleStatement));
		} catch (DriverException e) {
			throw translateException("Query", cql, e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.CqlOperationsNG#query(java.lang.String, org.springframework.cassandra.core.RowCallbackHandler)
	 */
	@Override
	public void query(String cql, RowCallbackHandler rch) throws DataAccessException {
		query(cql, new RowCallbackHandlerResultSetExtractor(rch));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.CqlOperationsNG#query(java.lang.String, org.springframework.cassandra.core.RowMapper)
	 */
	@Override
	public <T> List<T> query(String cql, RowMapper<T> rowMapper) throws DataAccessException {
		return query(cql, new RowMapperResultSetExtractor<>(rowMapper));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.CqlOperationsNG#queryForObject(java.lang.String, org.springframework.cassandra.core.RowMapper)
	 */
	@Override
	public <T> T queryForObject(String cql, RowMapper<T> rowMapper) throws DataAccessException {

		List<T> results = query(cql, rowMapper);
		return DataAccessUtils.requiredSingleResult(results);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.CqlOperationsNG#queryForObject(java.lang.String, java.lang.Class)
	 */
	@Override
	public <T> T queryForObject(String cql, Class<T> requiredType) throws DataAccessException {
		return queryForObject(cql, getSingleColumnRowMapper(requiredType));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.CqlOperationsNG#queryForMap(java.lang.String)
	 */
	@Override
	public Map<String, Object> queryForMap(String cql) throws DataAccessException {
		return queryForObject(cql, getColumnMapRowMapper());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.CqlOperationsNG#queryForList(java.lang.String, java.lang.Class)
	 */
	@Override
	public <T> List<T> queryForList(String cql, Class<T> elementType) throws DataAccessException {
		return query(cql, getSingleColumnRowMapper(elementType));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.CqlOperationsNG#queryForList(java.lang.String)
	 */
	@Override
	public List<Map<String, Object>> queryForList(String cql) throws DataAccessException {
		return query(cql, getColumnMapRowMapper());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.CqlOperationsNG#queryForResultSet(java.lang.String)
	 */
	@Override
	public ResultSet queryForResultSet(String cql) throws DataAccessException {
		return query(cql, rs -> rs);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.CqlOperationsNG#queryForRows(java.lang.String)
	 */
	@Override
	public Iterator<Row> queryForRows(String cql) throws DataAccessException {
		return queryForResultSet(cql).iterator();
	}

	// -------------------------------------------------------------------------
	// Methods dealing with com.datastax.driver.core.Statement
	// -------------------------------------------------------------------------

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.CqlOperationsNG#execute(com.datastax.driver.core.Statement)
	 */
	@Override
	public boolean execute(Statement statement) throws DataAccessException {

		Assert.notNull(statement, "CQL Statement must not be null");

		return queryForResultSet(statement).wasApplied();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.CqlOperationsNG#query(com.datastax.driver.core.Statement, org.springframework.cassandra.core.ResultSetExtractor)
	 */
	@Override
	public <T> T query(Statement statement, ResultSetExtractor<T> rse) throws DataAccessException {

		Assert.notNull(statement, "CQL Statement must not be null");
		Assert.notNull(rse, "ResultSetExtractor must not be null");

		try {

			if (logger.isDebugEnabled()) {
				logger.debug("Executing CQL Statement [{}]", statement);
			}

			applyStatementSettings(statement);

			return rse.extractData(getSession().execute(statement));
		} catch (DriverException e) {
			throw translateException("Query", statement.toString(), e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.CqlOperationsNG#query(com.datastax.driver.core.Statement, org.springframework.cassandra.core.RowCallbackHandler)
	 */
	@Override
	public void query(Statement statement, RowCallbackHandler rch) throws DataAccessException {
		query(statement, new RowCallbackHandlerResultSetExtractor(rch));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.CqlOperationsNG#query(com.datastax.driver.core.Statement, org.springframework.cassandra.core.RowMapper)
	 */
	@Override
	public <T> List<T> query(Statement statement, RowMapper<T> rowMapper) throws DataAccessException {
		return query(statement, new RowMapperResultSetExtractor<>(rowMapper));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.CqlOperationsNG#queryForObject(com.datastax.driver.core.Statement, org.springframework.cassandra.core.RowMapper)
	 */
	@Override
	public <T> T queryForObject(Statement statement, RowMapper<T> rowMapper) throws DataAccessException {

		List<T> results = query(statement, rowMapper);
		return DataAccessUtils.requiredSingleResult(results);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.CqlOperationsNG#queryForObject(com.datastax.driver.core.Statement, java.lang.Class)
	 */
	@Override
	public <T> T queryForObject(Statement statement, Class<T> requiredType) throws DataAccessException {
		return queryForObject(statement, getSingleColumnRowMapper(requiredType));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.CqlOperationsNG#queryForMap(com.datastax.driver.core.Statement)
	 */
	@Override
	public Map<String, Object> queryForMap(Statement statement) throws DataAccessException {
		return queryForObject(statement, getColumnMapRowMapper());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.CqlOperationsNG#queryForList(com.datastax.driver.core.Statement, java.lang.Class)
	 */
	@Override
	public <T> List<T> queryForList(Statement statement, Class<T> elementType) throws DataAccessException {
		return query(statement, getSingleColumnRowMapper(elementType));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.CqlOperationsNG#queryForList(com.datastax.driver.core.Statement)
	 */
	@Override
	public List<Map<String, Object>> queryForList(Statement statement) throws DataAccessException {
		return query(statement, getColumnMapRowMapper());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.CqlOperationsNG#queryForResultSet(com.datastax.driver.core.Statement)
	 */
	@Override
	public ResultSet queryForResultSet(Statement statement) throws DataAccessException {
		return query(statement, rs -> rs);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.CqlOperationsNG#queryForRows(com.datastax.driver.core.Statement)
	 */
	@Override
	public Iterator<Row> queryForRows(Statement statement) throws DataAccessException {
		return queryForResultSet(statement).iterator();
	}

	// -------------------------------------------------------------------------
	// Methods dealing with prepared statements
	// -------------------------------------------------------------------------

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.CqlOperationsNG#execute(org.springframework.cassandra.core.PreparedStatementCreator, org.springframework.cassandra.core.PreparedStatementCallback)
	 */
	@Override
	public <T> T execute(PreparedStatementCreator psc, PreparedStatementCallback<T> action) throws DataAccessException {

		Assert.notNull(psc, "PreparedStatementCreator must not be null");
		Assert.notNull(action, "PreparedStatementCallback object must not be null");

		try {

			if (logger.isDebugEnabled()) {
				logger.debug("Preparing statement [{}] using {}", getCql(psc), psc);
			}

			PreparedStatement preparedStatement = psc.createPreparedStatement(getSession());
			applyStatementSettings(preparedStatement);

			return action.doInPreparedStatement(preparedStatement);

		} catch (DriverException e) {
			throw translateException("PreparedStatementCallback", getCql(psc), e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.CqlOperationsNG#query(org.springframework.cassandra.core.PreparedStatementCreator, org.springframework.cassandra.core.PreparedStatementBinder, org.springframework.cassandra.core.ResultSetExtractor)
	 */
	@Override
	public <T> T query(PreparedStatementCreator psc, PreparedStatementBinder psb, ResultSetExtractor<T> rse)
			throws DataAccessException {

		Assert.notNull(psc, "PreparedStatementCreator must not be null");
		Assert.notNull(rse, "ResultSetExtractor object must not be null");

		try {

			if (logger.isDebugEnabled()) {
				logger.debug("Preparing statement [{}] using {}", getCql(psc), psc);
			}

			Session session = getSession();
			PreparedStatement ps = psc.createPreparedStatement(session);

			if (logger.isDebugEnabled()) {
				logger.debug("Executing prepared statement [{}]", ps);
			}

			BoundStatement boundStatement = psb != null ? psb.bindValues(ps) : ps.bind();

			applyStatementSettings(boundStatement);
			return rse.extractData(session.execute(boundStatement));

		} catch (DriverException e) {
			throw translateException("Query", getCql(psc), e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.CqlOperationsNG#execute(java.lang.String, org.springframework.cassandra.core.PreparedStatementCallback)
	 */
	@Override
	public <T> T execute(String cql, PreparedStatementCallback<T> action) throws DataAccessException {
		return execute(new SimplePreparedStatementCreator(cql), action);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.CqlOperationsNG#query(org.springframework.cassandra.core.PreparedStatementCreator, org.springframework.cassandra.core.ResultSetExtractor)
	 */
	@Override
	public <T> T query(PreparedStatementCreator psc, ResultSetExtractor<T> rse) throws DataAccessException {
		return query(psc, null, rse);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.CqlOperationsNG#query(java.lang.String, org.springframework.cassandra.core.PreparedStatementBinder, org.springframework.cassandra.core.ResultSetExtractor)
	 */
	@Override
	public <T> T query(String cql, PreparedStatementBinder psb, ResultSetExtractor<T> rse) throws DataAccessException {
		return query(new SimplePreparedStatementCreator(cql), psb, rse);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.CqlOperationsNG#query(java.lang.String, org.springframework.cassandra.core.ResultSetExtractor, java.lang.Object[])
	 */
	@Override
	public <T> T query(String cql, ResultSetExtractor<T> rse, Object... args) throws DataAccessException {
		return query(cql, newArgPreparedStatementBinder(args), rse);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.CqlOperationsNG#query(org.springframework.cassandra.core.PreparedStatementCreator, org.springframework.cassandra.core.RowCallbackHandler)
	 */
	@Override
	public void query(PreparedStatementCreator psc, RowCallbackHandler rch) throws DataAccessException {
		query(psc, new RowCallbackHandlerResultSetExtractor(rch));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.CqlOperationsNG#query(java.lang.String, org.springframework.cassandra.core.PreparedStatementBinder, org.springframework.cassandra.core.RowCallbackHandler)
	 */
	@Override
	public void query(String cql, PreparedStatementBinder psb, RowCallbackHandler rch) throws DataAccessException {
		query(cql, psb, new RowCallbackHandlerResultSetExtractor(rch));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.CqlOperationsNG#query(org.springframework.cassandra.core.PreparedStatementCreator, org.springframework.cassandra.core.PreparedStatementBinder, org.springframework.cassandra.core.RowCallbackHandler)
	 */
	@Override
	public void query(PreparedStatementCreator psc, PreparedStatementBinder psb, RowCallbackHandler rch)
			throws DataAccessException {
		query(psc, psb, new RowCallbackHandlerResultSetExtractor(rch));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.CqlOperationsNG#query(java.lang.String, org.springframework.cassandra.core.RowCallbackHandler, java.lang.Object[])
	 */
	@Override
	public void query(String cql, RowCallbackHandler rch, Object... args) throws DataAccessException {
		query(cql, newArgPreparedStatementBinder(args), new RowCallbackHandlerResultSetExtractor(rch));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.CqlOperationsNG#query(org.springframework.cassandra.core.PreparedStatementCreator, org.springframework.cassandra.core.RowMapper)
	 */
	@Override
	public <T> List<T> query(PreparedStatementCreator psc, RowMapper<T> rowMapper) throws DataAccessException {
		return query(psc, new RowMapperResultSetExtractor<>(rowMapper));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.CqlOperationsNG#query(java.lang.String, org.springframework.cassandra.core.PreparedStatementBinder, org.springframework.cassandra.core.RowMapper)
	 */
	@Override
	public <T> List<T> query(String cql, PreparedStatementBinder psb, RowMapper<T> rowMapper) throws DataAccessException {
		return query(cql, psb, new RowMapperResultSetExtractor<>(rowMapper));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.CqlOperationsNG#query(org.springframework.cassandra.core.PreparedStatementCreator, org.springframework.cassandra.core.PreparedStatementBinder, org.springframework.cassandra.core.RowMapper)
	 */
	@Override
	public <T> List<T> query(PreparedStatementCreator psc, PreparedStatementBinder psb, RowMapper<T> rowMapper)
			throws DataAccessException {
		return query(psc, psb, new RowMapperResultSetExtractor<>(rowMapper));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.CqlOperationsNG#query(java.lang.String, org.springframework.cassandra.core.RowMapper, java.lang.Object[])
	 */
	@Override
	public <T> List<T> query(String cql, RowMapper<T> rowMapper, Object... args) throws DataAccessException {
		return query(cql, newArgPreparedStatementBinder(args), new RowMapperResultSetExtractor<>(rowMapper));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.CqlOperationsNG#queryForObject(java.lang.String, org.springframework.cassandra.core.RowMapper, java.lang.Object[])
	 */
	@Override
	public <T> T queryForObject(String cql, RowMapper<T> rowMapper, Object... args) throws DataAccessException {

		List<T> results = query(cql, newArgPreparedStatementBinder(args), new RowMapperResultSetExtractor<>(rowMapper, 1));
		return DataAccessUtils.requiredSingleResult(results);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.CqlOperationsNG#queryForObject(java.lang.String, java.lang.Class, java.lang.Object[])
	 */
	@Override
	public <T> T queryForObject(String cql, Class<T> requiredType, Object... args) throws DataAccessException {
		return queryForObject(cql, getSingleColumnRowMapper(requiredType), args);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.CqlOperationsNG#queryForMap(java.lang.String, java.lang.Object[])
	 */
	@Override
	public Map<String, Object> queryForMap(String cql, Object... args) throws DataAccessException {
		return queryForObject(cql, getColumnMapRowMapper(), args);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.CqlOperationsNG#queryForList(java.lang.String, java.lang.Class, java.lang.Object[])
	 */
	@Override
	public <T> List<T> queryForList(String cql, Class<T> elementType, Object... args) throws DataAccessException {
		return query(cql, getSingleColumnRowMapper(elementType), args);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.CqlOperationsNG#queryForList(java.lang.String, java.lang.Object[])
	 */
	@Override
	public List<Map<String, Object>> queryForList(String cql, Object... args) throws DataAccessException {
		return query(cql, getColumnMapRowMapper(), args);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.CqlOperationsNG#queryForResultSet(java.lang.String, java.lang.Object[])
	 */
	@Override
	public ResultSet queryForResultSet(String cql, Object... args) throws DataAccessException {
		return query(cql, rs -> rs, args);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.CqlOperationsNG#queryForRows(java.lang.String, java.lang.Object[])
	 */
	@Override
	public Iterator<Row> queryForRows(String cql, Object... args) throws DataAccessException {
		return queryForResultSet(cql, args).iterator();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.CqlOperationsNG#execute(org.springframework.cassandra.core.PreparedStatementCreator)
	 */
	@Override
	public boolean execute(PreparedStatementCreator psc) throws DataAccessException {
		return query(psc, ResultSet::wasApplied);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.CqlOperationsNG#execute(java.lang.String, org.springframework.cassandra.core.PreparedStatementBinder)
	 */
	@Override
	public boolean execute(String cql, PreparedStatementBinder psb) throws DataAccessException {
		return query(new SimplePreparedStatementCreator(cql), psb, ResultSet::wasApplied);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.CqlOperationsNG#execute(java.lang.String, java.lang.Object[])
	 */
	@Override
	public boolean execute(String cql, Object... args) throws DataAccessException {
		return execute(cql, newArgPreparedStatementBinder(args));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.CqlOperationsNG#describeRing()
	 */
	@Override
	public List<RingMember> describeRing() throws DataAccessException {
		return (List<RingMember>) describeRing(RingMemberHostMapper.INSTANCE);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.CqlOperationsNG#describeRing(org.springframework.cassandra.core.HostMapper)
	 */
	@Override
	public <T> Collection<T> describeRing(HostMapper<T> hostMapper) throws DataAccessException {

		Assert.notNull(hostMapper, "HostMapper must not be null");

		return hostMapper.mapHosts(getHosts());
	}

	private Set<Host> getHosts() {
		return getSession().getCluster().getMetadata().getAllHosts();
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
	 * @return the exception translation {@link Function}
	 * @see CqlProvider
	 */
	@SuppressWarnings("ThrowableResultOfMethodCallIgnored")
	protected DataAccessException translateException(String task, String cql, DriverException driverException) {
		return translate(task, cql, driverException);
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
