/*
 * Copyright 2013-2018 the original author or authors.
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

import java.util.Map;
import java.util.Optional;
import java.util.stream.StreamSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataAccessException;
import org.springframework.data.cassandra.SessionFactory;
import org.springframework.data.cassandra.core.cql.session.DefaultSessionFactory;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.querybuilder.QueryBuilder;

/**
 * {@link CassandraAccessor} provides access to a Cassandra {@link SessionFactory} and the
 * {@link CassandraExceptionTranslator}.
 * <p>
 * Classes providing a higher abstraction level usually extend {@link CassandraAccessor} to provide a richer set of
 * functionality on top of a {@link SessionFactory} using {@link Session}.
 *
 * @author David Webb
 * @author Mark Paluch
 * @author John Blum
 * @see org.springframework.beans.factory.InitializingBean
 * @see com.datastax.driver.core.Session
 */
public class CassandraAccessor implements InitializingBean {

	/**
	 * Placeholder for default values.
	 */
	private static final Statement DEFAULTS = QueryBuilder.select().from("DEFAULT");

	/** Logger available to subclasses */
	protected final Logger logger = LoggerFactory.getLogger(getClass());

	private CqlExceptionTranslator exceptionTranslator = new CassandraExceptionTranslator();

	/**
	 * If this variable is set to a non-negative value, it will be used for setting the {@code fetchSize} property on
	 * statements used for query processing.
	 */
	private int fetchSize = -1;

	/**
	 * If this variable is set to a value, it will be used for setting the {@code consistencyLevel} property on statements
	 * used for query processing.
	 */
	private @Nullable com.datastax.driver.core.ConsistencyLevel consistencyLevel;

	/**
	 * If this variable is set to a value, it will be used for setting the {@code retryPolicy} property on statements used
	 * for query processing.
	 */
	private @Nullable com.datastax.driver.core.policies.RetryPolicy retryPolicy;

	private @Nullable SessionFactory sessionFactory;

	/**
	 * Ensures the Cassandra {@link Session} and exception translator has been propertly set.
	 */
	@Override
	public void afterPropertiesSet() {
		Assert.state(sessionFactory != null, "SessionFactory must not be null");
	}

	/**
	 * Set the consistency level for this template. Consistency level defines the number of nodes involved into query
	 * processing. Relaxed consistency level settings use fewer nodes but eventual consistency is more likely to occur
	 * while a higher consistency level involves more nodes to obtain results with a higher consistency guarantee.
	 *
	 * @see Statement#setConsistencyLevel(ConsistencyLevel)
	 * @see RetryPolicy
	 */
	public void setConsistencyLevel(@Nullable ConsistencyLevel consistencyLevel) {
		this.consistencyLevel = consistencyLevel;
	}

	/**
	 * @return the {@link ConsistencyLevel} specified for this template.
	 */
	@Nullable
	public ConsistencyLevel getConsistencyLevel() {
		return this.consistencyLevel;
	}

	/**
	 * Sets the exception translator used by this template to translate Cassandra specific Exceptions into Spring DAO's
	 * Exception Hierarchy.
	 *
	 * @param exceptionTranslator exception translator to set; must not be {@literal null}.
	 * @see CqlExceptionTranslator
	 */
	public void setExceptionTranslator(CqlExceptionTranslator exceptionTranslator) {

		Assert.notNull(exceptionTranslator, "CQLExceptionTranslator must not be null");

		this.exceptionTranslator = exceptionTranslator;
	}

	/**
	 * Return the exception translator used by this template to translate Cassandra specific Exceptions into Spring DAO's
	 * Exception Hierarchy.
	 *
	 * @return the Cassandra exception translator.
	 * @see CqlExceptionTranslator
	 */
	public CqlExceptionTranslator getExceptionTranslator() {
		return this.exceptionTranslator;
	}

	/**
	 * Set the fetch size for this template. This is important for processing large result sets: Setting this higher than
	 * the default value will increase processing speed at the cost of memory consumption; setting this lower can avoid
	 * transferring row data that will never be read by the application. Default is -1, indicating to use the CQL driver's
	 * default configuration (i.e. to not pass a specific fetch size setting on to the driver).
	 *
	 * @see Statement#setFetchSize(int)
	 */
	public void setFetchSize(int fetchSize) {
		this.fetchSize = fetchSize;
	}

	/**
	 * @return the fetch size specified for this template.
	 */
	public int getFetchSize() {
		return this.fetchSize;
	}

	/**
	 * Set the retry policy for this template. This is important for defining behavior when a request fails.
	 *
	 * @see Statement#setRetryPolicy(RetryPolicy)
	 * @see RetryPolicy
	 */
	public void setRetryPolicy(@Nullable RetryPolicy retryPolicy) {
		this.retryPolicy = retryPolicy;
	}

	/**
	 * @return the {@link RetryPolicy} specified for this template.
	 */
	@Nullable
	public RetryPolicy getRetryPolicy() {
		return this.retryPolicy;
	}

	/**
	 * Sets the Cassandra {@link Session} used by this template to perform Cassandra data access operations. The
	 * {@code session} will replace the current {@link #getSessionFactory()} with {@link DefaultSessionFactory}.
	 *
	 * @param session Cassandra {@link Session} used by this template, must not be{@literal null}.
	 * @see com.datastax.driver.core.Session
	 * @see DefaultSessionFactory
	 */
	public void setSession(Session session) {

		Assert.notNull(session, "Session must not be null");

		setSessionFactory(new DefaultSessionFactory(session));
	}

	/**
	 * Returns the Cassandra {@link Session} from {@link SessionFactory} used by this template to perform Cassandra data
	 * access operations.
	 *
	 * @return the Cassandra {@link Session} used by this template.
	 * @see com.datastax.driver.core.Session
	 * @deprecated since 2.0. This class uses a {@link SessionFactory} to dispatch CQL calls amongst different
	 *             {@link Session}s during its lifecycle.
	 */
	@Deprecated
	public Session getSession() {

		Assert.state(getSessionFactory() != null, "SessionFactory was not properly initialized");

		return getSessionFactory().getSession();
	}

	/**
	 * Sets the Cassandra {@link SessionFactory} used by this template to perform Cassandra data access operations.
	 *
	 * @param sessionFactory Cassandra {@link Session} used by this template. Must not be{@literal null}.
	 * @since 2.0
	 * @see com.datastax.driver.core.Session
	 */
	public void setSessionFactory(SessionFactory sessionFactory) {

		Assert.notNull(sessionFactory, "SessionFactory must not be null");

		this.sessionFactory = sessionFactory;
	}

	/**
	 * Returns the Cassandra {@link SessionFactory} used by this template to perform Cassandra data access operations.
	 *
	 * @return the Cassandra {@link SessionFactory} used by this template.
	 * @since 2.0
	 * @see SessionFactory
	 */
	@Nullable
	public SessionFactory getSessionFactory() {
		return this.sessionFactory;
	}

	/**
	 * Prepare the given CQL Statement (or {@link com.datastax.driver.core.PreparedStatement}), applying statement
	 * settings such as retry policy and consistency level.
	 *
	 * @param statement the CQL Statement to prepare
	 * @see #setRetryPolicy(RetryPolicy)
	 * @see #setConsistencyLevel(ConsistencyLevel)
	 */
	protected <T extends PreparedStatement> T applyStatementSettings(T statement) {

		ConsistencyLevel consistencyLevel = getConsistencyLevel();

		if (consistencyLevel != null) {
			statement.setConsistencyLevel(consistencyLevel);
		}

		RetryPolicy retryPolicy = getRetryPolicy();

		if (retryPolicy != null) {
			statement.setRetryPolicy(retryPolicy);
		}

		return statement;
	}

	/**
	 * Prepare the given CQL Statement (or {@link com.datastax.driver.core.PreparedStatement}), applying statement
	 * settings such as fetch size, retry policy, and consistency level.
	 *
	 * @param statement the CQL Statement to prepare
	 * @see #setFetchSize(int)
	 * @see #setRetryPolicy(RetryPolicy)
	 * @see #setConsistencyLevel(ConsistencyLevel)
	 */
	protected <T extends Statement> T applyStatementSettings(T statement) {

		ConsistencyLevel consistencyLevel = getConsistencyLevel();

		if (consistencyLevel != null && statement.getConsistencyLevel() == DEFAULTS.getConsistencyLevel()) {
			statement.setConsistencyLevel(consistencyLevel);
		}

		int fetchSize = getFetchSize();

		if (fetchSize != -1 && statement.getFetchSize() == DEFAULTS.getFetchSize()) {
			statement.setFetchSize(fetchSize);
		}

		RetryPolicy retryPolicy = getRetryPolicy();

		if (retryPolicy != null && statement.getRetryPolicy() == DEFAULTS.getRetryPolicy()) {
			statement.setRetryPolicy(retryPolicy);
		}

		return statement;
	}

	/**
	 * Translate the given {@link DriverException} into a generic {@link DataAccessException}.
	 * <p>
	 * The returned {@link DataAccessException} is supposed to contain the original {@code DriverException} as root cause.
	 * However, client code may not generally rely on this due to {@link DataAccessException}s possibly being caused by
	 * other resource APIs as well. That said, a {@code getRootCause() instanceof DataAccessException} check (and
	 * subsequent cast) is considered reliable when expecting Cassandra-based access to have happened.
	 *
	 * @param ex the offending {@link DriverException}
	 * @return the DataAccessException, wrapping the {@code DriverException}
	 * @see <a href=
	 *      "http://docs.spring.io/spring/docs/current/spring-framework-reference/data-access.html#dao-exceptions">Consistent
	 *      exception hierarchy</a>
	 * @see DataAccessException
	 */
	@Nullable
	protected DataAccessException translateExceptionIfPossible(DriverException ex) {

		Assert.notNull(ex, "DriverException must not be null");

		return getExceptionTranslator().translateExceptionIfPossible(ex);
	}

	/**
	 * Translate the given {@link DriverException} into a generic {@link DataAccessException}.
	 * <p>
	 * The returned {@link DataAccessException} is supposed to contain the original {@code DriverException} as root cause.
	 * However, client code may not generally rely on this due to {@link DataAccessException}s possibly being caused by
	 * other resource APIs as well. That said, a {@code getRootCause() instanceof DataAccessException} check (and
	 * subsequent cast) is considered reliable when expecting Cassandra-based access to have happened.
	 *
	 * @param task readable text describing the task being attempted
	 * @param cql CQL query or update that caused the problem (may be {@literal null})
	 * @param ex the offending {@link DriverException}
	 * @return the DataAccessException, wrapping the {@code DriverException}
	 * @see org.springframework.dao.DataAccessException#getRootCause()
	 * @see <a href=
	 *      "http://docs.spring.io/spring/docs/current/spring-framework-reference/data-access.html#dao-exceptions">Consistent
	 *      exception hierarchy</a>
	 */
	protected DataAccessException translate(String task, @Nullable String cql, DriverException ex) {

		Assert.notNull(ex, "DriverException must not be null");

		return getExceptionTranslator().translate(task, cql, ex);
	}

	/**
	 * Create a new arg-based PreparedStatementSetter using the args passed in. By default, we'll create an
	 * {@link ArgumentPreparedStatementBinder}. This method allows for the creation to be overridden by subclasses.
	 *
	 * @param args object array with arguments
	 * @return the new {@link PreparedStatementBinder} to use
	 */
	protected PreparedStatementBinder newPreparedStatementBinder(Object[] args) {
		return new ArgumentPreparedStatementBinder(args);
	}

	/**
	 * Constructs a new instance of the {@link ResultSetExtractor} initialized with and adapting the given
	 * {@link RowCallbackHandler}.
	 *
	 * @param rowCallbackHandler {@link RowCallbackHandler} to adapt as a {@link ResultSetExtractor}.
	 * @return a {@link ResultSetExtractor} implementation adapting an instance of the {@link RowCallbackHandler}.
	 * @see AsyncCqlTemplate.RowCallbackHandlerResultSetExtractor
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

	/**
	 * Create a new RowMapper for reading columns as key-value pairs.
	 *
	 * @return the RowMapper to use
	 * @see ColumnMapRowMapper
	 */
	protected RowMapper<Map<String, Object>> newColumnMapRowMapper() {
		return new ColumnMapRowMapper();
	}

	/**
	 * Create a new RowMapper for reading result objects from a single column.
	 *
	 * @param requiredType the type that each result object is expected to match
	 * @return the RowMapper to use
	 * @see SingleColumnRowMapper
	 */
	protected <T> RowMapper<T> newSingleColumnRowMapper(Class<T> requiredType) {
		return SingleColumnRowMapper.newInstance(requiredType);
	}

	/**
	 * Determine CQL from potential provider object.
	 *
	 * @param cqlProvider object that's potentially a {@link CqlProvider}
	 * @return the CQL string, or {@literal null}
	 * @see CqlProvider
	 */
	@Nullable
	protected static String toCql(@Nullable Object cqlProvider) {

		return Optional.ofNullable(cqlProvider) //
				.filter(o -> o instanceof CqlProvider) //
				.map(o -> (CqlProvider) o) //
				.map(CqlProvider::getCql) //
				.orElse(null);
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
