/*
 * Copyright 2013-2020 the original author or authors.
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

import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataAccessException;
import org.springframework.data.cassandra.SessionFactory;
import org.springframework.data.cassandra.core.cql.session.DefaultSessionFactory;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;

/**
 * {@link CassandraAccessor} provides access to a Cassandra {@link SessionFactory} and the
 * {@link CassandraExceptionTranslator}.
 * <p>
 * Classes providing a higher abstraction level usually extend {@link CassandraAccessor} to provide a richer set of
 * functionality on top of a {@link SessionFactory} using {@link CqlSession}.
 *
 * @author David Webb
 * @author Mark Paluch
 * @author John Blum
 * @see org.springframework.beans.factory.InitializingBean
 * @see com.datastax.driver.core.Session
 */
public class CassandraAccessor implements InitializingBean {

	/** Logger available to subclasses */
	protected final Logger logger = LoggerFactory.getLogger(getClass());

	private CqlExceptionTranslator exceptionTranslator = new CassandraExceptionTranslator();

	/**
	 * If this variable is set to a non-negative value, it will be used for setting the {@code pageSize} property on
	 * statements used for query processing.
	 */
	private int pageSize = -1;

	/**
	 * If this variable is set to a value, it will be used for setting the {@code consistencyLevel} property on statements
	 * used for query processing.
	 */
	private @Nullable ConsistencyLevel consistencyLevel;

	/**
	 * If this variable is set to a value, it will be used for setting the {@code retryPolicy} property on statements used
	 * for query processing.
	 */
	private @Deprecated @Nullable RetryPolicy retryPolicy;

	private @Nullable SessionFactory sessionFactory;

	/**
	 * Ensures the Cassandra {@link CqlSession} and exception translator has been propertly set.
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
	 * Set the retry policy for this template. This is important for defining behavior when a request fails.
	 *
	 * @see RetryPolicy
	 * @deprecated since 3.0. Use driver execution profiles instead.
	 */
	@Deprecated
	public void setRetryPolicy(@Nullable RetryPolicy retryPolicy) {
		this.retryPolicy = retryPolicy;
	}

	/**
	 * @return the {@link RetryPolicy} specified for this template.
	 * @deprecated since 3.0. Use driver execution profiles instead.
	 */
	@Nullable
	@Deprecated
	public RetryPolicy getRetryPolicy() {
		return this.retryPolicy;
	}

	/**
	 * Sets the Cassandra {@link CqlSession} used by this template to perform Cassandra data access operations. The
	 * {@code session} will replace the current {@link #getSessionFactory()} with {@link DefaultSessionFactory}.
	 *
	 * @param session Cassandra {@link CqlSession} used by this template, must not be{@literal null}.
	 * @see CqlSession
	 * @see DefaultSessionFactory
	 */
	public void setSession(CqlSession session) {

		Assert.notNull(session, "Session must not be null");

		setSessionFactory(new DefaultSessionFactory(session));
	}

	/**
	 * Returns the Cassandra {@link CqlSession} from {@link SessionFactory} used by this template to perform Cassandra
	 * data access operations.
	 *
	 * @return the Cassandra {@link CqlSession} used by this template.
	 * @see com.datastax.driver.core.Session
	 * @deprecated since 2.0. This class uses a {@link SessionFactory} to dispatch CQL calls amongst different
	 *             {@link CqlSession}s during its lifecycle.
	 */
	@Deprecated
	public CqlSession getSession() {

		Assert.state(getSessionFactory() != null, "SessionFactory was not properly initialized");

		return getSessionFactory().getSession();
	}

	/**
	 * Sets the Cassandra {@link SessionFactory} used by this template to perform Cassandra data access operations.
	 *
	 * @param sessionFactory Cassandra {@link CqlSession} used by this template. Must not be{@literal null}.
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
	 * Create a {@link SimpleStatement} given {@code cql}.
	 *
	 * @param cql the CQL query.
	 * @return the {@link SimpleStatement} to use.
	 * @since 3.0
	 */
	protected SimpleStatement newStatement(String cql) {
		return SimpleStatement.newInstance(cql);
	}

	/**
	 * Prepare the given CQL Statement applying statement settings such as page size and consistency level.
	 *
	 * @param statement the CQL Statement to prepare
	 * @see #setPageSize(int)
	 * @see #setConsistencyLevel(ConsistencyLevel)
	 */
	protected Statement<?> applyStatementSettings(Statement<?> statement) {

		Statement<?> statementToUse = statement;
		ConsistencyLevel consistencyLevel = getConsistencyLevel();

		if (getFetchSize() > -1 && statement.getPageSize() < 0) {
			statementToUse = statementToUse.setPageSize(getFetchSize());
		}

		if (consistencyLevel != null && statementToUse.getConsistencyLevel() == null) {
			statementToUse = statementToUse.setConsistencyLevel(getConsistencyLevel());
		}

		return statementToUse;
	}

	/**
	 * Translate the given {@link RuntimeException} into a generic {@link DataAccessException}.
	 * <p>
	 * The returned {@link DataAccessException} is supposed to contain the original {@code RuntimeException} as root
	 * cause. However, client code may not generally rely on this due to {@link DataAccessException}s possibly being
	 * caused by other resource APIs as well. That said, a {@code getRootCause() instanceof DataAccessException} check
	 * (and subsequent cast) is considered reliable when expecting Cassandra-based access to have happened.
	 *
	 * @param ex the offending {@link RuntimeException}
	 * @return the DataAccessException, wrapping the {@code RuntimeException}
	 * @see <a href=
	 *      "https://docs.spring.io/spring/docs/current/spring-framework-reference/data-access.html#dao-exceptions">Consistent
	 *      exception hierarchy</a>
	 * @see DataAccessException
	 */
	@Nullable
	protected DataAccessException translateExceptionIfPossible(RuntimeException ex) {

		Assert.notNull(ex, "RuntimeException must not be null");

		return getExceptionTranslator().translateExceptionIfPossible(ex);
	}

	/**
	 * Translate the given {@link RuntimeException} into a generic {@link DataAccessException}.
	 * <p>
	 * The returned {@link DataAccessException} is supposed to contain the original {@code RuntimeException} as root
	 * cause. However, client code may not generally rely on this due to {@link DataAccessException}s possibly being
	 * caused by other resource APIs as well. That said, a {@code getRootCause() instanceof DataAccessException} check
	 * (and subsequent cast) is considered reliable when expecting Cassandra-based access to have happened.
	 *
	 * @param task readable text describing the task being attempted
	 * @param cql CQL query or update that caused the problem (may be {@literal null})
	 * @param ex the offending {@link RuntimeException}
	 * @return the DataAccessException, wrapping the {@code RuntimeException}
	 * @see org.springframework.dao.DataAccessException#getRootCause()
	 * @see <a href=
	 *      "https://docs.spring.io/spring/docs/current/spring-framework-reference/data-access.html#dao-exceptions">Consistent
	 *      exception hierarchy</a>
	 */
	protected DataAccessException translate(String task, @Nullable String cql, RuntimeException ex) {

		Assert.notNull(ex, "RuntimeException must not be null");

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
}
