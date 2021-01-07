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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataAccessException;
import org.springframework.data.cassandra.ReactiveSession;
import org.springframework.data.cassandra.ReactiveSessionFactory;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Base class for {@link ReactiveCqlTemplate} and other CQL-accessing DAO helpers, defining common properties such as
 * {@link ReactiveSessionFactory} and exception translator.
 * <p>
 * Not intended to be used directly.
 *
 * @author Mark Paluch
 * @since 2.0
 * @see InitializingBean
 * @see ReactiveSession
 * @see ReactiveCqlTemplate
 */
public abstract class ReactiveCassandraAccessor implements InitializingBean {

	/** Logger available to subclasses */
	protected final Logger logger = LoggerFactory.getLogger(getClass());

	private CqlExceptionTranslator exceptionTranslator = new CassandraExceptionTranslator();

	private @Nullable ReactiveSessionFactory sessionFactory;

	/**
	 * Sets the {@link ReactiveSessionFactory} to use.
	 *
	 * @param sessionFactory must not be {@literal null}.
	 */
	public void setSessionFactory(ReactiveSessionFactory sessionFactory) {

		Assert.notNull(sessionFactory, "ReactiveSessionFactory must not be null");

		this.sessionFactory = sessionFactory;
	}

	/**
	 * Returns the configured {@link ReactiveSessionFactory}.
	 *
	 * @return the configured {@link ReactiveSessionFactory}.
	 */
	@Nullable
	public ReactiveSessionFactory getSessionFactory() {
		return sessionFactory;
	}

	/**
	 * Sets the exception translator used by this template to translate Cassandra specific exceptions into Spring DAO's
	 * Exception Hierarchy.
	 *
	 * @param exceptionTranslator exception translator to set; must not be {@literal null}.
	 * @see CassandraExceptionTranslator
	 * @see DataAccessException
	 */
	public void setExceptionTranslator(CqlExceptionTranslator exceptionTranslator) {

		Assert.notNull(exceptionTranslator, "CQLExceptionTranslator must not be null");

		this.exceptionTranslator = exceptionTranslator;
	}

	/**
	 * Returns the exception translator for this instance.
	 *
	 * @return the Cassandra exception translator.
	 * @see CassandraExceptionTranslator
	 */
	public CqlExceptionTranslator getExceptionTranslator() {
		return this.exceptionTranslator;
	}

	/**
	 * Ensures the Cassandra {@link ReactiveSessionFactory} and exception translator has been properly set.
	 */
	@Override
	public void afterPropertiesSet() {
		Assert.notNull(sessionFactory, "ReactiveSessionFactory must not be null");
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
	 *      "https://docs.spring.io/spring/docs/current/spring-framework-reference/data-access.html#dao-exceptions">Consistent
	 *      exception hierarchy</a>
	 * @see DataAccessException
	 */
	@Nullable
	protected DataAccessException translateExceptionIfPossible(RuntimeException ex) {

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
	 *      "https://docs.spring.io/spring/docs/current/spring-framework-reference/data-access.html#dao-exceptions">Consistent
	 *      exception hierarchy</a>
	 */
	protected DataAccessException translate(String task, @Nullable String cql, RuntimeException ex) {

		Assert.notNull(ex, "DriverException must not be null");

		return getExceptionTranslator().translate(task, cql, ex);
	}
}
