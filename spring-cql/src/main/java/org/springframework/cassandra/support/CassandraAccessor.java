/*
 * Copyright 2013-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.cassandra.support;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import com.datastax.driver.core.Session;

/**
 * {@link CassandraAccessor} provides access to a Cassandra {@link Session} and the {@link CassandraExceptionTranslator}
 * .
 * <p>
 * Classes providing a higher abstraction level usually extend {@link CassandraAccessor} to provide a richer set of
 * functionality on top of a {@link Session}.
 *
 * @author David Webb
 * @author Mark Paluch
 * @author John Blum
 * @see org.springframework.beans.factory.InitializingBean
 * @see com.datastax.driver.core.Session
 */
public class CassandraAccessor implements InitializingBean {

	protected CassandraExceptionTranslator exceptionTranslator = new CassandraExceptionTranslator();

	protected final Logger logger = LoggerFactory.getLogger(getClass());

	private Session session;

	/**
	 * Ensures the Cassandra {@link Session} and exception translator has been propertly set.
	 */
	@Override
	public void afterPropertiesSet() {
		Assert.state(session != null, "Session must not be null");
	}

	/* (non-Javadoc) */
	protected void logDebug(String logMessage, Object... array) {
		if (logger.isDebugEnabled()) {
			logger.debug(logMessage, array);
		}
	}

	/**
	 * Sets the exception translator used by this template to translate Cassandra specific Exceptions into Spring DAO's
	 * Exception Hierarchy.
	 *
	 * @param exceptionTranslator exception translator to set; must not be {@literal null}.
	 * @see org.springframework.cassandra.support.CassandraExceptionTranslator
	 */
	public void setExceptionTranslator(CassandraExceptionTranslator exceptionTranslator) {
		Assert.notNull(exceptionTranslator, "CassandraExceptionTranslator must not be null");
		this.exceptionTranslator = exceptionTranslator;
	}

	/**
	 * Return the exception translator used by this template to translate Cassandra specific Exceptions into Spring DAO's
	 * Exception Hierarchy.
	 *
	 * @return the Cassandra exception translator.
	 * @see org.springframework.cassandra.support.CassandraExceptionTranslator
	 */
	public CassandraExceptionTranslator getExceptionTranslator() {
		Assert.state(this.exceptionTranslator != null, "CassandraExceptionTranslator was not properly initialized");
		return this.exceptionTranslator;
	}

	/**
	 * Sets the Cassandra {@link Session} used by this template to perform Cassandra data access operations.
	 *
	 * @param session Cassandra {@link Session} used by this template. Must not be{@literal null}.
	 * @see com.datastax.driver.core.Session
	 */
	public void setSession(Session session) {
		Assert.notNull(session, "Session must not be null");
		this.session = session;
	}

	/**
	 * Returns the Cassandra {@link Session} used by this template to perform Cassandra data access operations.
	 *
	 * @return the Cassandra {@link Session} used by this template.
	 * @see com.datastax.driver.core.Session
	 */
	public Session getSession() {
		Assert.state(this.session != null, "Session was not properly initialized");
		return this.session;
	}
}
