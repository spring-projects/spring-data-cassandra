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
 * A {@link CassandraAccessor} is able to access a Cassandra {@link Session} and the
 * {@link CassandraExceptionTranslator}.
 * <p>
 * Classes providing a higher abstraction level usually extend {@link CassandraAccessor} to provide a richer set of
 * functionality on top of a {@link Session}.
 *
 * @author David Webb
 * @author Mark Paluch
 * @see org.springframework.beans.factory.InitializingBean
 */
public class CassandraAccessor implements InitializingBean {

	/** Logger available to subclasses */
	protected final Logger logger = LoggerFactory.getLogger(getClass());

	private Session session;
	private CassandraExceptionTranslator exceptionTranslator = new CassandraExceptionTranslator();

	/**
	 * Ensure that the Cassandra Session has been set
	 */
	@Override
	public void afterPropertiesSet() {

		Assert.notNull(session, "Session must not be null!");
		Assert.notNull(exceptionTranslator, "CassandraExceptionTranslator must not be null!");
	}

	/**
	 * Set the exception translator for this instance.
	 *
	 * @param exceptionTranslator the exception translator to set, must not be {@literal null}.
	 * @see org.springframework.cassandra.support.CassandraExceptionTranslator
	 */
	public void setExceptionTranslator(CassandraExceptionTranslator exceptionTranslator) {

		Assert.notNull(exceptionTranslator, "CassandraExceptionTranslator must not be null!");
		this.exceptionTranslator = exceptionTranslator;
	}

	/**
	 * Return the exception translator for this instance.
	 *
	 * @return the exception translator
	 */
	public CassandraExceptionTranslator getExceptionTranslator() {
		return this.exceptionTranslator;
	}

	/**
	 * Returns the session.
	 *
	 * @return the session.
	 */
	public Session getSession() {
		return session;
	}

	/**
	 * @param session The session to set, must not be{@literal null}
	 */
	public void setSession(Session session) {

		Assert.notNull(session, "Session must not be null!");
		this.session = session;
	}
}
