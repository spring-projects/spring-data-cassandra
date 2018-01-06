/*
 * Copyright 2017-2018 the original author or authors.
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
package org.springframework.data.cassandra.core.cql.session;

import org.springframework.data.cassandra.SessionFactory;
import org.springframework.util.Assert;

import com.datastax.driver.core.Session;

/**
 * Default {@link SessionFactory} implementation.
 * <p>
 * This class uses a singleton {@link Session} and returns the same instances.
 *
 * @author Mark Paluch
 * @since 2.0
 * @see #getSession()
 * @see Session
 */
public class DefaultSessionFactory implements SessionFactory {

	private final Session session;

	/**
	 * Constructs a new {@link DefaultSessionFactory} given {@link Session}.
	 *
	 * @param session the {@link Session} to be used in {@link #getSession()}.
	 */
	public DefaultSessionFactory(Session session) {

		Assert.notNull(session, "Session must not be null");

		this.session = session;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.session.SessionFactory#getSession()
	 */
	@Override
	public Session getSession() {
		return session;
	}
}
