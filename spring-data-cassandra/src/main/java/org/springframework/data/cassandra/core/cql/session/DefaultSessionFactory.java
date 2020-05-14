/*
 * Copyright 2017-2020 the original author or authors.
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
package org.springframework.data.cassandra.core.cql.session;

import com.datastax.oss.driver.api.core.CqlSession;

import org.springframework.data.cassandra.SessionFactory;
import org.springframework.util.Assert;

/**
 * Default {@link SessionFactory} implementation.
 * <p>
 * This class uses a singleton {@link CqlSession} and returns the same instances.
 *
 * @author Mark Paluch
 * @since 2.0
 * @see #getSession()
 * @see CqlSession
 */
public class DefaultSessionFactory implements SessionFactory {

	private final CqlSession session;

	/**
	 * Constructs a new {@link DefaultSessionFactory} given {@link CqlSession}.
	 *
	 * @param session the {@link CqlSession} to be used in {@link #getSession()}.
	 */
	public DefaultSessionFactory(CqlSession session) {

		Assert.notNull(session, "Session must not be null");

		this.session = session;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.session.SessionFactory#getSession()
	 */
	@Override
	public CqlSession getSession() {
		return session;
	}
}
