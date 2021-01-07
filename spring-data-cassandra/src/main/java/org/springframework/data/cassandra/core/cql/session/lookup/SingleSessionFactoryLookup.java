/*
 * Copyright 2017-2021 the original author or authors.
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
package org.springframework.data.cassandra.core.cql.session.lookup;

import org.springframework.data.cassandra.SessionFactory;
import org.springframework.util.Assert;

/**
 * An implementation of {@link SessionFactoryLookup} that simply wraps a single given {@link SessionFactory}, returned
 * for any session factory name. Useful for testing or environments that provide only one {@link SessionFactory}.
 *
 * @author Mark Paluch
 * @since 2.0
 */
public class SingleSessionFactoryLookup implements SessionFactoryLookup {

	private final SessionFactory sessionFactory;

	/**
	 * Create a new instance of {@link SingleSessionFactoryLookup} given {@link SessionFactory}.
	 *
	 * @param sessionFactory the single {@link SessionFactory} to wrap, must not be {@literal null}.
	 */
	public SingleSessionFactoryLookup(SessionFactory sessionFactory) {

		Assert.notNull(sessionFactory, "SessionFactory must not be null");

		this.sessionFactory = sessionFactory;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.session.lookup.SessionFactoryLookup#getSessionFactory(java.lang.String)
	 */
	@Override
	public SessionFactory getSessionFactory(String sessionFactoryName) throws SessionFactoryLookupFailureException {
		return sessionFactory;
	}
}
