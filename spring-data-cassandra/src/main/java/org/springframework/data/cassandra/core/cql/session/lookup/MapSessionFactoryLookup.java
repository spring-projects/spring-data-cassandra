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

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.data.cassandra.SessionFactory;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Simple {@link SessionFactoryLookup} implementation that relies on a map for doing lookups.
 * <p>
 * Useful for testing environments or applications that need to match arbitrary {@link String} names to target
 * {@link SessionFactory} objects. This class is not thread-safe for modifications. Once initialized, it can be shared
 * amongst multiple threads and is thread-safe for reading.
 *
 * @author Mark Paluch
 * @since 2.0
 */
public class MapSessionFactoryLookup implements SessionFactoryLookup {

	private final Map<String, SessionFactory> sessionFactories = new ConcurrentHashMap<>(4);

	/**
	 * Create a new instance of {@link MapSessionFactoryLookup}.
	 */
	public MapSessionFactoryLookup() {}

	/**
	 * Create a new instance of {@link MapSessionFactoryLookup}.
	 *
	 * @param sessionFactories the {@link Map} of {@link SessionFactory session factories}. The keys are {@link String
	 *          Strings}, the values are actual {@link SessionFactory} instances.
	 */
	public MapSessionFactoryLookup(Map<String, SessionFactory> sessionFactories) {
		setSessionFactories(sessionFactories);
	}

	/**
	 * Create a new instance of {@link MapSessionFactoryLookup}.
	 *
	 * @param sessionFactoryName the name under which the supplied {@link SessionFactory} is to be added
	 * @param sessionFactory the {@link SessionFactory} to be added
	 */
	public MapSessionFactoryLookup(String sessionFactoryName, SessionFactory sessionFactory) {
		addSessionFactory(sessionFactoryName, sessionFactory);
	}

	/**
	 * Set the {@link Map} of {@link SessionFactory session factories}; the keys are {@link String Strings}, the values
	 * are actual {@link SessionFactory} instances.
	 * <p>
	 * If the supplied {@link Map} is {@literal null}, then this method call effectively has no effect.
	 *
	 * @param sessionFactories {@link Map} of {@link SessionFactory session factories}.
	 */
	public void setSessionFactories(@Nullable Map<String, SessionFactory> sessionFactories) {
		if (sessionFactories != null) {
			this.sessionFactories.putAll(sessionFactories);
		}
	}

	/**
	 * Get the {@link Map} of {@link SessionFactory session factories} maintained by this object.
	 * <p>
	 * The returned {@link Map} is {@link Collections#unmodifiableMap(java.util.Map) unmodifiable}.
	 *
	 * @return {@link Map} of {@link SessionFactory session factories}.
	 */
	public Map<String, SessionFactory> getSessionFactories() {
		return Collections.unmodifiableMap(this.sessionFactories);
	}

	/**
	 * Add the supplied {@link SessionFactory} to the map of {@link SessionFactory session factories} maintained by this
	 * object.
	 *
	 * @param sessionFactoryName the name under which the supplied {@link SessionFactory} is to be added
	 * @param sessionFactory the {@link SessionFactory} to be so added
	 */
	public void addSessionFactory(String sessionFactoryName, SessionFactory sessionFactory) {

		Assert.notNull(sessionFactoryName, "SessionFactory name must not be null");
		Assert.notNull(sessionFactory, "SessionFactory must not be null");

		this.sessionFactories.put(sessionFactoryName, sessionFactory);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.session.lookup.SessionFactoryLookup#getSessionFactory(java.lang.String)
	 */
	@Override
	public SessionFactory getSessionFactory(String sessionFactoryName) throws SessionFactoryLookupFailureException {

		Assert.notNull(sessionFactoryName, "SessionFactory name must not be null");

		SessionFactory sessionFactory = this.sessionFactories.get(sessionFactoryName);

		if (sessionFactory == null) {
			throw new SessionFactoryLookupFailureException(
					String.format("No SessionFactory with name [%s] registered", sessionFactoryName));
		}

		return sessionFactory;
	}
}
