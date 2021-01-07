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

import java.util.HashMap;
import java.util.Map;

import com.datastax.oss.driver.api.core.CqlSession;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.cassandra.SessionFactory;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Abstract {@link SessionFactory} implementation that routes {@link #getSession()} calls to one of various target
 * {@link SessionFactory factories} based on a lookup key. The latter is usually (but not necessarily) determined
 * through some thread-bound transaction context.
 *
 * @author Mark Paluch
 * @since 2.0
 * @see #setTargetSessionFactories(Map)
 * @see #setDefaultTargetSessionFactory(Object)
 * @see #determineCurrentLookupKey()
 * @see SessionFactoryLookup
 */
public abstract class AbstractRoutingSessionFactory implements SessionFactory, InitializingBean {

	private @Nullable Map<Object, Object> targetSessionFactories;

	private @Nullable Object defaultTargetSessionFactory;

	private boolean lenientFallback = true;

	private SessionFactoryLookup sessionFactoryLookup = new MapSessionFactoryLookup();

	private @Nullable Map<Object, SessionFactory> resolvedSessionFactories;

	private @Nullable SessionFactory resolvedDefaultSessionFactory;

	/**
	 * Specify the map of target session factories, with the lookup key as key.
	 * <p>
	 * The mapped value can either be a corresponding {@link SessionFactory} instance or a data source name String (to be
	 * resolved via a {@link #setSessionFactoryLookup(SessionFactoryLookup)}).
	 * <p>
	 * The key can be of arbitrary type; this class implements the generic lookup process only. The concrete key
	 * representation will be handled by {@link #resolveSpecifiedLookupKey(Object)} and
	 * {@link #determineCurrentLookupKey()}.
	 */
	public void setTargetSessionFactories(Map<Object, Object> targetSessionFactories) {

		Assert.notNull(targetSessionFactories, "Target SessionFactories must not be null");

		this.targetSessionFactories = targetSessionFactories;
	}

	/**
	 * Specify the default target {@link SessionFactory}, if any.
	 * <p>
	 * The mapped value can either be a corresponding {@link SessionFactory} instance or a data source name String (to be
	 * resolved via a {@link #setSessionFactoryLookup(SessionFactoryLookup)}).
	 * <p>
	 * This {@link SessionFactory} will be used as target if none of the keyed {@link #setTargetSessionFactories(Map)}
	 * match the {@link #determineCurrentLookupKey()} current lookup key.
	 */
	public void setDefaultTargetSessionFactory(Object defaultTargetSessionFactory) {

		Assert.notNull(defaultTargetSessionFactory, "Default target SessionFactory must not be null");

		this.defaultTargetSessionFactory = defaultTargetSessionFactory;
	}

	/**
	 * Specify whether to apply a lenient fallback to the default {@link SessionFactory} if no specific
	 * {@link SessionFactory} could be found for the current lookup key.
	 * <p>
	 * Default is {@literal true}, accepting lookup keys without a corresponding entry in the target
	 * {@link SessionFactory} map - simply falling back to the default {@link SessionFactory} in that case.
	 * <p>
	 * Switch this flag to {@literal false} if you would prefer the fallback to only apply if the lookup key was
	 * {@literal null}. Lookup keys without a {@link SessionFactory} entry will then lead to an
	 * {@link IllegalStateException}.
	 *
	 * @param lenientFallback {@literal true} to accepting lookup keys without a corresponding entry in the target.
	 * @see #setTargetSessionFactories
	 * @see #setDefaultTargetSessionFactory
	 * @see #determineCurrentLookupKey()
	 */
	public void setLenientFallback(boolean lenientFallback) {
		this.lenientFallback = lenientFallback;
	}

	/**
	 * Set the {@link SessionFactoryLookup} implementation to use for resolving session factory name Strings in the
	 * {@link #setTargetSessionFactories(Map)} map.
	 * <p>
	 * Default is a {@link MapSessionFactoryLookup}, allowing a string keyed map of {@link SessionFactory session
	 * factories}.
	 *
	 * @param sessionFactoryLookup the {@link SessionFactoryLookup}. Defaults to {@link MapSessionFactoryLookup} if
	 *          {@literal null}.
	 */
	public void setSessionFactoryLookup(@Nullable SessionFactoryLookup sessionFactoryLookup) {
		this.sessionFactoryLookup = (sessionFactoryLookup != null ? sessionFactoryLookup : new MapSessionFactoryLookup());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.session.SessionFactory#getSession()
	 */
	@Override
	public CqlSession getSession() {
		return determineTargetSessionFactory().getSession();
	}

	// -------------------------------------------------------------------------
	// Implementation hooks and helper methods
	// -------------------------------------------------------------------------

	@Override
	public void afterPropertiesSet() {

		Assert.notNull(this.targetSessionFactories, "Property targetSessionFactories is required");

		this.resolvedSessionFactories = new HashMap<>(this.targetSessionFactories.size());

		for (Map.Entry<Object, Object> entry : this.targetSessionFactories.entrySet()) {

			Object lookupKey = resolveSpecifiedLookupKey(entry.getKey());
			SessionFactory sessionFactory = resolveSpecifiedSessionFactory(entry.getValue());
			this.resolvedSessionFactories.put(lookupKey, sessionFactory);
		}

		if (this.defaultTargetSessionFactory != null) {
			this.resolvedDefaultSessionFactory = resolveSpecifiedSessionFactory(this.defaultTargetSessionFactory);
		}
	}

	/**
	 * Resolve the given lookup key object, as specified in the {@link #setTargetSessionFactories(Map)} map, into the
	 * actual lookup key to be used for matching with the {@link #determineCurrentLookupKey() current lookup key}.
	 * <p>
	 * The default implementation simply returns the given key as-is.
	 *
	 * @param lookupKey the lookup key object as specified by the user
	 * @return the lookup key as needed for matching
	 */
	protected Object resolveSpecifiedLookupKey(Object lookupKey) {
		return lookupKey;
	}

	/**
	 * Resolve the specified {@code sessionFactory} object into a {@link SessionFactory} instance.
	 * <p>
	 * The default implementation handles {@link SessionFactory} instances and session factory names (to be resolved via a
	 * {@link #setSessionFactoryLookup(SessionFactoryLookup)}).
	 *
	 * @param sessionFactory the session factory value object as specified in the {@link #setTargetSessionFactories(Map)}
	 *          map
	 * @return the resolved {@link SessionFactory}
	 * @throws IllegalArgumentException in case of an unsupported value type.
	 * @throws SessionFactoryLookupFailureException if the lookup failed.
	 */
	protected SessionFactory resolveSpecifiedSessionFactory(Object sessionFactory) throws IllegalArgumentException {

		if (sessionFactory instanceof SessionFactory) {
			return (SessionFactory) sessionFactory;
		} else if (sessionFactory instanceof String) {
			return this.sessionFactoryLookup.getSessionFactory((String) sessionFactory);
		} else {
			throw new IllegalArgumentException(String.format(
					"Illegal session factory value. Only [org.springframework.data.cassandra.core.cql.session.SessionFactory]"
							+ " and String supported: %s",
					sessionFactory));
		}
	}

	/**
	 * Retrieve the current target {@link SessionFactory}. Determines the {@link #determineCurrentLookupKey() current
	 * lookup key}, performs a lookup in the {@link #setTargetSessionFactories(Map)} map, falls back to the specified
	 * {@link #setDefaultTargetSessionFactory default target SessionFactory} if necessary.
	 *
	 * @see #determineCurrentLookupKey()
	 */
	protected SessionFactory determineTargetSessionFactory() {

		Assert.notNull(this.resolvedSessionFactories, "SessionFactory router not initialized");

		Object lookupKey = determineCurrentLookupKey();
		SessionFactory sessionFactory = this.resolvedSessionFactories.get(lookupKey);

		if (sessionFactory == null && (this.lenientFallback || lookupKey == null)) {
			sessionFactory = this.resolvedDefaultSessionFactory;
		}

		if (sessionFactory == null) {
			throw new IllegalStateException(
					String.format("Cannot determine target SessionFactory for lookup key [%s]", lookupKey));
		}

		return sessionFactory;
	}

	/**
	 * Determine the current lookup key. This will typically be implemented to check a thread-bound context.
	 * <p>
	 * Allows for arbitrary keys.
	 *
	 * @return the current lookup key. The returned key needs to match the stored lookup key type.
	 */
	@Nullable
	protected abstract Object determineCurrentLookupKey();
}
