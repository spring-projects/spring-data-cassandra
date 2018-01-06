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
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.DriverException;

/**
 * This {@link PreparedStatementCreator} maintains a static cache of all prepared statements for the duration of the JVM
 * runtime, more specific the lifecycle of the associated {@link ClassLoader}. When preparing statements with Cassandra,
 * each Statement should be prepared once and only once due to the overhead of preparing the statement.
 * <p>
 * {@link CachedPreparedStatementCreator} is thread-safe and does not require external synchronization when used by
 * concurrent threads.
 *
 * @author David Webb
 * @author Mark Paluch
 * @deprecated since 2.0. This class uses an unsafe, static held cache and is not able to prepare
 *             {@link com.datastax.driver.core.querybuilder.BuiltStatement}. Use
 *             {@link org.springframework.data.cassandra.core.cql.support.CachedPreparedStatementCreator}.
 */
@Deprecated
public class CachedPreparedStatementCreator implements PreparedStatementCreator {

	private static final Map<Session, Map<String, PreparedStatement>> CACHE = new ConcurrentHashMap<>();

	protected final Logger log = LoggerFactory.getLogger(getClass());

	private final String cql;

	/**
	 * Create a {@link PreparedStatementCreator} from the provided CQL.
	 *
	 * @param cql must not be empty or {@literal null}.
	 */
	public CachedPreparedStatementCreator(String cql) {

		Assert.hasText(cql, "CQL is required to create a PreparedStatement");

		this.cql = cql;
	}

	/**
	 * Returns the CQL statement on which the {@link PreparedStatement} will be based.
	 *
	 * @return a String containing the CQL of the {@link PreparedStatement}.
	 */
	public String getCql() {
		return this.cql;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.PreparedStatementCreator#createPreparedStatement(com.datastax.driver.core.Session)
	 */
	@Override
	public PreparedStatement createPreparedStatement(Session session) throws DriverException {

		String cacheKey = String.valueOf(session.getLoggedKeyspace()).concat("|").concat(this.cql);

		log.debug("Cacheable PreparedStatement in Keyspace {}", session.getLoggedKeyspace());

		Map<String, PreparedStatement> sessionCache = getOrCreateSessionLocalCache(session);

		return getOrPrepareStatement(session, cacheKey, sessionCache);
	}

	@SuppressWarnings("all")
	private Map<String, PreparedStatement> getOrCreateSessionLocalCache(Session session) {

		Map<String, PreparedStatement> sessionMap = CACHE.get(session);

		if (sessionMap == null) {

			synchronized (session) {

				if (CACHE.containsKey(session)) {
					sessionMap = CACHE.get(session);
				} else {
					sessionMap = new ConcurrentHashMap<String, PreparedStatement>();
					CACHE.put(session, sessionMap);
				}
			}
		}

		return sessionMap;
	}

	@SuppressWarnings("all")
	private PreparedStatement getOrPrepareStatement(Session session, String cacheKey,
			Map<String, PreparedStatement> sessionCache) {

		PreparedStatement preparedStatement = sessionCache.get(cacheKey);

		if (preparedStatement == null) {

			synchronized (sessionCache) {

				if (sessionCache.containsKey(cacheKey)) {
					log.debug("Found cached PreparedStatement");
					preparedStatement = sessionCache.get(cacheKey);
				} else {
					log.debug("No cached PreparedStatement found... creating and caching");
					preparedStatement = session.prepare(this.cql);
					sessionCache.put(cacheKey, preparedStatement);
				}
			}
		}

		return preparedStatement;
	}
}
