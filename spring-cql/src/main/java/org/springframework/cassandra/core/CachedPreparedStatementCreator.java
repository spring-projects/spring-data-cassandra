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
package org.springframework.cassandra.core;

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
 */
public class CachedPreparedStatementCreator implements PreparedStatementCreator {

	private static final Logger log = LoggerFactory.getLogger(CachedPreparedStatementCreator.class);
	private static final Map<Session, Map<String, PreparedStatement>> CACHE = new ConcurrentHashMap<Session, Map<String, PreparedStatement>>();

	private final String cql;

	/**
	 * Create a {@link PreparedStatementCreator} from the provided CQL.
	 * 
	 * @param cql must not be empty and not {@literal null}.
	 */
	public CachedPreparedStatementCreator(String cql) {

		Assert.hasText(cql, "CQL is required to create a PreparedStatement");

		this.cql = cql;
	}

	public String getCql() {
		return this.cql;
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.PreparedStatementCreator#createPreparedStatement(com.datastax.driver.core.Session)
	 */
	@Override
	public PreparedStatement createPreparedStatement(Session session) throws DriverException {

		StringBuilder cacheKey = new StringBuilder().append(session.getLoggedKeyspace()).append("|").append(this.cql);

		log.debug("Cachable PreparedStatement in Keyspace {}", session.getLoggedKeyspace());

		Map<String, PreparedStatement> sessionCache = getOrCreateSessionLocalCache(session);
		return getOrPrepareStatement(session, cacheKey.toString(), sessionCache);
	}

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

	private PreparedStatement getOrPrepareStatement(Session session, String cacheKey,
			Map<String, PreparedStatement> sessionCache) {

		PreparedStatement pstmt = sessionCache.get(cacheKey);

		if (pstmt == null) {

			synchronized (sessionCache) {

				if (sessionCache.containsKey(cacheKey)) {

					log.debug("Found cached PreparedStatement");
					return sessionCache.get(cacheKey);
				}

				log.debug("No Cached PreparedStatement found...Creating and Caching");

				pstmt = session.prepare(this.cql);
				sessionCache.put(cacheKey, pstmt);

				return pstmt;
			}
		}

		log.debug("Found cached PreparedStatement");

		return pstmt;
	}
}
