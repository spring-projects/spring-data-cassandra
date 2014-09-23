/*
 * Copyright 2013-2014 the original author or authors.
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
 * This Prepared Statement Creator maintains a cache of all prepared statements for the duration of this life of the
 * container. When preparing statements with Cassandra, each Statement should be prepared once and only once due to the
 * overhead of preparing the statement.
 * 
 * @author David Webb
 */
public class CachedPreparedStatementCreator implements PreparedStatementCreator {

	private static final Logger log = LoggerFactory.getLogger(CachedPreparedStatementCreator.class);

	private final String cql;

	private static final Map<Session, Map<String, PreparedStatement>> psMap = new ConcurrentHashMap<Session, Map<String, PreparedStatement>>();

	/**
	 * Create a PreparedStatementCreator from the provided CQL.
	 * 
	 * @param cql
	 */
	public CachedPreparedStatementCreator(String cql) {
		Assert.notNull(cql, "CQL is required to create a PreparedStatement");
		this.cql = cql;
	}

	public String getCql() {
		return this.cql;
	}

	@Override
	public PreparedStatement createPreparedStatement(Session session) throws DriverException {

		StringBuilder keyspaceCQLKey = new StringBuilder().append(session.getLoggedKeyspace()).append("|").append(this.cql);

		log.debug(String.format("Cachable PreparedStatement in Keyspace [%s]", session.getLoggedKeyspace()));

		Map<String, PreparedStatement> sessionMap = psMap.get(session);
		if (sessionMap == null) {
			sessionMap = new ConcurrentHashMap<String, PreparedStatement>();
			psMap.put(session, sessionMap);
		}

		PreparedStatement pstmt = sessionMap.get(keyspaceCQLKey.toString());
		if (pstmt == null) {
			log.debug("No Cached PreparedStatement found...Creating and Caching");
			pstmt = session.prepare(this.cql);
			sessionMap.put(keyspaceCQLKey.toString(), pstmt);
		} else {
			log.debug("Found cached PreparedStatement");
		}

		return pstmt;
	}

}
