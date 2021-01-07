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
package org.springframework.data.cassandra.core.cql.support;

import java.util.function.Supplier;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

/**
 * Cache interface to synchronously prepare CQL statements.
 * <p />
 * Implementing classes of {@link PreparedStatementCache} come with own synchronization and cache implementation
 * characteristics. A cache implementation should optimize for reduction of preparation calls and cache statements using
 * Cassandras cache key which is specific to the Cluster, keyspace, and CQL text.
 *
 * @author Mark Paluch
 * @since 2.0
 * @see PreparedStatement
 * @deprecated since 3.2, the Cassandra driver has a built-in prepared statement cache with makes external caching of
 *             prepared statements superfluous.
 */
@Deprecated
public interface PreparedStatementCache {

	/**
	 * Create a default cache backed by a {@link java.util.concurrent.ConcurrentHashMap}.
	 *
	 * @return a new {@link MapPreparedStatementCache}.
	 */
	static PreparedStatementCache create() {
		return MapPreparedStatementCache.create();
	}

	/**
	 * Obtain a {@link PreparedStatement} by {@link CqlSession} and {@link SimpleStatement}.
	 *
	 * @param session must not be {@literal null}.
	 * @param statement must not be {@literal null}.
	 * @return the {@link PreparedStatement}.
	 */
	default PreparedStatement getPreparedStatement(CqlSession session, SimpleStatement statement) {
		return getPreparedStatement(session, statement, () -> session.prepare(statement));
	}

	/**
	 * Obtain a {@link PreparedStatement} by {@link CqlSession} and {@link SimpleStatement}.
	 *
	 * @param session must not be {@literal null}.
	 * @param statement must not be {@literal null}.
	 * @param preparer must not be {@literal null}.
	 * @return the {@link PreparedStatement}.
	 */
	PreparedStatement getPreparedStatement(CqlSession session, SimpleStatement statement,
			Supplier<PreparedStatement> preparer);
}
