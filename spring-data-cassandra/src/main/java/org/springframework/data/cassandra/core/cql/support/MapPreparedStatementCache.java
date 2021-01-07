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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

/**
 * {@link PreparedStatementCache} backed by a {@link Map} cache. Defaults to simple {@link ConcurrentHashMap} caching.
 * <p/>
 * Statements are cached with a key consisting of {@link CqlSession#getName() session name}, {@code keyspace} and the
 * {@code cql} text. Statement options (idempotency, timeouts) apply from the statement that was initially prepared.
 *
 * @author Mark Paluch
 * @author Aldo Bongio
 * @since 2.0
 * @deprecated since 3.2, the Cassandra driver has a built-in prepared statement cache with makes external caching of prepared statements superfluous.
 */
@Deprecated
public class MapPreparedStatementCache implements PreparedStatementCache {

	private final Map<CacheKey, PreparedStatement> cache;

	/**
	 * Create a new {@link MapPreparedStatementCache}.
	 *
	 * @param cache must not be {@literal null}.
	 */
	private MapPreparedStatementCache(Map<CacheKey, PreparedStatement> cache) {

		Assert.notNull(cache, "Cache must not be null");

		this.cache = cache;
	}

	/**
	 * Create a {@link MapPreparedStatementCache} using {@link ConcurrentHashMap}.
	 *
	 * @return the new {@link MapPreparedStatementCache} backed by {@link ConcurrentHashMap}.
	 */
	public static MapPreparedStatementCache create() {
		return of(new ConcurrentHashMap<>());
	}

	/**
	 * Create a {@link MapPreparedStatementCache} using the given {@link Map}.
	 *
	 * @return the new {@link MapPreparedStatementCache} backed the given {@link Map}.
	 */
	public static MapPreparedStatementCache of(Map<CacheKey, PreparedStatement> cache) {
		return new MapPreparedStatementCache(cache);
	}

	/**
	 * @return the underlying {@link Map cache}.
	 */
	protected Map<CacheKey, PreparedStatement> getCache() {
		return this.cache;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.support.PreparedStatementCache#getPreparedStatement(com.datastax.oss.driver.api.core.CqlSession, com.datastax.oss.driver.api.core.cql.SimpleStatement, java.util.function.Supplier)
	 */
	@Override
	public PreparedStatement getPreparedStatement(CqlSession session, SimpleStatement statement,
			Supplier<PreparedStatement> preparer) {

		CacheKey cacheKey = new CacheKey(session, statement.getQuery());

		return getCache().computeIfAbsent(cacheKey, key -> preparer.get());
	}

	/**
	 * {@link CacheKey} for {@link PreparedStatement} caching.
	 */
	protected static class CacheKey {

		final String sessionName;
		final String keyspace;
		final String cql;

		CacheKey(CqlSession session, String cql) {

			this.sessionName = session.getName();
			this.keyspace = session.getKeyspace().orElse(CqlIdentifier.fromCql("system")).asInternal();
			this.cql = cql;
		}

		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#equals(java.lang.Object)
		 */
		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (!(o instanceof CacheKey)) {
				return false;
			}
			CacheKey cacheKey = (CacheKey) o;
			if (!ObjectUtils.nullSafeEquals(sessionName, cacheKey.sessionName)) {
				return false;
			}
			if (!ObjectUtils.nullSafeEquals(keyspace, cacheKey.keyspace)) {
				return false;
			}
			return ObjectUtils.nullSafeEquals(cql, cacheKey.cql);
		}

		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		@Override
		public int hashCode() {
			int result = ObjectUtils.nullSafeHashCode(sessionName);
			result = 31 * result + ObjectUtils.nullSafeHashCode(keyspace);
			result = 31 * result + ObjectUtils.nullSafeHashCode(cql);
			return result;
		}
	}
}
