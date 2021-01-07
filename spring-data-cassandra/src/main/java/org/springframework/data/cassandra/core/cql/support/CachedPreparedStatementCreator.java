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

import org.springframework.data.cassandra.core.cql.PreparedStatementCreator;
import org.springframework.data.cassandra.core.cql.QueryOptions;
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;

/**
 * {@link PreparedStatementCreator} implementation using caching of prepared statements.
 * <p />
 * Regular CQL statements are prepared on first use and executed as prepared statements. Prepared statements are cached
 * by Cassandra itself (invalidation/eviction possible), in the driver to be able to re-prepare a statement and in this
 * {@link CachedPreparedStatementCreator} using {@link PreparedStatementCache}.
 *
 * @author Mark Paluch
 * @since 2.0
 * @see PreparedStatementCache
 * @deprecated since 3.2, the Cassandra driver has a built-in prepared statement cache with makes external caching of prepared statements superfluous.
 */
@Deprecated
public class CachedPreparedStatementCreator implements PreparedStatementCreator {

	private final PreparedStatementCache cache;

	private final SimpleStatement statement;

	/**
	 * Create a new {@link CachedPreparedStatementCreator}.
	 *
	 * @param cache must not be {@literal null}.
	 * @param statement must not be {@literal null}.
	 */
	protected CachedPreparedStatementCreator(PreparedStatementCache cache, SimpleStatement statement) {

		Assert.notNull(cache, "Cache must not be null");

		this.cache = cache;
		this.statement = statement;
	}

	/**
	 * Create a new {@link CachedPreparedStatementCreator} given {@link PreparedStatementCache} and
	 * {@link SimpleStatement} to prepare. Subsequent calls require the a {@link SimpleStatement} object with the same CQL
	 * test for a cache hit. Otherwise, the statement is likely to be re-prepared.
	 *
	 * @param cache must not be {@literal null}.
	 * @param statement must not be {@literal null}.
	 * @return the {@link CachedPreparedStatementCreator} for {@link Statement}.
	 */
	public static CachedPreparedStatementCreator of(PreparedStatementCache cache, SimpleStatement statement) {

		Assert.notNull(cache, "Cache must not be null");
		Assert.notNull(statement, "Statement must not be null");

		return new CachedPreparedStatementCreator(cache, statement);
	}

	/**
	 * Create a new {@link CachedPreparedStatementCreator} given {@link PreparedStatementCache} and {@code cql} to
	 * prepare. Subsequent calls require the a CQL statement that {@link String#equals(Object) are equal} to the
	 * previously used CQL string for a cache hit. Otherwise, the statement is likely to be re-prepared.
	 *
	 * @param cache must not be {@literal null}.
	 * @param cql must not be {@literal null} or empty.
	 * @return the {@link CachedPreparedStatementCreator} for {@code cql}.
	 */
	public static CachedPreparedStatementCreator of(PreparedStatementCache cache, String cql) {

		Assert.notNull(cache, "Cache must not be null");
		Assert.hasText(cql, "CQL statement is required");

		return new CachedPreparedStatementCreator(cache, SimpleStatement.newInstance(cql));
	}

	/**
	 * Create a new {@link CachedPreparedStatementCreator} given {@link PreparedStatementCache} and {@code cql} to
	 * prepare. This method applies {@link QueryOptions} to the {@link SimpleStatement} before preparing it. Subsequent
	 * calls require the a CQL statement that {@link String#equals(Object) are equal} to the previously used CQL string
	 * for a cache hit. Otherwise, the statement is likely to be re-prepared.
	 *
	 * @param cache must not be {@literal null}.
	 * @param cql must not be {@literal null} or empty.
	 * @param queryOptions must not be {@literal null}.
	 * @return the {@link CachedPreparedStatementCreator} for {@code cql}.
	 */
	public static CachedPreparedStatementCreator of(PreparedStatementCache cache, String cql, QueryOptions queryOptions) {

		Assert.notNull(cache, "Cache must not be null");
		Assert.hasText(cql, "CQL statement is required");
		Assert.notNull(queryOptions, "QueryOptions must not be null");

		return new CachedPreparedStatementCreator(cache, SimpleStatement.newInstance(cql));
	}

	/**
	 * @return the underlying {@link PreparedStatementCache}.
	 */
	public PreparedStatementCache getCache() {
		return this.cache;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.PreparedStatementCreator#createPreparedStatement(com.datastax.oss.driver.api.core.CqlSession)
	 */
	@Override
	public PreparedStatement createPreparedStatement(CqlSession session) throws DriverException {
		return getCache().getPreparedStatement(session, this.statement);
	}
}
