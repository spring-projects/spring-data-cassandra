/*
 * Copyright 2013-present the original author or authors.
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
package org.springframework.data.cassandra.core.cql;

import java.nio.ByteBuffer;
import java.time.Duration;

import org.jspecify.annotations.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;

/**
 * Cassandra Query Options for queries. {@link QueryOptions} allow tuning of various query options on a per-request
 * level. Only options that are set are applied to queries.
 *
 * @author David Webb
 * @author Mark Paluch
 * @author Tomasz Lelek
 * @author Sam Lightfoot
 */
public class QueryOptions {

	private static final QueryOptions EMPTY = QueryOptions.builder().build();

	private final @Nullable ConsistencyLevel consistencyLevel;

	private final ExecutionProfileResolver executionProfileResolver;

	private final @Nullable Boolean idempotent;

	private final @Nullable CqlIdentifier keyspace;

	private final @Nullable Integer pageSize;

	private final @Nullable CqlIdentifier routingKeyspace;

	private final @Nullable ByteBuffer routingKey;

	private final @Nullable ConsistencyLevel serialConsistencyLevel;

	private final Duration timeout;

	private final @Nullable Boolean tracing;

	protected QueryOptions(@Nullable ConsistencyLevel consistencyLevel, ExecutionProfileResolver executionProfileResolver,
			@Nullable Boolean idempotent, @Nullable CqlIdentifier keyspace, @Nullable Integer pageSize,
			@Nullable CqlIdentifier routingKeyspace, @Nullable ByteBuffer routingKey,
			@Nullable ConsistencyLevel serialConsistencyLevel, Duration timeout, @Nullable Boolean tracing) {

		this.consistencyLevel = consistencyLevel;
		this.executionProfileResolver = executionProfileResolver;
		this.idempotent = idempotent;
		this.keyspace = keyspace;
		this.pageSize = pageSize;
		this.routingKey = routingKey;
		this.routingKeyspace = routingKeyspace;
		this.serialConsistencyLevel = serialConsistencyLevel;
		this.timeout = timeout;
		this.tracing = tracing;
	}

	/**
	 * Create a new {@link QueryOptionsBuilder}.
	 *
	 * @return a new {@link QueryOptionsBuilder}.
	 * @since 1.5
	 */
	public static QueryOptionsBuilder builder() {
		return new QueryOptionsBuilder();
	}

	/**
	 * Create default {@link QueryOptions}.
	 *
	 * @return default {@link QueryOptions}.
	 * @since 2.0
	 */
	public static QueryOptions empty() {
		return EMPTY;
	}

	/**
	 * Create a new {@link QueryOptionsBuilder} to mutate properties of this {@link QueryOptions}.
	 *
	 * @return a new {@link QueryOptionsBuilder} initialized with this {@link QueryOptions}.
	 * @since 2.0
	 */
	public QueryOptionsBuilder mutate() {
		return new QueryOptionsBuilder(this);
	}

	/**
	 * @return the driver {@link ConsistencyLevel}.
	 * @since 1.5
	 */
	@Nullable
	protected ConsistencyLevel getConsistencyLevel() {
		return this.consistencyLevel;
	}

	/**
	 * @return the {@link ExecutionProfileResolver}.
	 * @since 3.0
	 */
	protected ExecutionProfileResolver getExecutionProfileResolver() {
		return this.executionProfileResolver;
	}

	/**
	 * @return whether query is idempotent. May be {@literal null} if not set.
	 * @since 3.4
	 * @see com.datastax.oss.driver.api.core.cql.Statement#setIdempotent(Boolean)
	 */
	@Nullable
	protected Boolean isIdempotent() {
		return this.idempotent;
	}

	/**
	 * @return the keyspace associated with the query. If it is {@literal null}, it means that either keyspace configured
	 *         on the statement or from the {@link CqlSession} will be used.
	 * @since 3.1
	 */
	@Nullable
	public CqlIdentifier getKeyspace() {
		return keyspace;
	}

	/**
	 * @return the number of rows to fetch per chunking request. May be {@literal null} if not set.
	 * @since 1.5
	 */
	@Nullable
	protected Integer getPageSize() {
		return this.pageSize;
	}

	/**
	 * @return the keyspace used for token-aware routing. May be {@literal null} if token-aware routing is disabled.
	 * @since 3.4
	 * @see com.datastax.oss.driver.api.core.cql.Statement#setRoutingKeyspace(CqlIdentifier)
	 */
	@Nullable
	protected CqlIdentifier getRoutingKeyspace() {
		return this.routingKeyspace;
	}

	/**
	 * @return the key used for token-aware routing. May be {@literal null} if token-aware routing is disabled.
	 * @since 3.4
	 * @see com.datastax.oss.driver.api.core.cql.Statement#setRoutingKey(ByteBuffer)
	 */
	@Nullable
	protected ByteBuffer getRoutingKey() {
		return this.routingKey;
	}

	/**
	 * @return the command timeout. May be {@link Duration#isNegative() negative} if not set.
	 * @since 3.0
	 * @see com.datastax.oss.driver.api.core.cql.Statement#setTimeout(Duration)
	 */
	protected Duration getTimeout() {
		return this.timeout;
	}

	/**
	 * @return the serial {@link ConsistencyLevel}.
	 * @since 3.0
	 * @see com.datastax.oss.driver.api.core.cql.Statement#setSerialConsistencyLevel(ConsistencyLevel)
	 */
	@Nullable
	protected ConsistencyLevel getSerialConsistencyLevel() {
		return this.serialConsistencyLevel;
	}

	/**
	 * @return whether to enable tracing. May be {@literal null} if not set.
	 */
	@Nullable
	protected Boolean getTracing() {
		return this.tracing;
	}

	@Override
	public boolean equals(@Nullable Object o) {

		if (this == o) {
			return true;
		}

		if (!(o instanceof QueryOptions options)) {
			return false;
		}

		if (!ObjectUtils.nullSafeEquals(consistencyLevel, options.consistencyLevel)) {
			return false;
		}

		if (!ObjectUtils.nullSafeEquals(executionProfileResolver, options.executionProfileResolver)) {
			return false;
		}

		if (!ObjectUtils.nullSafeEquals(idempotent, options.idempotent)) {
			return false;
		}

		if (!ObjectUtils.nullSafeEquals(pageSize, options.pageSize)) {
			return false;
		}

		if (!ObjectUtils.nullSafeEquals(routingKeyspace, options.routingKeyspace)) {
			return false;
		}

		if (!ObjectUtils.nullSafeEquals(routingKey, options.routingKey)) {
			return false;
		}

		if (!ObjectUtils.nullSafeEquals(serialConsistencyLevel, options.serialConsistencyLevel)) {
			return false;
		}

		if (!ObjectUtils.nullSafeEquals(timeout, options.timeout)) {
			return false;
		}

		if (!ObjectUtils.nullSafeEquals(tracing, options.tracing)) {
			return false;
		}

		return ObjectUtils.nullSafeEquals(keyspace, options.keyspace);
	}

	@Override
	public int hashCode() {
		int result = ObjectUtils.nullSafeHashCode(consistencyLevel);
		result = 31 * result + ObjectUtils.nullSafeHashCode(executionProfileResolver);
		result = 31 * result + ObjectUtils.nullSafeHashCode(idempotent);
		result = 31 * result + ObjectUtils.nullSafeHashCode(keyspace);
		result = 31 * result + ObjectUtils.nullSafeHashCode(pageSize);
		result = 31 * result + ObjectUtils.nullSafeHashCode(routingKeyspace);
		result = 31 * result + ObjectUtils.nullSafeHashCode(routingKey);
		result = 31 * result + ObjectUtils.nullSafeHashCode(serialConsistencyLevel);
		result = 31 * result + ObjectUtils.nullSafeHashCode(timeout);
		result = 31 * result + ObjectUtils.nullSafeHashCode(tracing);

		return result;
	}

	/**
	 * Builder for {@link QueryOptions}.
	 *
	 * @author Mark Paluch
	 * @author Seungho Kang
	 * @since 1.5
	 */
	public static class QueryOptionsBuilder {

		protected @Nullable ConsistencyLevel consistencyLevel;

		protected ExecutionProfileResolver executionProfileResolver = ExecutionProfileResolver.none();

		protected @Nullable Boolean idempotent;

		protected @Nullable CqlIdentifier keyspace;

		protected @Nullable ByteBuffer routingKey;

		protected @Nullable CqlIdentifier routingKeyspace;

		protected @Nullable Integer pageSize;

		protected @Nullable ConsistencyLevel serialConsistencyLevel;

		protected Duration timeout = Duration.ofMillis(-1);

		protected @Nullable Boolean tracing;

		QueryOptionsBuilder() {}

		QueryOptionsBuilder(QueryOptions queryOptions) {

			this.consistencyLevel = queryOptions.consistencyLevel;
			this.executionProfileResolver = queryOptions.executionProfileResolver;
			this.idempotent = queryOptions.idempotent;
			this.keyspace = queryOptions.keyspace;
			this.pageSize = queryOptions.pageSize;
			this.routingKey = queryOptions.routingKey;
			this.routingKeyspace = queryOptions.routingKeyspace;
			this.serialConsistencyLevel = queryOptions.serialConsistencyLevel;
			this.timeout = queryOptions.timeout;
			this.tracing = queryOptions.tracing;
		}

		/**
		 * Sets the {@link ConsistencyLevel} to use.
		 *
		 * @param consistencyLevel must not be {@literal null}.
		 * @return {@code this} {@link QueryOptionsBuilder}
		 */
		public QueryOptionsBuilder consistencyLevel(ConsistencyLevel consistencyLevel) {

			Assert.notNull(consistencyLevel, "ConsistencyLevel must not be null");

			this.consistencyLevel = consistencyLevel;

			return this;
		}

		/**
		 * Sets the {@code execution profile} to use.
		 *
		 * @param profileName must not be {@literal null} or empty.
		 * @return {@code this} {@link QueryOptionsBuilder}
		 * @since 3.0
		 * @see com.datastax.oss.driver.api.core.cql.Statement#setExecutionProfileName(String)
		 */
		public QueryOptionsBuilder executionProfile(String profileName) {
			return executionProfile(ExecutionProfileResolver.from(profileName));
		}

		/**
		 * Sets the {@link ExecutionProfileResolver} to use.
		 *
		 * @param executionProfileResolver must not be {@literal null}.
		 * @return {@code this} {@link QueryOptionsBuilder}
		 * @see com.datastax.oss.driver.api.core.cql.Statement#setExecutionProfile(DriverExecutionProfile)
		 */
		public QueryOptionsBuilder executionProfile(ExecutionProfileResolver executionProfileResolver) {

			Assert.notNull(executionProfileResolver, "ExecutionProfileResolver must not be null");

			this.executionProfileResolver = executionProfileResolver;

			return this;
		}

		/**
		 * Set query execution idempotency.
		 *
		 * @param idempotent {@literal true} to mark the query as idempotent.
		 * @return {@code this} {@link QueryOptionsBuilder}.
		 * @since 3.4
		 */
		public QueryOptionsBuilder idempotent(boolean idempotent) {

			this.idempotent = idempotent;

			return this;
		}

		/**
		 * Sets the {@link CqlIdentifier keyspace} to use. If left unconfigured, then the keyspace set on the statement or
		 * {@link CqlSession} will be used.
		 *
		 * @param keyspace the specific keyspace to use to run a statement, must not be {@literal null}.
		 * @return {@code this} {@link QueryOptionsBuilder}.
		 * @since 3.1
		 */
		public QueryOptionsBuilder keyspace(CqlIdentifier keyspace) {

			Assert.notNull(keyspace, "Keyspace must not be null");

			this.keyspace = keyspace;

			return this;
		}

		/**
		 * Sets the query fetch size for {@link com.datastax.oss.driver.api.core.cql.ResultSet} chunks.
		 * <p>
		 * The fetch size controls how much resulting rows will be retrieved simultaneously (the goal being to avoid loading
		 * too many results in memory for queries yielding large results). Please note that while value as low as 1 can be
		 * used, it is <strong>highly</strong> discouraged to use such a low value in practice as it will yield very poor
		 * performance.
		 *
		 * @param pageSize the number of rows to fetch per chunking request. To disable chunking of the result set, use
		 *          {@code pageSize == Integer.MAX_VALUE}. Negative values are not allowed.
		 * @return {@code this} {@link QueryOptionsBuilder}
		 */
		public QueryOptionsBuilder pageSize(int pageSize) {

			Assert.isTrue(pageSize >= 0, "Page size must be greater than equal to zero");

			this.pageSize = pageSize;

			return this;
		}

		/**
		 * Set query routing keyspace.
		 *
		 * @param routingKeyspace the routing keyspace to use for token-aware routing.
		 * @return {@code this} {@link QueryOptionsBuilder}.
		 * @since 3.4
		 */
		public QueryOptionsBuilder routingKeyspace(CqlIdentifier routingKeyspace) {

			Assert.notNull(routingKeyspace, "Routing keyspace must not be null");

			this.routingKeyspace = routingKeyspace;

			return this;
		}

		/**
		 * Set query routing key.
		 *
		 * @param routingKey the routing key to use for token-aware routing.
		 * @return {@code this} {@link QueryOptionsBuilder}
		 * @since 3.4
		 */
		public QueryOptionsBuilder routingKey(ByteBuffer routingKey) {

			Assert.notNull(routingKey, "Routing key must not be null");

			this.routingKey = routingKey;

			return this;
		}

		/**
		 * Sets the serial {@link ConsistencyLevel} to use.
		 *
		 * @param consistencyLevel must not be {@literal null}.
		 * @return {@code this} {@link QueryOptionsBuilder}
		 */
		public QueryOptionsBuilder serialConsistencyLevel(ConsistencyLevel consistencyLevel) {

			Assert.notNull(consistencyLevel, "Serial ConsistencyLevel must not be null");

			this.serialConsistencyLevel = consistencyLevel;

			return this;
		}

		/**
		 * Sets the request timeout. Overrides the default timeout.
		 *
		 * @param timeout the read timeout. Negative values are not allowed. If it is {@code 0}, the read timeout will be
		 *          disabled for this statement.
		 * @return {@code this} {@link QueryOptionsBuilder}
		 * @see com.datastax.oss.driver.api.core.cql.SimpleStatement#setTimeout(Duration)
		 * @since 3.0
		 */
		public QueryOptionsBuilder timeout(Duration timeout) {

			Assert.isTrue(!timeout.isNegative(), "ReadTimeout must be greater than equal to zero");

			this.timeout = timeout;

			return this;
		}

		/**
		 * Enables statement tracing.
		 *
		 * @param tracing {@literal true} to enable statement tracing to the executed statements.
		 * @return {@code this} {@link QueryOptionsBuilder}
		 */
		public QueryOptionsBuilder tracing(boolean tracing) {

			this.tracing = tracing;

			return this;
		}

		/**
		 * Enables statement tracing.
		 *
		 * @return {@code this} {@link QueryOptionsBuilder}
		 */
		public QueryOptionsBuilder withTracing() {
			return tracing(true);
		}

		/**
		 * Builds a new {@link QueryOptions} with the configured values.
		 *
		 * @return a new {@link QueryOptions} with the configured values
		 */
		public QueryOptions build() {
			return new QueryOptions(this.consistencyLevel, this.executionProfileResolver, this.idempotent, this.keyspace,
					this.pageSize, this.routingKeyspace, this.routingKey, this.serialConsistencyLevel, this.timeout,
					this.tracing);
		}

	}

}
