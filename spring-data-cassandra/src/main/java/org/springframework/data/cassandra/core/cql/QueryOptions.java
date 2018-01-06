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

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import lombok.EqualsAndHashCode;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.RetryPolicy;

/**
 * Cassandra Query Options for queries. {@link QueryOptions} allow tuning of various query options on a per-request
 * level. Only options that are set are applied to queries.
 *
 * @author David Webb
 * @author Mark Paluch
 */
@EqualsAndHashCode
public class QueryOptions {

	private static final QueryOptions EMPTY = QueryOptions.builder().build();

	private final @Nullable Boolean tracing;

	private final @Nullable ConsistencyLevel consistencyLevel;

	private final Duration readTimeout;

	private final @Nullable Integer fetchSize;

	private final @Nullable RetryPolicy retryPolicy;

	protected QueryOptions(@Nullable ConsistencyLevel consistencyLevel, @Nullable RetryPolicy retryPolicy,
			@Nullable Boolean tracing, @Nullable Integer fetchSize, Duration readTimeout) {

		this.consistencyLevel = consistencyLevel;
		this.retryPolicy = retryPolicy;
		this.tracing = tracing;
		this.fetchSize = fetchSize;
		this.readTimeout = readTimeout;
	}

	/**
	 * Creates new {@link QueryOptions} for the given {@link ConsistencyLevel} and {@link RetryPolicy}.
	 *
	 * @param consistencyLevel the consistency level, may be {@literal null}.
	 * @param retryPolicy the retry policy, may be {@literal null}.
	 * @deprecated since 2.0, use {@link #builder()}.
	 */
	@Deprecated
	public QueryOptions(@Nullable ConsistencyLevel consistencyLevel, @Nullable RetryPolicy retryPolicy) {

		this.consistencyLevel = consistencyLevel;
		this.retryPolicy = retryPolicy;
		this.tracing = false;
		this.fetchSize = null;
		this.readTimeout = Duration.ofMillis(-1);
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
	 * @return the the driver {@link ConsistencyLevel}
	 * @since 1.5
	 */
	@Nullable
	protected ConsistencyLevel getConsistencyLevel() {
		return this.consistencyLevel;
	}

	/**
	 * @return the number of rows to fetch per chunking request. May be {@literal null} if not set.
	 * @since 1.5
	 */
	@Nullable
	protected Integer getFetchSize() {
		return this.fetchSize;
	}

	/**
	 * @return the read timeout in milliseconds. May be {@literal null} if not set.
	 * @since 1.5
	 */
	protected Duration getReadTimeout() {
		return this.readTimeout;
	}

	/**
	 * @return the driver {@link RetryPolicy}
	 * @since 1.5
	 */
	@Nullable
	protected RetryPolicy getRetryPolicy() {
		return this.retryPolicy;
	}

	/**
	 * @return whether to enable tracing. May be {@literal null} if not set.
	 */
	@Nullable
	protected Boolean getTracing() {
		return this.tracing;
	}

	/**
	 * Builder for {@link QueryOptions}.
	 *
	 * @author Mark Paluch
	 * @since 1.5
	 */
	public static class QueryOptionsBuilder {

		protected @Nullable Boolean tracing;

		protected @Nullable ConsistencyLevel consistencyLevel;

		protected Duration readTimeout = Duration.ofMillis(-1);

		protected @Nullable Integer fetchSize;

		protected @Nullable RetryPolicy retryPolicy;

		QueryOptionsBuilder() {}

		QueryOptionsBuilder(QueryOptions queryOptions) {

			this.consistencyLevel = queryOptions.consistencyLevel;
			this.fetchSize = queryOptions.fetchSize;
			this.readTimeout = queryOptions.readTimeout;
			this.retryPolicy = queryOptions.retryPolicy;
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
		 * Sets the {@link RetryPolicy driver RetryPolicy} to use. Setting both ( {@link RetryPolicy} and {@link RetryPolicy
		 * driver RetryPolicy}) retry policies is not supported.
		 *
		 * @param retryPolicy must not be {@literal null}.
		 * @return {@code this} {@link QueryOptionsBuilder}
		 */
		public QueryOptionsBuilder retryPolicy(RetryPolicy retryPolicy) {

			Assert.notNull(retryPolicy, "RetryPolicy must not be null");

			this.retryPolicy = retryPolicy;

			return this;
		}

		/**
		 * Sets the query fetch size for {@link com.datastax.driver.core.ResultSet} chunks.
		 * <p>
		 * The fetch size controls how much resulting rows will be retrieved simultaneously (the goal being to avoid loading
		 * too much results in memory for queries yielding large results). Please note that while value as low as 1 can be
		 * used, it is *highly* discouraged to use such a low value in practice as it will yield very poor performance.
		 *
		 * @param fetchSize the number of rows to fetch per chunking request. To disable chunking of the result set, use
		 *          {@code fetchSize == Integer.MAX_VALUE}. Negative values are not allowed.
		 * @return {@code this} {@link QueryOptionsBuilder}
		 * @see com.datastax.driver.core.QueryOptions#getFetchSize()
		 * @see com.datastax.driver.core.Cluster.Builder#withQueryOptions(com.datastax.driver.core.QueryOptions)
		 */
		public QueryOptionsBuilder fetchSize(int fetchSize) {

			Assert.isTrue(fetchSize >= 0, "FetchSize must be greater than equal to zero");

			this.fetchSize = fetchSize;

			return this;
		}

		/**
		 * Sets the read timeout in milliseconds. Overrides the default per-host read timeout.
		 *
		 * @param readTimeout the read timeout in milliseconds. Negative values are not allowed. If it is {@code 0}, the
		 *          read timeout will be disabled for this statement.
		 * @return {@code this} {@link QueryOptionsBuilder}
		 * @see SocketOptions#getReadTimeoutMillis()
		 * @see com.datastax.driver.core.Cluster.Builder#withSocketOptions(SocketOptions)
		 */
		public QueryOptionsBuilder readTimeout(long readTimeout) {
			return readTimeout(Duration.ofMillis(readTimeout));
		}

		/**
		 * Sets the read timeout. Overrides the default per-host read timeout.
		 *
		 * @param readTimeout the read timeout value. Negative values are not allowed. If it is {@code 0}, the read timeout
		 *          will be disabled for this statement.
		 * @param timeUnit the {@link TimeUnit} for the supplied timeout; must not be {@literal null}.
		 * @return {@code this} {@link QueryOptionsBuilder}
		 * @see SocketOptions#getReadTimeoutMillis()
		 * @see com.datastax.driver.core.Cluster.Builder#withSocketOptions(SocketOptions)
		 * @deprecated since 2.0, use {@link #readTimeout(Duration)}.
		 */
		@Deprecated
		public QueryOptionsBuilder readTimeout(long readTimeout, TimeUnit timeUnit) {

			Assert.isTrue(readTimeout >= 0, "ReadTimeout must be greater than equal to zero");
			Assert.notNull(timeUnit, "TimeUnit must not be null");

			return readTimeout(Duration.ofMillis(timeUnit.toMillis(readTimeout)));
		}

		/**
		 * Sets the read timeout. Overrides the default per-host read timeout.
		 *
		 * @param readTimeout the read timeout. Negative values are not allowed. If it is {@code 0}, the read timeout will
		 *          be disabled for this statement.
		 * @return {@code this} {@link QueryOptionsBuilder}
		 * @see SocketOptions#getReadTimeoutMillis()
		 * @see com.datastax.driver.core.Cluster.Builder#withSocketOptions(SocketOptions)
		 * @since 2.0
		 */
		public QueryOptionsBuilder readTimeout(Duration readTimeout) {

			Assert.isTrue(!readTimeout.isZero() && !readTimeout.isNegative(),
					"ReadTimeout must be greater than equal to zero");

			this.readTimeout = readTimeout;

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
			return new QueryOptions(this.consistencyLevel, this.retryPolicy, this.tracing,
					this.fetchSize, this.readTimeout);
		}
	}
}
