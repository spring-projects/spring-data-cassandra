/*
 * Copyright 2013-2017 the original author or authors.
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

import java.util.Optional;
import java.util.concurrent.TimeUnit;

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
public class QueryOptions {

	private ConsistencyLevel consistencyLevel;

	private RetryPolicy retryPolicy;

	private Boolean tracing;

	private Integer fetchSize;

	private Long readTimeout;

	/**
	 * Creates new {@link QueryOptions}.
	 */
	public QueryOptions() {}

	/**
	 * Creates new {@link QueryOptions} for the given {@link ConsistencyLevel} and {@link RetryPolicy}.
	 *
	 * @param consistencyLevel the consistency level, may be {@literal null}.
	 * @param retryPolicy the retry policy, may be {@literal null}.
	 */
	public QueryOptions(ConsistencyLevel consistencyLevel, RetryPolicy retryPolicy) {
		setConsistencyLevel(consistencyLevel);
		setRetryPolicy(retryPolicy);
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
	 * Sets the driver {@link ConsistencyLevel}. Setting both ({@link ConsistencyLevel} and {@link ConsistencyLevel driver
	 * ConsistencyLevel}) consistency levels is not supported.
	 *
	 * @param consistencyLevel the driver {@link ConsistencyLevel} to set.
	 * @since 1.5
	 */
	public void setConsistencyLevel(ConsistencyLevel consistencyLevel) {
		this.consistencyLevel = consistencyLevel;
	}

	/**
	 * @return the the driver {@link ConsistencyLevel}
	 * @since 1.5
	 */
	protected ConsistencyLevel getConsistencyLevel() {
		return this.consistencyLevel;
	}

	/**
	 * Sets the {@link RetryPolicy}. Setting both ({@link RetryPolicy} and {@link RetryPolicy driver RetryPolicy}) retry
	 * policies is not supported.
	 *
	 * @param retryPolicy the driver {@link RetryPolicy} to set.
	 * @since 1.5
	 * @throws IllegalStateException if the {@link RetryPolicy} is set
	 */
	public void setRetryPolicy(RetryPolicy retryPolicy) {
		this.retryPolicy = retryPolicy;
	}

	/**
	 * @return the driver {@link RetryPolicy}
	 * @since 1.5
	 */
	protected RetryPolicy getRetryPolicy() {
		return this.retryPolicy;
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
	 * @since 1.5
	 * @see com.datastax.driver.core.QueryOptions#getFetchSize()
	 * @see com.datastax.driver.core.Cluster.Builder#withQueryOptions(com.datastax.driver.core.QueryOptions)
	 */
	public void setFetchSize(int fetchSize) {

		Assert.isTrue(fetchSize >= 0, "FetchSize must be greater than equal to zero");

		this.fetchSize = fetchSize;
	}

	/**
	 * @return the number of rows to fetch per chunking request. May be {@literal null} if not set.
	 * @since 1.5
	 */
	protected Integer getFetchSize() {
		return fetchSize;
	}

	/**
	 * Sets the read timeout in milliseconds. Overrides the default per-host read timeout (
	 * {@link SocketOptions#getReadTimeoutMillis()}).
	 *
	 * @param readTimeout the read timeout in milliseconds. Negative values are not allowed. If it is {@code 0}, the read
	 *          timeout will be disabled for this statement.
	 * @since 1.5
	 * @see SocketOptions#getReadTimeoutMillis()
	 * @see com.datastax.driver.core.Cluster.Builder#withSocketOptions(SocketOptions)
	 */
	public void setReadTimeout(long readTimeout) {

		Assert.isTrue(readTimeout >= 0, "ReadTimeout must be greater than equal to zero");

		this.readTimeout = readTimeout;
	}

	/**
	 * @return the read timeout in milliseconds. May be {@literal null} if not set.
	 * @since 1.5
	 */
	protected Long getReadTimeout() {
		return this.readTimeout;
	}

	/**
	 * Enables statement tracing.
	 *
	 * @param tracing {@literal true} to enable statement tracing to the executed statements.
	 * @since 1.5
	 */
	public void setTracing(boolean tracing) {
		this.tracing = tracing;
	}

	/**
	 * @return whether to enable tracing. May be {@literal null} if not set.
	 */
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

		private ConsistencyLevel consistencyLevel;

		private RetryPolicy retryPolicy;

		private Boolean tracing;

		private Integer fetchSize;

		private Long readTimeout;

		QueryOptionsBuilder() {}

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

			Assert.isTrue(readTimeout >= 0, "ReadTimeout must be greater than equal to zero");

			this.readTimeout = readTimeout;

			return this;
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
		 */
		public QueryOptionsBuilder readTimeout(long readTimeout, TimeUnit timeUnit) {

			Assert.isTrue(readTimeout >= 0, "ReadTimeout must be greater than equal to zero");
			Assert.notNull(timeUnit, "TimeUnit must not be null");

			this.readTimeout = timeUnit.toMillis(readTimeout);

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
			return applyOptions(new QueryOptions());
		}

		protected <T> T applyOptions(T options) {

			QueryOptions queryOptions = (QueryOptions) options;

			queryOptions.setConsistencyLevel(consistencyLevel);
			queryOptions.setRetryPolicy(retryPolicy);

			Optional.ofNullable(this.fetchSize).ifPresent(queryOptions::setFetchSize);
			Optional.ofNullable(this.readTimeout).ifPresent(queryOptions::setReadTimeout);
			Optional.ofNullable(this.tracing).ifPresent(queryOptions::setTracing);

			return options;
		}
	}
}
