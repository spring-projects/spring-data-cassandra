/*
 * Copyright 2013-2017 the original author or authors
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

import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.policies.RetryPolicy;

/**
 * Cassandra Write Options are an extension to {@link QueryOptions} for write operations. {@link WriteOptions}allow
 * tuning of various query options on a per-request level. Only options that are set are applied to queries.
 *
 * @author David Webb
 * @author Mark Paluch
 * @see QueryOptions
 */
public class WriteOptions extends QueryOptions {

	private Integer ttl;

	/**
	 * Creates new {@link WriteOptions}.
	 */
	public WriteOptions() {}

	/**
	 * Creates new {@link WriteOptions} for the given {@link ConsistencyLevel} and {@link RetryPolicy}.
	 *
	 * @param consistencyLevel the consistency level, may be {@literal null}.
	 * @param retryPolicy the retry policy, may be {@literal null}.
	 */
	public WriteOptions(ConsistencyLevel consistencyLevel, RetryPolicy retryPolicy) {
		this(consistencyLevel, retryPolicy, null);
	}

	/**
	 * Creates new {@link WriteOptions} for the given {@link ConsistencyLevel}, {@link RetryPolicy} and {@code ttl}.
	 *
	 * @param consistencyLevel the consistency level, may be {@literal null}.
	 * @param retryPolicy the retry policy, may be {@literal null}.
	 * @param ttl the ttl, may be {@literal null}.
	 */
	public WriteOptions(ConsistencyLevel consistencyLevel, RetryPolicy retryPolicy, Integer ttl) {
		super(consistencyLevel, retryPolicy);
		setTtl(ttl);
	}

	/**
	 * Create a new {@link WriteOptionsBuilder}.
	 *
	 * @return a new {@link WriteOptionsBuilder}.
	 * @since 1.5
	 */
	public static WriteOptionsBuilder builder() {
		return new WriteOptionsBuilder();
	}

	/**
	 * @return the time to live, if set.
	 */
	public Integer getTtl() {
		return this.ttl;
	}

	/**
	 * Sets the time to live for write operations.
	 *
	 * @param ttl the ttl to set.
	 */
	public void setTtl(Integer ttl) {
		this.ttl = ttl;
	}

	/**
	 * Builder for {@link QueryOptions}.
	 *
	 * @author Mark Paluch
	 * @since 1.5
	 */
	public static class WriteOptionsBuilder extends QueryOptionsBuilder {

		private Integer ttl;

		protected WriteOptionsBuilder() {}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.QueryOptions.QueryOptionsBuilder#consistencyLevel(com.datastax.driver.core.ConsistencyLevel)
		 */
		@Override
		public WriteOptionsBuilder consistencyLevel(com.datastax.driver.core.ConsistencyLevel consistencyLevel) {
			return (WriteOptionsBuilder) super.consistencyLevel(consistencyLevel);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.QueryOptions.QueryOptionsBuilder#retryPolicy(org.springframework.data.cassandra.core.cql.RetryPolicy)
		 */
		@Override
		public WriteOptionsBuilder retryPolicy(com.datastax.driver.core.policies.RetryPolicy driverRetryPolicy) {
			return (WriteOptionsBuilder) super.retryPolicy(driverRetryPolicy);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.QueryOptions.QueryOptionsBuilder#fetchSize(int)
		 */
		@Override
		public WriteOptionsBuilder fetchSize(int fetchSize) {
			return (WriteOptionsBuilder) super.fetchSize(fetchSize);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.QueryOptions.QueryOptionsBuilder#readTimeout(long)
		 */
		@Override
		public WriteOptionsBuilder readTimeout(long readTimeout) {
			return (WriteOptionsBuilder) super.readTimeout(readTimeout);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.QueryOptions.QueryOptionsBuilder#readTimeout(long, java.util.concurrent.TimeUnit)
		 */
		@Override
		public WriteOptionsBuilder readTimeout(long readTimeout, TimeUnit timeUnit) {
			return (WriteOptionsBuilder) super.readTimeout(readTimeout, timeUnit);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.QueryOptions.QueryOptionsBuilder#tracing(boolean)
		 */
		@Override
		public WriteOptionsBuilder tracing(boolean tracing) {
			return (WriteOptionsBuilder) super.tracing(tracing);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.QueryOptions.QueryOptionsBuilder#withTracing()
		 */
		@Override
		public WriteOptionsBuilder withTracing() {
			return (WriteOptionsBuilder) super.withTracing();
		}

		/**
		 * Sets the time to live for write operations.
		 *
		 * @param ttl the time to live.
		 * @return {@code this} {@link WriteOptionsBuilder}
		 */
		public WriteOptionsBuilder ttl(int ttl) {
			this.ttl = ttl;
			return this;
		}

		/**
		 * Builds a new {@link WriteOptions} with the configured values.
		 *
		 * @return a new {@link WriteOptions} with the configured values
		 */
		public WriteOptions build() {
			return applyOptions(new WriteOptions());
		}

		@Override
		protected <T> T applyOptions(T queryOptions) {

			WriteOptions writeOptions = (WriteOptions) queryOptions;

			writeOptions.setTtl(this.ttl);

			return super.applyOptions(queryOptions);
		}
	}
}
