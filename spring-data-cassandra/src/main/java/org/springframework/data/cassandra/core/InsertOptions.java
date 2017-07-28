/*
 * Copyright 2017 the original author or authors.
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
package org.springframework.data.cassandra.core;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.springframework.data.cassandra.core.cql.WriteOptions;
import org.springframework.lang.Nullable;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.policies.RetryPolicy;

/**
 * Extension to {@link WriteOptions} for use with {@code INSERT} operations.
 *
 * @author Mark Paluch
 * @since 2.0
 */
public class InsertOptions extends WriteOptions {

	private static final InsertOptions EMPTY = new InsertOptionsBuilder().build();

	private boolean ifNotExists;

	private InsertOptions(@Nullable ConsistencyLevel consistencyLevel, @Nullable RetryPolicy retryPolicy,
			@Nullable Boolean tracing, @Nullable Integer fetchSize, Duration readTimeout, Duration ttl, boolean ifNotExists) {

		super(consistencyLevel, retryPolicy, tracing, fetchSize, readTimeout, ttl);
		this.ifNotExists = ifNotExists;
	}

	/**
	 * Create default {@link InsertOptions}.
	 *
	 * @return default {@link InsertOptions}.
	 * @since 2.0
	 */
	public static InsertOptions empty() {
		return EMPTY;
	}

	/**
	 * Create a new {@link InsertOptionsBuilder}.
	 *
	 * @return a new {@link InsertOptionsBuilder}.
	 */
	public static InsertOptionsBuilder builder() {
		return new InsertOptionsBuilder();
	}

	/**
	 * @return {@literal true} to apply {@code IF NOT EXISTS} to {@code INSERT} operations.
	 */
	public boolean isIfNotExists() {
		return this.ifNotExists;
	}

	/**
	 * Builder for {@link InsertOptions}.
	 *
	 * @author Mark Paluch
	 * @since 2.0
	 */
	public static class InsertOptionsBuilder extends WriteOptionsBuilder {

		private boolean ifNotExists;

		private InsertOptionsBuilder() {}

		@Override
		public InsertOptionsBuilder consistencyLevel(com.datastax.driver.core.ConsistencyLevel consistencyLevel) {
			return (InsertOptionsBuilder) super.consistencyLevel(consistencyLevel);
		}

		@Override
		public InsertOptionsBuilder retryPolicy(com.datastax.driver.core.policies.RetryPolicy driverRetryPolicy) {
			return (InsertOptionsBuilder) super.retryPolicy(driverRetryPolicy);
		}

		@Override
		public InsertOptionsBuilder fetchSize(int fetchSize) {
			return (InsertOptionsBuilder) super.fetchSize(fetchSize);
		}

		@Override
		public InsertOptionsBuilder readTimeout(long readTimeout) {
			return (InsertOptionsBuilder) super.readTimeout(readTimeout);
		}

		@Override
		@Deprecated
		public InsertOptionsBuilder readTimeout(long readTimeout, TimeUnit timeUnit) {
			return (InsertOptionsBuilder) super.readTimeout(readTimeout, timeUnit);
		}

		@Override
		public InsertOptionsBuilder readTimeout(Duration readTimeout) {
			return (InsertOptionsBuilder) super.readTimeout(readTimeout);
		}

		@Override
		public InsertOptionsBuilder ttl(Duration ttl) {
			return (InsertOptionsBuilder) super.ttl(ttl);
		}

		@Override
		public InsertOptionsBuilder tracing(boolean tracing) {
			return (InsertOptionsBuilder) super.tracing(tracing);
		}

		@Override
		public InsertOptionsBuilder withTracing() {
			return (InsertOptionsBuilder) super.withTracing();
		}

		public InsertOptionsBuilder ttl(int ttl) {
			return (InsertOptionsBuilder) super.ttl(ttl);
		}

		/**
		 * Use light-weight transactions by applying {@code IF NOT EXISTS}.
		 *
		 * @return {@code this} {@link InsertOptionsBuilder}
		 */
		public InsertOptionsBuilder withIfNotExists() {
			return ifNotExists(true);
		}

		/**
		 * Use light-weight transactions by applying {@code IF NOT EXISTS}.
		 *
		 * @param ifNotExists {@literal true} to enable {@code IF NOT EXISTS}.
		 * @return {@code this} {@link InsertOptionsBuilder}
		 */
		public InsertOptionsBuilder ifNotExists(boolean ifNotExists) {

			this.ifNotExists = ifNotExists;

			return this;
		}

		/**
		 * Builds a new {@link InsertOptions} with the configured values.
		 *
		 * @return a new {@link InsertOptions} with the configured values
		 */
		public InsertOptions build() {
			return new InsertOptions(consistencyLevel, retryPolicy, tracing, fetchSize, readTimeout, ttl, ifNotExists);
		}
	}
}
