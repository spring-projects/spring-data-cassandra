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
 * Extension to {@link WriteOptions} for use with {@code UPDATE} operations.
 *
 * @author Mark Paluch
 * @since 2.0
 */
public class UpdateOptions extends WriteOptions {

	private static final UpdateOptions EMPTY = new UpdateOptionsBuilder().build();

	private boolean ifExists;

	private UpdateOptions(@Nullable ConsistencyLevel consistencyLevel, @Nullable RetryPolicy retryPolicy,
			@Nullable Boolean tracing, @Nullable Integer fetchSize, Duration readTimeout, Duration ttl, boolean ifExists) {

		super(consistencyLevel, retryPolicy, tracing, fetchSize, readTimeout, ttl);
		this.ifExists = ifExists;
	}

	/**
	 * Create default {@link UpdateOptions}.
	 *
	 * @return default {@link UpdateOptions}.
	 * @since 2.0
	 */
	public static UpdateOptions empty() {
		return EMPTY;
	}

	/**
	 * Create a new {@link UpdateOptionsBuilder}.
	 *
	 * @return a new {@link UpdateOptionsBuilder}.
	 */
	public static UpdateOptionsBuilder builder() {
		return new UpdateOptionsBuilder();
	}

	/**
	 * @return {@literal true} to apply {@code IF EXISTS} to {@code UPDATE} operations.
	 */
	public boolean isIfExists() {
		return this.ifExists;
	}

	/**
	 * Builder for {@link UpdateOptions}.
	 *
	 * @author Mark Paluch
	 * @since 2.0
	 */
	public static class UpdateOptionsBuilder extends WriteOptionsBuilder {

		private boolean ifExists;

		private UpdateOptionsBuilder() {}

		@Override
		public UpdateOptionsBuilder consistencyLevel(com.datastax.driver.core.ConsistencyLevel consistencyLevel) {
			return (UpdateOptionsBuilder) super.consistencyLevel(consistencyLevel);
		}

		@Override
		public UpdateOptionsBuilder retryPolicy(com.datastax.driver.core.policies.RetryPolicy driverRetryPolicy) {
			return (UpdateOptionsBuilder) super.retryPolicy(driverRetryPolicy);
		}

		@Override
		public UpdateOptionsBuilder fetchSize(int fetchSize) {
			return (UpdateOptionsBuilder) super.fetchSize(fetchSize);
		}

		@Override
		public UpdateOptionsBuilder readTimeout(long readTimeout) {
			return (UpdateOptionsBuilder) super.readTimeout(readTimeout);
		}

		@Override
		@Deprecated
		public UpdateOptionsBuilder readTimeout(long readTimeout, TimeUnit timeUnit) {
			return (UpdateOptionsBuilder) super.readTimeout(readTimeout, timeUnit);
		}

		@Override
		public UpdateOptionsBuilder readTimeout(Duration readTimeout) {
			return (UpdateOptionsBuilder) super.readTimeout(readTimeout);
		}

		@Override
		public UpdateOptionsBuilder ttl(Duration ttl) {
			return (UpdateOptionsBuilder) super.ttl(ttl);
		}

		@Override
		public UpdateOptionsBuilder tracing(boolean tracing) {
			return (UpdateOptionsBuilder) super.tracing(tracing);
		}

		@Override
		public UpdateOptionsBuilder withTracing() {
			return (UpdateOptionsBuilder) super.withTracing();
		}

		public UpdateOptionsBuilder ttl(int ttl) {
			return (UpdateOptionsBuilder) super.ttl(ttl);
		}

		/**
		 * Use light-weight transactions by applying {@code IF EXISTS}.
		 *
		 * @return {@code this} {@link UpdateOptionsBuilder}
		 */
		public UpdateOptionsBuilder withIfExists() {
			return ifExists(true);
		}

		/**
		 * Use light-weight transactions by applying {@code IF EXISTS}.
		 *
		 * @param ifNotExists {@literal true} to enable {@code IF EXISTS}.
		 * @return {@code this} {@link UpdateOptionsBuilder}
		 */
		public UpdateOptionsBuilder ifExists(boolean ifNotExists) {

			this.ifExists = ifNotExists;

			return this;
		}

		/**
		 * Builds a new {@link UpdateOptions} with the configured values.
		 *
		 * @return a new {@link UpdateOptions} with the configured values
		 */
		public UpdateOptions build() {
			return new UpdateOptions(consistencyLevel, retryPolicy, tracing, fetchSize, readTimeout, ttl, ifExists);
		}
	}
}
