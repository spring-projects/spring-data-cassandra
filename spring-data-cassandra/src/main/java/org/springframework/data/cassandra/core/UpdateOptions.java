/*
 * Copyright 2017-2018 the original author or authors.
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

import lombok.EqualsAndHashCode;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

import org.springframework.data.cassandra.core.cql.WriteOptions;
import org.springframework.lang.Nullable;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.policies.RetryPolicy;

/**
 * Extension to {@link WriteOptions} for use with {@code UPDATE} operations.
 *
 * @author Mark Paluch
 * @author Lukasz Antoniak
 * @since 2.0
 */
@EqualsAndHashCode(callSuper = true)
public class UpdateOptions extends WriteOptions {

	private static final UpdateOptions EMPTY = new UpdateOptionsBuilder().build();

	private boolean ifExists;

	private UpdateOptions(@Nullable ConsistencyLevel consistencyLevel, @Nullable RetryPolicy retryPolicy,
			@Nullable Boolean tracing, @Nullable Integer fetchSize, Duration readTimeout, Duration ttl,
			@Nullable Long timestamp, boolean ifExists) {

		super(consistencyLevel, retryPolicy, tracing, fetchSize, readTimeout, ttl, timestamp);

		this.ifExists = ifExists;
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
	 * Create default {@link UpdateOptions}.
	 *
	 * @return default {@link UpdateOptions}.
	 * @since 2.0
	 */
	public static UpdateOptions empty() {
		return EMPTY;
	}

	/**
	 * Create a new {@link UpdateOptionsBuilder} to mutate properties of this {@link UpdateOptions}.
	 *
	 * @return a new {@link UpdateOptionsBuilder} initialized with this {@link UpdateOptions}.
	 */
	@Override
	public UpdateOptionsBuilder mutate() {
		return new UpdateOptionsBuilder(this);
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
	 * @author Lukasz Antoniak
	 * @since 2.0
	 */
	public static class UpdateOptionsBuilder extends WriteOptionsBuilder {

		private boolean ifExists;

		private UpdateOptionsBuilder() {}

		private UpdateOptionsBuilder(UpdateOptions updateOptions) {

			super(updateOptions);

			this.ifExists = updateOptions.ifExists;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.WriteOptions.WriteOptionsBuilder#consistencyLevel(com.datastax.driver.core.ConsistencyLevel)
		 */
		@Override
		public UpdateOptionsBuilder consistencyLevel(com.datastax.driver.core.ConsistencyLevel consistencyLevel) {

			super.consistencyLevel(consistencyLevel);
			return this;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.WriteOptions.WriteOptionsBuilder#retryPolicy(com.datastax.driver.core.policies.RetryPolicy)
		 */
		@Override
		public UpdateOptionsBuilder retryPolicy(com.datastax.driver.core.policies.RetryPolicy driverRetryPolicy) {

			super.retryPolicy(driverRetryPolicy);
			return this;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.WriteOptions.WriteOptionsBuilder#fetchSize(int)
		 */
		@Override
		public UpdateOptionsBuilder fetchSize(int fetchSize) {

			super.fetchSize(fetchSize);
			return this;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.WriteOptions.WriteOptionsBuilder#readTimeout(long)
		 */
		@Override
		public UpdateOptionsBuilder readTimeout(long readTimeout) {

			super.readTimeout(readTimeout);
			return this;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.WriteOptions.WriteOptionsBuilder#readTimeout(long, java.util.concurrent.TimeUnit)
		 */
		@Override
		@Deprecated
		public UpdateOptionsBuilder readTimeout(long readTimeout, TimeUnit timeUnit) {

			super.readTimeout(readTimeout, timeUnit);
			return this;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.WriteOptions.WriteOptionsBuilder#readTimeout(java.time.Duration)
		 */
		@Override
		public UpdateOptionsBuilder readTimeout(Duration readTimeout) {

			super.readTimeout(readTimeout);
			return this;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.WriteOptions.WriteOptionsBuilder#ttl(java.time.Duration)
		 */
		@Override
		public UpdateOptionsBuilder ttl(Duration ttl) {

			super.ttl(ttl);
			return this;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.WriteOptions.WriteOptionsBuilder#tracing(boolean)
		 */
		@Override
		public UpdateOptionsBuilder tracing(boolean tracing) {

			super.tracing(tracing);
			return this;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.WriteOptions.WriteOptionsBuilder#withTracing()
		 */
		@Override
		public UpdateOptionsBuilder withTracing() {

			super.withTracing();
			return this;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.WriteOptions.WriteOptionsBuilder#ttl(int)
		 */
		public UpdateOptionsBuilder ttl(int ttl) {

			super.ttl(ttl);
			return this;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.WriteOptions.WriteOptionsBuilder#timestamp(long)
		 */
		@Override
		public UpdateOptionsBuilder timestamp(long timestamp) {

			super.timestamp(timestamp);
			return this;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.WriteOptions.WriteOptionsBuilder#timestamp(java.time.Instant)
		 */
		@Override
		public UpdateOptionsBuilder timestamp(Instant timestamp) {

			super.timestamp(timestamp);
			return this;
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
			return new UpdateOptions(this.consistencyLevel, this.retryPolicy, this.tracing, this.fetchSize, this.readTimeout,
					this.ttl, this.timestamp, this.ifExists);
		}
	}
}
