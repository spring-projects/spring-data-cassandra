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
 * Extension to {@link WriteOptions} for use with {@code INSERT} operations.
 *
 * @author Mark Paluch
 * @author Lukasz Antoniak
 * @since 2.0
 */
@EqualsAndHashCode(callSuper = true)
public class InsertOptions extends WriteOptions {

	private static final InsertOptions EMPTY = new InsertOptionsBuilder().build();

	private boolean ifNotExists;

	private InsertOptions(@Nullable ConsistencyLevel consistencyLevel, @Nullable RetryPolicy retryPolicy,
			@Nullable Boolean tracing, @Nullable Integer fetchSize, Duration readTimeout, Duration ttl,
			@Nullable Long timestamp, boolean ifNotExists) {

		super(consistencyLevel, retryPolicy, tracing, fetchSize, readTimeout, ttl, timestamp);

		this.ifNotExists = ifNotExists;
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
	 * Create default {@link InsertOptions}.
	 *
	 * @return default {@link InsertOptions}.
	 * @since 2.0
	 */
	public static InsertOptions empty() {
		return EMPTY;
	}

	/**
	 * Create a new {@link InsertOptionsBuilder} to mutate properties of this {@link InsertOptions}.
	 *
	 * @return a new {@link InsertOptionsBuilder} initialized with this {@link InsertOptions}.
	 */
	@Override
	public InsertOptionsBuilder mutate() {
		return new InsertOptionsBuilder(this);
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
	 * @author Lukasz Antoniak
	 * @since 2.0
	 */
	public static class InsertOptionsBuilder extends WriteOptionsBuilder {

		private boolean ifNotExists;

		private InsertOptionsBuilder() {}

		private InsertOptionsBuilder(InsertOptions insertOptions) {

			super(insertOptions);

			this.ifNotExists = insertOptions.ifNotExists;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.WriteOptions.WriteOptionsBuilder#consistencyLevel(com.datastax.driver.core.ConsistencyLevel)
		 */
		@Override
		public InsertOptionsBuilder consistencyLevel(com.datastax.driver.core.ConsistencyLevel consistencyLevel) {

			super.consistencyLevel(consistencyLevel);
			return this;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.WriteOptions.WriteOptionsBuilder#retryPolicy(com.datastax.driver.core.policies.RetryPolicy)
		 */
		@Override
		public InsertOptionsBuilder retryPolicy(com.datastax.driver.core.policies.RetryPolicy driverRetryPolicy) {

			super.retryPolicy(driverRetryPolicy);
			return this;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.WriteOptions.WriteOptionsBuilder#fetchSize(int)
		 */
		@Override
		public InsertOptionsBuilder fetchSize(int fetchSize) {
			return (InsertOptionsBuilder) super.fetchSize(fetchSize);
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.WriteOptions.WriteOptionsBuilder#readTimeout(long)
		 */
		@Override
		public InsertOptionsBuilder readTimeout(long readTimeout) {

			super.readTimeout(readTimeout);
			return this;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.WriteOptions.WriteOptionsBuilder#readTimeout(long, java.util.concurrent.TimeUnit)
		 */
		@Override
		@Deprecated
		public InsertOptionsBuilder readTimeout(long readTimeout, TimeUnit timeUnit) {

			super.readTimeout(readTimeout, timeUnit);
			return this;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.WriteOptions.WriteOptionsBuilder#readTimeout(java.time.Duration)
		 */
		@Override
		public InsertOptionsBuilder readTimeout(Duration readTimeout) {

			super.readTimeout(readTimeout);
			return this;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.WriteOptions.WriteOptionsBuilder#ttl(java.time.Duration)
		 */
		@Override
		public InsertOptionsBuilder ttl(Duration ttl) {

			super.ttl(ttl);
			return this;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.WriteOptions.WriteOptionsBuilder#tracing(boolean)
		 */
		@Override
		public InsertOptionsBuilder tracing(boolean tracing) {

			super.tracing(tracing);
			return this;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.WriteOptions.WriteOptionsBuilder#withTracing()
		 */
		@Override
		public InsertOptionsBuilder withTracing() {

			super.withTracing();
			return this;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.WriteOptions.WriteOptionsBuilder#ttl(int)
		 */
		@Override
		public InsertOptionsBuilder ttl(int ttl) {

			super.ttl(ttl);
			return this;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.WriteOptions.WriteOptionsBuilder#timestamp(long)
		 */
		@Override
		public InsertOptionsBuilder timestamp(long timestamp) {

			super.timestamp(timestamp);
			return this;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.WriteOptions.WriteOptionsBuilder#timestamp(java.time.Instant)
		 */
		@Override
		public InsertOptionsBuilder timestamp(Instant timestamp) {

			super.timestamp(timestamp);
			return this;
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
			return new InsertOptions(this.consistencyLevel, this.retryPolicy, this.tracing, this.fetchSize, this.readTimeout,
					this.ttl, this.timestamp, this.ifNotExists);
		}
	}
}
