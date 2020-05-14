/*
 * Copyright 2013-2020 the original author or authors.
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

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

import com.datastax.oss.driver.api.core.ConsistencyLevel;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * Cassandra Write Options are an extension to {@link QueryOptions} for write operations. {@link WriteOptions} allow
 * tuning of various query options on a per-request level. Only options that are set are applied to queries.
 *
 * @author David Webb
 * @author Mark Paluch
 * @author Lukasz Antoniak
 * @see QueryOptions
 */
public class WriteOptions extends QueryOptions {

	private static final WriteOptions EMPTY = new WriteOptionsBuilder().build();

	private final Duration ttl;
	private final @Nullable Long timestamp;

	protected WriteOptions(@Nullable ConsistencyLevel consistencyLevel, ExecutionProfileResolver executionProfileResolver,
			@Nullable Integer pageSize, @Nullable ConsistencyLevel serialConsistencyLevel, Duration timeout, Duration ttl,
			@Nullable Long timestamp, @Nullable Boolean tracing) {

		super(consistencyLevel, executionProfileResolver, pageSize, serialConsistencyLevel, timeout, tracing);

		this.ttl = ttl;
		this.timestamp = timestamp;
	}

	/**
	 * Create default {@link WriteOptions}.
	 *
	 * @return default {@link WriteOptions}.
	 * @since 2.0
	 */
	public static WriteOptions empty() {
		return EMPTY;
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
	 * Create a new {@link WriteOptionsBuilder} to mutate properties of this {@link WriteOptions}.
	 *
	 * @return a new {@link WriteOptionsBuilder} initialized with this {@link WriteOptions}.
	 * @since 2.0
	 */
	@Override
	public WriteOptionsBuilder mutate() {
		return new WriteOptionsBuilder(this);
	}

	/**
	 * @return the time to live, if set.
	 */
	public Duration getTtl() {
		return this.ttl;
	}

	/**
	 * @return mutation timestamp in microseconds.
	 * @since 2.1
	 */
	@Nullable
	public Long getTimestamp() {
		return this.timestamp;
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

		if (!(o instanceof WriteOptions)) {
			return false;
		}

		if (!super.equals(o)) {
			return false;
		}

		WriteOptions that = (WriteOptions) o;

		if (!ObjectUtils.nullSafeEquals(ttl, that.ttl)) {
			return false;
		}

		return ObjectUtils.nullSafeEquals(timestamp, that.timestamp);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public int hashCode() {
		int result = super.hashCode();
		result = 31 * result + ObjectUtils.nullSafeHashCode(ttl);
		result = 31 * result + ObjectUtils.nullSafeHashCode(timestamp);
		return result;
	}

	/**
	 * Builder for {@link WriteOptions}.
	 *
	 * @author Mark Paluch
	 * @author Lukasz Antoniak
	 * @since 1.5
	 */
	public static class WriteOptionsBuilder extends QueryOptionsBuilder {

		protected Duration ttl = Duration.ofMillis(-1);
		protected Long timestamp = null;

		protected WriteOptionsBuilder() {}

		protected WriteOptionsBuilder(WriteOptions writeOptions) {

			super(writeOptions);

			this.ttl = writeOptions.ttl;
			this.timestamp = writeOptions.timestamp;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.QueryOptions.QueryOptionsBuilder#consistencyLevel(com.datastax.oss.driver.api.core.ConsistencyLevel)
		 */
		@Override
		public WriteOptionsBuilder consistencyLevel(ConsistencyLevel consistencyLevel) {

			super.consistencyLevel(consistencyLevel);
			return this;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.QueryOptions.QueryOptionsBuilder#executionProfile(String)
		 */
		@Override
		public WriteOptionsBuilder executionProfile(String profileName) {
			super.executionProfile(profileName);
			return this;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.QueryOptions.QueryOptionsBuilder#executionProfile(org.springframework.data.cassandra.core.cql.ExecutionProfileResolver)
		 */
		@Override
		public WriteOptionsBuilder executionProfile(ExecutionProfileResolver executionProfileResolver) {
			super.executionProfile(executionProfileResolver);
			return this;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.QueryOptions.QueryOptionsBuilder#fetchSize(int)
		 */
		@Override
		@Deprecated
		public WriteOptionsBuilder fetchSize(int pageSize) {

			super.fetchSize(pageSize);
			return this;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.QueryOptions.QueryOptionsBuilder#pageSize(int)
		 */
		@Override
		public WriteOptionsBuilder pageSize(int pageSize) {

			super.pageSize(pageSize);
			return this;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.QueryOptions.QueryOptionsBuilder#readTimeout(long)
		 */
		@Override
		@Deprecated
		public WriteOptionsBuilder readTimeout(long readTimeout) {

			super.readTimeout(readTimeout);
			return this;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.QueryOptions.QueryOptionsBuilder#readTimeout(long, java.util.concurrent.TimeUnit)
		 */
		@Override
		@Deprecated
		public WriteOptionsBuilder readTimeout(long readTimeout, TimeUnit timeUnit) {

			super.readTimeout(readTimeout, timeUnit);
			return this;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.QueryOptions.QueryOptionsBuilder#serialConsistencyLevel(com.datastax.oss.driver.api.core.ConsistencyLevel)
		 */
		@Override
		public WriteOptionsBuilder serialConsistencyLevel(ConsistencyLevel consistencyLevel) {
			super.serialConsistencyLevel(consistencyLevel);
			return this;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.QueryOptions.QueryOptionsBuilder#readTimeout(java.time.Duration)
		 */
		@Override
		public WriteOptionsBuilder timeout(Duration timeout) {

			super.timeout(timeout);
			return this;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.QueryOptions.QueryOptionsBuilder#tracing(boolean)
		 */
		@Override
		public WriteOptionsBuilder tracing(boolean tracing) {

			super.tracing(tracing);
			return this;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.QueryOptions.QueryOptionsBuilder#withTracing()
		 */
		@Override
		public WriteOptionsBuilder withTracing() {

			super.withTracing();
			return this;
		}

		/**
		 * Sets the time to live in seconds for write operations.
		 *
		 * @param ttl the time to live.
		 * @return {@code this} {@link WriteOptionsBuilder}
		 */
		public WriteOptionsBuilder ttl(int ttl) {

			Assert.isTrue(ttl >= 0, "TTL must be greater than equal to zero");

			this.ttl = Duration.ofSeconds(ttl);

			return this;
		}

		/**
		 * Sets the time to live in seconds for write operations.
		 *
		 * @param ttl the time to live.
		 * @return {@code this} {@link WriteOptionsBuilder}
		 * @since 2.0
		 */
		public WriteOptionsBuilder ttl(Duration ttl) {

			Assert.notNull(ttl, "TTL must not be null");
			Assert.isTrue(!ttl.isNegative(), "TTL must be greater than equal to zero");

			this.ttl = ttl;

			return this;
		}

		/**
		 * Sets the timestamp of write operations.
		 *
		 * @param timestamp mutation timestamp in microseconds.
		 * @return {@code this} {@link WriteOptionsBuilder}
		 * @since 2.1
		 * @see TimeUnit#MICROSECONDS
		 */
		public WriteOptionsBuilder timestamp(long timestamp) {

			Assert.isTrue(timestamp >= 0, "Timestamp must be greater than equal to zero");

			this.timestamp = timestamp;

			return this;
		}

		/**
		 * Sets the timestamp of write operations.
		 *
		 * @param timestamp mutation date time.
		 * @return {@code this} {@link WriteOptionsBuilder}
		 * @since 2.1
		 */
		public WriteOptionsBuilder timestamp(Instant timestamp) {

			Assert.notNull(timestamp, "Timestamp must not be null");

			this.timestamp = TimeUnit.MILLISECONDS.toMicros(timestamp.toEpochMilli());

			return this;
		}

		/**
		 * Builds a new {@link WriteOptions} with the configured values.
		 *
		 * @return a new {@link WriteOptions} with the configured values
		 */
		public WriteOptions build() {
			return new WriteOptions(this.consistencyLevel, this.executionProfileResolver, this.pageSize,
					this.serialConsistencyLevel, this.timeout, this.ttl, this.timestamp, this.tracing);
		}
	}
}
