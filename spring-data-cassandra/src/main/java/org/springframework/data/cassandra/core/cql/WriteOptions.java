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
import java.time.Instant;
import java.util.concurrent.TimeUnit;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * Cassandra Write Options are an extension to {@link QueryOptions} for write operations. {@link WriteOptions} allow
 * tuning of various query options on a per-request level. Only options that are set are applied to queries.
 *
 * @author David Webb
 * @author Mark Paluch
 * @author Lukasz Antoniak
 * @author Tomasz Lelek
 * @author Sam Lightfoot
 * @author Thomas Strau&szlig;
 * @see QueryOptions
 */
public class WriteOptions extends QueryOptions {

	private static final WriteOptions EMPTY = new WriteOptionsBuilder().build();

	private final @Nullable Duration ttl;

	private final @Nullable Long timestamp;

	protected WriteOptions(@Nullable ConsistencyLevel consistencyLevel, ExecutionProfileResolver executionProfileResolver,
			@Nullable Boolean idempotent, @Nullable CqlIdentifier keyspace, @Nullable Integer pageSize,
			@Nullable CqlIdentifier routingKeyspace, @Nullable ByteBuffer routingKey,
			@Nullable ConsistencyLevel serialConsistencyLevel, Duration timeout, @Nullable Duration ttl,
			@Nullable Long timestamp,
			@Nullable Boolean tracing) {

		super(consistencyLevel, executionProfileResolver, idempotent, keyspace, pageSize, routingKeyspace, routingKey,
				serialConsistencyLevel, timeout, tracing);

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
	 * @return the time to live, if set, otherwise {@literal null}.
	 */
	@Nullable
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

	@Override
	public boolean equals(@Nullable Object o) {

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
	 * @author Thomas Strau&szlig;
	 * @author Tudor Marc
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

		@Override
		public WriteOptionsBuilder consistencyLevel(ConsistencyLevel consistencyLevel) {

			super.consistencyLevel(consistencyLevel);
			return this;
		}

		@Override
		public WriteOptionsBuilder executionProfile(String profileName) {
			super.executionProfile(profileName);
			return this;
		}

		@Override
		public WriteOptionsBuilder executionProfile(ExecutionProfileResolver executionProfileResolver) {
			super.executionProfile(executionProfileResolver);
			return this;
		}

		@Override
		@Deprecated(forRemoval = true)
		public WriteOptionsBuilder fetchSize(int pageSize) {

			super.fetchSize(pageSize);
			return this;
		}

		@Override
		public WriteOptionsBuilder idempotent(boolean idempotent) {

			super.idempotent(idempotent);
			return this;
		}

		@Override
		public WriteOptionsBuilder keyspace(CqlIdentifier keyspace) {

			super.keyspace(keyspace);
			return this;
		}

		@Override
		public WriteOptionsBuilder pageSize(int pageSize) {

			super.pageSize(pageSize);
			return this;
		}

		@Override
		@Deprecated(forRemoval = true)
		public WriteOptionsBuilder readTimeout(long readTimeout) {

			super.readTimeout(readTimeout);
			return this;
		}

		@Override
		@Deprecated(forRemoval = true)
		public WriteOptionsBuilder readTimeout(long readTimeout, TimeUnit timeUnit) {

			super.readTimeout(readTimeout, timeUnit);
			return this;
		}

		@Override
		public WriteOptionsBuilder routingKeyspace(CqlIdentifier routingKeyspace) {

			super.routingKeyspace(routingKeyspace);
			return this;
		}

		@Override
		public WriteOptionsBuilder routingKey(ByteBuffer routingKey) {

			super.routingKey(routingKey);
			return this;
		}

		@Override
		public WriteOptionsBuilder serialConsistencyLevel(ConsistencyLevel consistencyLevel) {
			super.serialConsistencyLevel(consistencyLevel);
			return this;
		}

		@Override
		public WriteOptionsBuilder timeout(Duration timeout) {

			super.timeout(timeout);
			return this;
		}

		@Override
		public WriteOptionsBuilder tracing(boolean tracing) {

			super.tracing(tracing);
			return this;
		}

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
			return ttl(Duration.ofSeconds(ttl));
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
			return new WriteOptions(this.consistencyLevel, this.executionProfileResolver, this.idempotent, this.keyspace,
					this.pageSize, this.routingKeyspace, this.routingKey, this.serialConsistencyLevel, this.timeout, this.ttl,
					this.timestamp, this.tracing);
		}
	}
}
