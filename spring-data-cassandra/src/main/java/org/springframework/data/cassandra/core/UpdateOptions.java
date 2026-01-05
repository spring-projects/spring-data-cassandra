/*
 * Copyright 2017-present the original author or authors.
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
package org.springframework.data.cassandra.core;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

import org.springframework.data.cassandra.core.cql.ExecutionProfileResolver;
import org.springframework.data.cassandra.core.cql.WriteOptions;
import org.springframework.data.cassandra.core.query.CriteriaDefinition;
import org.springframework.data.cassandra.core.query.Filter;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * Extension to {@link WriteOptions} for use with {@code UPDATE} operations.
 *
 * @author Mark Paluch
 * @author Lukasz Antoniak
 * @author Tomasz Lelek
 * @author Sam Lightfoot
 * @since 2.0
 */
public class UpdateOptions extends WriteOptions {

	private static final UpdateOptions EMPTY = new UpdateOptionsBuilder().build();

	private final @Nullable Filter ifCondition;

	private final boolean ifExists;

	private UpdateOptions(@Nullable ConsistencyLevel consistencyLevel, ExecutionProfileResolver executionProfileResolver,
			@Nullable Filter ifCondition, boolean ifExists, @Nullable Boolean idempotent, @Nullable CqlIdentifier keyspace,
			@Nullable Integer pageSize, @Nullable CqlIdentifier routingKeyspace, @Nullable ByteBuffer routingKey,
			@Nullable ConsistencyLevel serialConsistencyLevel, Duration timeout, @Nullable Duration ttl,
			@Nullable Long timestamp,
			@Nullable Boolean tracing) {

		super(consistencyLevel, executionProfileResolver, idempotent, keyspace, pageSize, routingKeyspace, routingKey,
				serialConsistencyLevel, timeout, ttl, timestamp, tracing);

		this.ifCondition = ifCondition;
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
	 * @return the {@link Filter IF condition} for conditional updates.
	 * @since 2.2
	 */
	@Nullable
	public Filter getIfCondition() {
		return ifCondition;
	}

	/**
	 * @return {@literal true} to apply {@code IF EXISTS} to {@code UPDATE} operations.
	 */
	public boolean isIfExists() {
		return this.ifExists;
	}

	@Override
	public boolean equals(@Nullable Object o) {

		if (this == o) {
			return true;
		}

		if (!(o instanceof UpdateOptions)) {
			return false;
		}

		if (!super.equals(o)) {
			return false;
		}

		UpdateOptions that = (UpdateOptions) o;

		if (ifExists != that.ifExists) {
			return false;
		}

		return ObjectUtils.nullSafeEquals(ifCondition, that.ifCondition);
	}

	@Override
	public int hashCode() {
		int result = super.hashCode();
		result = 31 * result + (ifExists ? 1 : 0);
		result = 31 * result + ObjectUtils.nullSafeHashCode(ifCondition);
		return result;
	}

	/**
	 * Builder for {@link UpdateOptions}.
	 *
	 * @author Mark Paluch
	 * @author Lukasz Antoniak
	 * @since 2.0
	 */
	public static class UpdateOptionsBuilder extends WriteOptionsBuilder {

		private @Nullable Filter ifCondition;

		private boolean ifExists;

		private UpdateOptionsBuilder() {}

		private UpdateOptionsBuilder(UpdateOptions updateOptions) {

			super(updateOptions);

			this.ifCondition = updateOptions.ifCondition;
			this.ifExists = updateOptions.ifExists;
		}

		@Override
		public UpdateOptionsBuilder consistencyLevel(ConsistencyLevel consistencyLevel) {

			super.consistencyLevel(consistencyLevel);
			return this;
		}

		@Override
		public UpdateOptionsBuilder executionProfile(String profileName) {
			super.executionProfile(profileName);
			return this;
		}

		@Override
		public UpdateOptionsBuilder executionProfile(ExecutionProfileResolver executionProfileResolver) {
			super.executionProfile(executionProfileResolver);
			return this;
		}

		@Override
		@Deprecated(forRemoval = true)
		public UpdateOptionsBuilder fetchSize(int fetchSize) {

			super.fetchSize(fetchSize);
			return this;
		}

		@Override
		public UpdateOptionsBuilder idempotent(boolean idempotent) {

			super.idempotent(idempotent);
			return this;
		}

		@Override
		public UpdateOptionsBuilder keyspace(CqlIdentifier keyspace) {

			super.keyspace(keyspace);
			return this;
		}

		@Override
		public UpdateOptionsBuilder pageSize(int pageSize) {

			super.pageSize(pageSize);
			return this;
		}

		@Override
		@Deprecated(forRemoval = true)
		public UpdateOptionsBuilder readTimeout(long readTimeout) {

			super.readTimeout(readTimeout);
			return this;
		}

		@Override
		@Deprecated(forRemoval = true)
		public UpdateOptionsBuilder readTimeout(long readTimeout, TimeUnit timeUnit) {

			super.readTimeout(readTimeout, timeUnit);
			return this;
		}

		@Override
		public UpdateOptionsBuilder routingKeyspace(CqlIdentifier routingKeyspace) {

			super.routingKeyspace(routingKeyspace);
			return this;
		}

		@Override
		public UpdateOptionsBuilder routingKey(ByteBuffer routingKey) {

			super.routingKey(routingKey);
			return this;
		}

		@Override
		public UpdateOptionsBuilder serialConsistencyLevel(ConsistencyLevel consistencyLevel) {
			super.serialConsistencyLevel(consistencyLevel);
			return this;
		}

		@Override
		public UpdateOptionsBuilder timeout(Duration timeout) {

			super.timeout(timeout);
			return this;
		}

		@Override
		public UpdateOptionsBuilder ttl(Duration ttl) {

			super.ttl(ttl);
			return this;
		}

		@Override
		public UpdateOptionsBuilder tracing(boolean tracing) {

			super.tracing(tracing);
			return this;
		}

		@Override
		public UpdateOptionsBuilder withTracing() {

			super.withTracing();
			return this;
		}

		public UpdateOptionsBuilder ttl(int ttl) {

			super.ttl(ttl);
			return this;
		}

		@Override
		public UpdateOptionsBuilder timestamp(long timestamp) {

			super.timestamp(timestamp);
			return this;
		}

		@Override
		public UpdateOptionsBuilder timestamp(Instant timestamp) {

			super.timestamp(timestamp);
			return this;
		}

		/**
		 * Use light-weight transactions by applying {@code IF} {@link CriteriaDefinition condition}. Replaces a previous
		 * {@link #ifCondition(Filter)} and {@link #ifExists(boolean)}.
		 *
		 * @param criteria the {@link Filter criteria} to apply for conditional updates, must not be {@literal null}.
		 * @return {@code this} {@link UpdateOptionsBuilder}
		 * @since 2.2
		 */
		public UpdateOptionsBuilder ifCondition(CriteriaDefinition criteria) {

			Assert.notNull(criteria, "CriteriaDefinition must not be null");

			return ifCondition(Filter.from(criteria));
		}

		/**
		 * Use light-weight transactions by applying {@code IF} {@link Filter condition}. Replaces a previous
		 * {@link #ifCondition(Filter)} and {@link #ifExists(boolean)}.
		 *
		 * @param condition the {@link Filter condition} to apply for conditional updates, must not be {@literal null}.
		 * @return {@code this} {@link UpdateOptionsBuilder}
		 * @since 2.2
		 */
		public UpdateOptionsBuilder ifCondition(Filter condition) {

			Assert.notNull(condition, "Filter condition must not be null");

			this.ifCondition = condition;
			this.ifExists = false;

			return this;
		}

		/**
		 * Use light-weight transactions by applying {@code IF EXISTS}. Replaces a previous {@link #ifCondition(Filter)}.
		 *
		 * @return {@code this} {@link UpdateOptionsBuilder}
		 */
		public UpdateOptionsBuilder withIfExists() {
			return ifExists(true);
		}

		/**
		 * Use light-weight transactions by applying {@code IF EXISTS}. Replaces a previous {@link #ifCondition(Filter)}.
		 *
		 * @param ifNotExists {@literal true} to enable {@code IF EXISTS}.
		 * @return {@code this} {@link UpdateOptionsBuilder}
		 */
		public UpdateOptionsBuilder ifExists(boolean ifNotExists) {

			this.ifExists = ifNotExists;
			this.ifCondition = null;

			return this;
		}

		/**
		 * Builds a new {@link UpdateOptions} with the configured values.
		 *
		 * @return a new {@link UpdateOptions} with the configured values
		 */
		public UpdateOptions build() {
			return new UpdateOptions(this.consistencyLevel, this.executionProfileResolver, this.ifCondition, this.ifExists,
					this.idempotent, this.keyspace, this.pageSize, this.routingKeyspace, this.routingKey,
					this.serialConsistencyLevel, this.timeout, this.ttl, this.timestamp, this.tracing);
		}
	}
}
