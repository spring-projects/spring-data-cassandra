/*
 * Copyright 2017-2021 the original author or authors.
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
 * @since 2.0
 */
public class UpdateOptions extends WriteOptions {

	private static final UpdateOptions EMPTY = new UpdateOptionsBuilder().build();

	private final boolean ifExists;

	private final @Nullable Filter ifCondition;

	private UpdateOptions(@Nullable ConsistencyLevel consistencyLevel, ExecutionProfileResolver executionProfileResolver,
			@Nullable CqlIdentifier keyspace, @Nullable Integer pageSize, @Nullable ConsistencyLevel serialConsistencyLevel,
			Duration timeout, Duration ttl, @Nullable Long timestamp, @Nullable Boolean tracing, boolean ifExists,
			@Nullable Filter ifCondition) {

		super(consistencyLevel, executionProfileResolver, keyspace, pageSize, serialConsistencyLevel, timeout, ttl,
				timestamp, tracing);

		this.ifExists = ifExists;
		this.ifCondition = ifCondition;
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
	 * @return the {@link Filter IF condition} for conditional updates.
	 * @since 2.2
	 */
	@Nullable
	public Filter getIfCondition() {
		return ifCondition;
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

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
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

		private boolean ifExists;

		private @Nullable Filter ifCondition;

		private UpdateOptionsBuilder() {}

		private UpdateOptionsBuilder(UpdateOptions updateOptions) {

			super(updateOptions);

			this.ifExists = updateOptions.ifExists;
			this.ifCondition = updateOptions.ifCondition;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.WriteOptions.WriteOptionsBuilder#consistencyLevel(com.datastax.driver.core.ConsistencyLevel)
		 */
		@Override
		public UpdateOptionsBuilder consistencyLevel(ConsistencyLevel consistencyLevel) {

			super.consistencyLevel(consistencyLevel);
			return this;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.QueryOptions.QueryOptionsBuilder#executionProfile(String)
		 */
		@Override
		public UpdateOptionsBuilder executionProfile(String profileName) {
			super.executionProfile(profileName);
			return this;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.QueryOptions.QueryOptionsBuilder#executionProfile(org.springframework.data.cassandra.core.cql.ExecutionProfileResolver)
		 */
		@Override
		public UpdateOptionsBuilder executionProfile(ExecutionProfileResolver executionProfileResolver) {
			super.executionProfile(executionProfileResolver);
			return this;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.WriteOptions.WriteOptionsBuilder#fetchSize(int)
		 */
		@Override
		@Deprecated
		public UpdateOptionsBuilder fetchSize(int fetchSize) {

			super.fetchSize(fetchSize);
			return this;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.WriteOptions.WriteOptionsBuilder#keyspace()
		 */
		@Override
		public UpdateOptionsBuilder keyspace(CqlIdentifier keyspace) {

			super.keyspace(keyspace);
			return this;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.WriteOptions.WriteOptionsBuilder#pageSize(int)
		 */
		@Override
		public UpdateOptionsBuilder pageSize(int pageSize) {

			super.pageSize(pageSize);
			return this;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.WriteOptions.WriteOptionsBuilder#readTimeout(long)
		 */
		@Override
		@Deprecated
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
		 * @see org.springframework.data.cassandra.core.cql.QueryOptions.QueryOptionsBuilder#serialConsistencyLevel(com.datastax.oss.driver.api.core.ConsistencyLevel)
		 */
		@Override
		public UpdateOptionsBuilder serialConsistencyLevel(ConsistencyLevel consistencyLevel) {
			super.serialConsistencyLevel(consistencyLevel);
			return this;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.WriteOptions.WriteOptionsBuilder#readTimeout(java.time.Duration)
		 */
		@Override
		public UpdateOptionsBuilder timeout(Duration timeout) {

			super.timeout(timeout);
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
		 * Builds a new {@link UpdateOptions} with the configured values.
		 *
		 * @return a new {@link UpdateOptions} with the configured values
		 */
		public UpdateOptions build() {
			return new UpdateOptions(this.consistencyLevel, this.executionProfileResolver, this.keyspace, this.pageSize,
					this.serialConsistencyLevel, this.timeout, this.ttl, this.timestamp, this.tracing, this.ifExists,
					this.ifCondition);
		}
	}
}
