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
import org.springframework.lang.Nullable;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * Extension to {@link WriteOptions} for use with {@code INSERT} operations.
 *
 * @author Mark Paluch
 * @author Lukasz Antoniak
 * @author Tomasz Lelek
 * @since 2.0
 */
public class InsertOptions extends WriteOptions {

	private static final InsertOptions EMPTY = new InsertOptionsBuilder().build();

	private final boolean ifNotExists;

	private final boolean insertNulls;

	private InsertOptions(@Nullable ConsistencyLevel consistencyLevel, ExecutionProfileResolver executionProfileResolver,
			@Nullable CqlIdentifier keyspace, @Nullable Integer pageSize, @Nullable ConsistencyLevel serialConsistencyLevel,
			Duration timeout, Duration ttl, @Nullable Long timestamp, @Nullable Boolean tracing, boolean ifNotExists,
			boolean insertNulls) {

		super(consistencyLevel, executionProfileResolver, keyspace, pageSize, serialConsistencyLevel, timeout, ttl,
				timestamp, tracing);

		this.ifNotExists = ifNotExists;
		this.insertNulls = insertNulls;
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
	 * @return {@literal true} to insert {@literal null} values from an entity.
	 * @since 2.1
	 */
	public boolean isInsertNulls() {
		return this.insertNulls;
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

		if (!(o instanceof InsertOptions)) {
			return false;
		}

		if (!super.equals(o)) {
			return false;
		}

		InsertOptions that = (InsertOptions) o;

		if (ifNotExists != that.ifNotExists) {
			return false;
		}

		return insertNulls == that.insertNulls;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public int hashCode() {
		int result = super.hashCode();
		result = 31 * result + (ifNotExists ? 1 : 0);
		result = 31 * result + (insertNulls ? 1 : 0);
		return result;
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

		private boolean insertNulls;

		private InsertOptionsBuilder() {}

		private InsertOptionsBuilder(InsertOptions insertOptions) {

			super(insertOptions);

			this.ifNotExists = insertOptions.ifNotExists;
			this.insertNulls = insertOptions.insertNulls;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.WriteOptions.WriteOptionsBuilder#consistencyLevel(com.datastax.driver.core.ConsistencyLevel)
		 */
		@Override
		public InsertOptionsBuilder consistencyLevel(ConsistencyLevel consistencyLevel) {

			super.consistencyLevel(consistencyLevel);
			return this;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.QueryOptions.QueryOptionsBuilder#executionProfile(String)
		 */
		@Override
		public InsertOptionsBuilder executionProfile(String profileName) {
			super.executionProfile(profileName);
			return this;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.QueryOptions.QueryOptionsBuilder#executionProfile(org.springframework.data.cassandra.core.cql.ExecutionProfileResolver)
		 */
		@Override
		public InsertOptionsBuilder executionProfile(ExecutionProfileResolver executionProfileResolver) {
			super.executionProfile(executionProfileResolver);
			return this;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.WriteOptions.WriteOptionsBuilder#fetchSize(int)
		 */
		@Override
		@Deprecated
		public InsertOptionsBuilder fetchSize(int fetchSize) {
			return (InsertOptionsBuilder) super.fetchSize(fetchSize);
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.WriteOptions.WriteOptionsBuilder#keyspace()
		 */
		@Override
		public InsertOptionsBuilder keyspace(CqlIdentifier keyspace) {

			super.keyspace(keyspace);
			return this;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.WriteOptions.WriteOptionsBuilder#pageSize(int)
		 */
		@Override
		public InsertOptionsBuilder pageSize(int pageSize) {
			return (InsertOptionsBuilder) super.pageSize(pageSize);
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.WriteOptions.WriteOptionsBuilder#readTimeout(long)
		 */
		@Override
		@Deprecated
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
		 * @see org.springframework.data.cassandra.core.cql.QueryOptions.QueryOptionsBuilder#serialConsistencyLevel(com.datastax.oss.driver.api.core.ConsistencyLevel)
		 */
		@Override
		public InsertOptionsBuilder serialConsistencyLevel(ConsistencyLevel consistencyLevel) {
			super.serialConsistencyLevel(consistencyLevel);
			return this;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.WriteOptions.WriteOptionsBuilder#readTimeout(java.time.Duration)
		 */
		@Override
		public InsertOptionsBuilder timeout(Duration timeout) {

			super.timeout(timeout);
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
		 * Insert {@literal null} values from an entity. This allows the usage of {@code INSERT} statements as upsert by
		 * ensuring that the whole entity state is persisted. Inserting {@literal null}s in Cassandra creates tombstones so
		 * this option should be used with caution.
		 *
		 * @return {@code this} {@link InsertOptionsBuilder}
		 * @since 2.1
		 */
		public InsertOptionsBuilder withInsertNulls() {
			return withInsertNulls(true);
		}

		/**
		 * Insert {@literal null} values from an entity. This allows the usage of {@code INSERT} statements as upsert by
		 * ensuring that the whole entity state is persisted. Inserting {@literal null}s in Cassandra creates tombstones so
		 * this option should be used with caution.
		 *
		 * @param insertNulls {@literal true} to enable insertion of {@literal null} values.
		 * @return {@code this} {@link InsertOptionsBuilder}
		 * @since 2.1
		 */
		public InsertOptionsBuilder withInsertNulls(boolean insertNulls) {

			this.insertNulls = insertNulls;

			return this;
		}

		/**
		 * Builds a new {@link InsertOptions} with the configured values.
		 *
		 * @return a new {@link InsertOptions} with the configured values
		 */
		public InsertOptions build() {
			return new InsertOptions(this.consistencyLevel, this.executionProfileResolver, this.keyspace, this.pageSize,
					this.serialConsistencyLevel, this.timeout, this.ttl, this.timestamp, this.tracing, this.ifNotExists,
					this.insertNulls);
		}
	}
}
