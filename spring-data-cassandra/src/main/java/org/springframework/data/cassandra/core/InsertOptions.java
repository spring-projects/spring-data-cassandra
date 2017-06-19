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

import java.util.concurrent.TimeUnit;

import org.springframework.data.cassandra.core.cql.WriteOptions;

/**
 * Extension to {@link WriteOptions} for use with {@code INSERT} operations.
 *
 * @author Mark Paluch
 * @since 2.0
 */
public class InsertOptions extends WriteOptions {

	private boolean ifNotExists;

	/**
	 * Creates new {@link InsertOptions}.
	 */
	InsertOptions() {}

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

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.QueryOptions.QueryOptionsBuilder#consistencyLevel(com.datastax.driver.core.ConsistencyLevel)
		 */
		@Override
		public InsertOptionsBuilder consistencyLevel(com.datastax.driver.core.ConsistencyLevel consistencyLevel) {
			return (InsertOptionsBuilder) super.consistencyLevel(consistencyLevel);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.QueryOptions.QueryOptionsBuilder#retryPolicy(org.springframework.data.cassandra.core.cql.RetryPolicy)
		 */
		@Override
		public InsertOptionsBuilder retryPolicy(com.datastax.driver.core.policies.RetryPolicy driverRetryPolicy) {
			return (InsertOptionsBuilder) super.retryPolicy(driverRetryPolicy);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.QueryOptions.QueryOptionsBuilder#fetchSize(int)
		 */
		@Override
		public InsertOptionsBuilder fetchSize(int fetchSize) {
			return (InsertOptionsBuilder) super.fetchSize(fetchSize);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.QueryOptions.QueryOptionsBuilder#readTimeout(long)
		 */
		@Override
		public InsertOptionsBuilder readTimeout(long readTimeout) {
			return (InsertOptionsBuilder) super.readTimeout(readTimeout);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.QueryOptions.QueryOptionsBuilder#readTimeout(long, java.util.concurrent.TimeUnit)
		 */
		@Override
		public InsertOptionsBuilder readTimeout(long readTimeout, TimeUnit timeUnit) {
			return (InsertOptionsBuilder) super.readTimeout(readTimeout, timeUnit);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.QueryOptions.QueryOptionsBuilder#tracing(boolean)
		 */
		@Override
		public InsertOptionsBuilder tracing(boolean tracing) {
			return (InsertOptionsBuilder) super.tracing(tracing);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.QueryOptions.QueryOptionsBuilder#withTracing()
		 */
		@Override
		public InsertOptionsBuilder withTracing() {
			return (InsertOptionsBuilder) super.withTracing();
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.WriteOptions.WriteOptionsBuilder#ttl(int)
		 */
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

			InsertOptions insertOptions = applyOptions(new InsertOptions());

			insertOptions.ifNotExists = this.ifNotExists;

			return insertOptions;
		}
	}
}
