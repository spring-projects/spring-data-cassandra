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
 * Extension to {@link WriteOptions} for use with {@code UPDATE} operations.
 *
 * @author Mark Paluch
 * @since 2.0
 */
public class UpdateOptions extends WriteOptions {

	private boolean ifExists;

	/**
	 * Creates new {@link UpdateOptions}.
	 */
	UpdateOptions() {}

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

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.QueryOptions.QueryOptionsBuilder#consistencyLevel(com.datastax.driver.core.ConsistencyLevel)
		 */
		@Override
		public UpdateOptionsBuilder consistencyLevel(com.datastax.driver.core.ConsistencyLevel consistencyLevel) {
			return (UpdateOptionsBuilder) super.consistencyLevel(consistencyLevel);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.QueryOptions.QueryOptionsBuilder#retryPolicy(org.springframework.data.cassandra.core.cql.RetryPolicy)
		 */
		@Override
		public UpdateOptionsBuilder retryPolicy(com.datastax.driver.core.policies.RetryPolicy driverRetryPolicy) {
			return (UpdateOptionsBuilder) super.retryPolicy(driverRetryPolicy);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.QueryOptions.QueryOptionsBuilder#fetchSize(int)
		 */
		@Override
		public UpdateOptionsBuilder fetchSize(int fetchSize) {
			return (UpdateOptionsBuilder) super.fetchSize(fetchSize);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.QueryOptions.QueryOptionsBuilder#readTimeout(long)
		 */
		@Override
		public UpdateOptionsBuilder readTimeout(long readTimeout) {
			return (UpdateOptionsBuilder) super.readTimeout(readTimeout);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.QueryOptions.QueryOptionsBuilder#readTimeout(long, java.util.concurrent.TimeUnit)
		 */
		@Override
		public UpdateOptionsBuilder readTimeout(long readTimeout, TimeUnit timeUnit) {
			return (UpdateOptionsBuilder) super.readTimeout(readTimeout, timeUnit);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.QueryOptions.QueryOptionsBuilder#tracing(boolean)
		 */
		@Override
		public UpdateOptionsBuilder tracing(boolean tracing) {
			return (UpdateOptionsBuilder) super.tracing(tracing);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.QueryOptions.QueryOptionsBuilder#withTracing()
		 */
		@Override
		public UpdateOptionsBuilder withTracing() {
			return (UpdateOptionsBuilder) super.withTracing();
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.cql.WriteOptions.WriteOptionsBuilder#ttl(int)
		 */
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

			UpdateOptions insertOptions = applyOptions(new UpdateOptions());

			insertOptions.ifExists = this.ifExists;

			return insertOptions;
		}
	}
}
