/*
 * Copyright 2016 the original author or authors.
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
package org.springframework.cassandra.core;

import org.springframework.util.Assert;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;

/**
 * Utility class to associate {@link QueryOptions} and {@link WriteOptions} with QueryBuilder {@link Statement}s.
 * 
 * @author Mark Paluch
 * @since 2.0
 */
public abstract class QueryOptionsUtil {

	/**
	 * Add common {@link QueryOptions} to Cassandra {@link PreparedStatement}s.
	 *
	 * @param preparedStatement the Cassandra {@link PreparedStatement}, must not be {@literal null}.
	 * @param queryOptions query options (e.g. consistency level) to add to the Cassandra {@link PreparedStatement}.
	 */
	public static PreparedStatement addPreparedStatementOptions(PreparedStatement preparedStatement,
			QueryOptions queryOptions) {

		Assert.notNull(preparedStatement, "PreparedStatement must not be null");

		if (queryOptions != null) {
			if (queryOptions.getDriverConsistencyLevel() != null) {
				preparedStatement.setConsistencyLevel(queryOptions.getDriverConsistencyLevel());
			} else if (queryOptions.getConsistencyLevel() != null) {
				preparedStatement.setConsistencyLevel(ConsistencyLevelResolver.resolve(queryOptions.getConsistencyLevel()));
			}

			if (queryOptions.getDriverRetryPolicy() != null) {
				preparedStatement.setRetryPolicy(queryOptions.getDriverRetryPolicy());
			} else if (queryOptions.getRetryPolicy() != null) {
				preparedStatement.setRetryPolicy(RetryPolicyResolver.resolve(queryOptions.getRetryPolicy()));
			}
		}

		return preparedStatement;
	}

	/**
	 * Add common {@link QueryOptions} to all types of queries.
	 *
	 * @param statement CQL {@link Statement}, must not be {@literal null}.
	 * @param queryOptions query options (e.g. consistency level) to add to the CQL statement.
	 * @return the given {@link Statement}.
	 */
	public static <T extends Statement> T addQueryOptions(T statement, QueryOptions queryOptions) {

		Assert.notNull(statement, "Statement must not be null");

		if (queryOptions != null) {
			if (queryOptions.getDriverConsistencyLevel() != null) {
				statement.setConsistencyLevel(queryOptions.getDriverConsistencyLevel());
			} else if (queryOptions.getConsistencyLevel() != null) {
				statement.setConsistencyLevel(ConsistencyLevelResolver.resolve(queryOptions.getConsistencyLevel()));
			}

			if (queryOptions.getDriverRetryPolicy() != null) {
				statement.setRetryPolicy(queryOptions.getDriverRetryPolicy());
			} else if (queryOptions.getRetryPolicy() != null) {
				statement.setRetryPolicy(RetryPolicyResolver.resolve(queryOptions.getRetryPolicy()));
			}

			if (queryOptions.getFetchSize() != null) {
				statement.setFetchSize(queryOptions.getFetchSize());
			}

			if (queryOptions.getReadTimeout() != null) {
				statement.setReadTimeoutMillis(queryOptions.getReadTimeout().intValue());
			}

			if (queryOptions.getTracing() != null) {
				if (queryOptions.getTracing()) {
					statement.enableTracing();
				} else {
					statement.disableTracing();
				}
			}
		}

		return statement;
	}

	/**
	 * Add common {@link WriteOptions} options to {@link Insert} CQL statements.
	 *
	 * @param insert {@link Insert} CQL statement, must not be {@literal null}.
	 * @param writeOptions write options (e.g. consistency level) to add to the CQL statement.
	 * @return the given {@link Insert}.
	 */
	public static Insert addWriteOptions(Insert insert, WriteOptions writeOptions) {

		Assert.notNull(insert, "Insert must not be null");

		if (writeOptions != null) {

			addQueryOptions(insert, writeOptions);

			if (writeOptions.getTtl() != null) {
				insert.using(QueryBuilder.ttl(writeOptions.getTtl()));
			}
		}

		return insert;
	}

	/**
	 * Add common {@link WriteOptions} options to {@link Update} CQL statements.
	 *
	 * @param update {@link Update} CQL statement, must not be {@literal null}.
	 * @param writeOptions write options (e.g. consistency level) to add to the CQL statement.
	 * @return the given {@link Update}.
	 */
	public static Update addWriteOptions(Update update, WriteOptions writeOptions) {

		Assert.notNull(update, "Update must not be null");

		if (writeOptions != null) {

			addQueryOptions(update, writeOptions);

			if (writeOptions.getTtl() != null) {
				update.using(QueryBuilder.ttl(writeOptions.getTtl()));
			}
		}

		return update;
	}
}
