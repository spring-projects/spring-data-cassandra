/*
 * Copyright 2016-2020 the original author or authors.
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

import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.cql.SimpleStatementBuilder;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.delete.DeleteSelection;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.update.Update;
import com.datastax.oss.driver.api.querybuilder.update.UpdateStart;

/**
 * Utility class to associate {@link QueryOptions} and {@link WriteOptions} with QueryBuilder {@link Statement}s.
 *
 * @author Mark Paluch
 * @author Lukasz Antoniak
 * @since 2.0
 */
public abstract class QueryOptionsUtil {

	/**
	 * Add common {@link QueryOptions} to all types of queries.
	 *
	 * @param statement CQL {@link Statement}, must not be {@literal null}.
	 * @param queryOptions query options (e.g. consistency level) to add to the CQL statement.
	 * @return the given {@link Statement}.
	 */
	public static <T extends Statement<?>> T addQueryOptions(T statement, QueryOptions queryOptions) {

		Assert.notNull(statement, "Statement must not be null");
		Statement<?> statementToUse = statement;

		if (queryOptions.getConsistencyLevel() != null) {
			statementToUse = statementToUse.setConsistencyLevel(queryOptions.getConsistencyLevel());
		}

		if (queryOptions.getPageSize() != null) {
			statementToUse = statementToUse.setPageSize(queryOptions.getPageSize());
		}

		if (!queryOptions.getReadTimeout().isNegative()) {
			statementToUse = statementToUse.setTimeout(queryOptions.getReadTimeout());
		}

		if (queryOptions.getTracing() != null) {
			if (queryOptions.getTracing()) {
				statementToUse = statementToUse.setTracing(true);
			} else {
				statementToUse = statementToUse.setTracing(false);
			}
		}

		return (T) statementToUse;
	}

	/**
	 * Add common {@link QueryOptions} to all types of queries.
	 *
	 * @param statement a {@link SimpleStatementBuilder}, must not be {@literal null}.
	 * @param queryOptions query options (e.g. consistency level) to add to the CQL statement.
	 */
	public static void addQueryOptions(SimpleStatementBuilder statementBuilder, QueryOptions queryOptions) {

		Assert.notNull(statementBuilder, "SimpleStatementBuilder must not be null");

		if (queryOptions.getConsistencyLevel() != null) {
			statementBuilder.setConsistencyLevel(queryOptions.getConsistencyLevel());
		}

		if (queryOptions.getPageSize() != null) {
			statementBuilder.setPageSize(queryOptions.getPageSize());
		}

		if (!queryOptions.getReadTimeout().isNegative()) {
			statementBuilder.setTimeout(queryOptions.getReadTimeout());
		}

		if (queryOptions.getTracing() != null) {
			if (queryOptions.getTracing()) {
				statementBuilder.setTracing(true);
			} else {
				statementBuilder.setTracing(false);
			}
		}
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
		Assert.notNull(writeOptions, "WriteOptions must not be null");

		Insert insertToUse = insert;

		if (!writeOptions.getTtl().isNegative()) {
			insertToUse = insertToUse.usingTtl(Math.toIntExact(writeOptions.getTtl().getSeconds()));
		}

		if (writeOptions.getTimestamp() != null) {
			insertToUse = insertToUse.usingTimestamp(writeOptions.getTimestamp());
		}

		return insertToUse;
	}

	/**
	 * Add common {@link WriteOptions} options to {@link Delete} CQL statements.
	 *
	 * @param delete {@link Delete} CQL statement, must not be {@literal null}.
	 * @param writeOptions write options (e.g. consistency level) to add to the CQL statement.
	 * @return the given {@link Delete}.
	 * @since 2.1
	 */
	public static Delete addWriteOptions(Delete delete, WriteOptions writeOptions) {

		Assert.notNull(delete, "Delete must not be null");
		Assert.notNull(writeOptions, "WriteOptions must not be null");

		if (writeOptions.getTimestamp() != null) {
			delete = (Delete) ((DeleteSelection) delete).usingTimestamp(writeOptions.getTimestamp());
		}

		return delete;
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
		Assert.notNull(writeOptions, "WriteOptions must not be null");

		if (hasTtl(writeOptions.getTtl())) {
			update = (Update) ((UpdateStart) update).usingTtl(getTtlSeconds(writeOptions.getTtl()));
		}

		if (writeOptions.getTimestamp() != null) {
			update = (Update) ((UpdateStart) update).usingTimestamp(writeOptions.getTimestamp());
		}

		return update;
	}

	private static int getTtlSeconds(Duration ttl) {
		return Math.toIntExact(ttl.getSeconds());
	}

	private static boolean hasTtl(Duration ttl) {
		return !ttl.isZero() && !ttl.isNegative();
	}
}
