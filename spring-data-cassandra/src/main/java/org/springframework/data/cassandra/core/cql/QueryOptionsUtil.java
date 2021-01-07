/*
 * Copyright 2016-2021 the original author or authors.
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

import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
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
 * @author Tomasz Lelek
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
	@SuppressWarnings("unchecked")
	public static <T extends Statement<?>> T addQueryOptions(T statement, QueryOptions queryOptions) {

		Assert.notNull(statement, "Statement must not be null");

		Statement<?> statementToUse = statement;

		if (queryOptions.getConsistencyLevel() != null) {
			statementToUse = statementToUse.setConsistencyLevel(queryOptions.getConsistencyLevel());
		}

		statementToUse = queryOptions.getExecutionProfileResolver().apply(statementToUse);

		if (queryOptions.getPageSize() != null) {
			statementToUse = statementToUse.setPageSize(queryOptions.getPageSize());
		}

		if (queryOptions.getSerialConsistencyLevel() != null) {
			statementToUse = statementToUse.setSerialConsistencyLevel(queryOptions.getSerialConsistencyLevel());
		}

		if (!queryOptions.getTimeout().isNegative()) {
			statementToUse = statementToUse.setTimeout(queryOptions.getTimeout());
		}

		if (queryOptions.getTracing() != null) {
			// While the following statement is null-safe, avoid setting Statement tracing if the tracing query option
			// is null since Statements are immutable and the call creates a new object.  Therefore keep the following
			// statement wrapped in the conditional null check to avoid additional garbage and added GC pressure.
			statementToUse = statementToUse.setTracing(Boolean.TRUE.equals(queryOptions.getTracing()));
		}
		if (queryOptions.getKeyspace() != null) {
			if (statementToUse instanceof BoundStatement) {
				throw new IllegalArgumentException("Keyspace cannot be set for a BoundStatement");
			}
			if (statementToUse instanceof BatchStatement) {
				statementToUse = ((BatchStatement) statementToUse).setKeyspace(queryOptions.getKeyspace());
			}
			if (statementToUse instanceof SimpleStatement) {
				statementToUse = ((SimpleStatement) statementToUse).setKeyspace(queryOptions.getKeyspace());
			}

		}

		return (T) statementToUse;
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
