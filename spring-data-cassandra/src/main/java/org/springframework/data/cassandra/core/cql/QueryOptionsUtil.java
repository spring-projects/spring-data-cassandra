/*
 * Copyright 2016-present the original author or authors.
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
import java.util.function.BiFunction;

import org.jspecify.annotations.Nullable;

import org.springframework.data.cassandra.core.cql.util.Bindings;
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.querybuilder.BindMarker;
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
 * @author Sam Lightfoot
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

		if (queryOptions.isIdempotent() != null) {
			statementToUse = statementToUse.setIdempotent(queryOptions.isIdempotent());
		}

		if (queryOptions.getPageSize() != null) {
			statementToUse = statementToUse.setPageSize(queryOptions.getPageSize());
		}

		if (queryOptions.getRoutingKeyspace() != null) {
			statementToUse = statementToUse.setRoutingKeyspace(queryOptions.getRoutingKeyspace());
		}

		if (queryOptions.getRoutingKey() != null) {
			statementToUse = statement.setRoutingKey(queryOptions.getRoutingKey());
		}

		if (queryOptions.getSerialConsistencyLevel() != null) {
			statementToUse = statementToUse.setSerialConsistencyLevel(queryOptions.getSerialConsistencyLevel());
		}

		if (!queryOptions.getTimeout().isNegative()) {
			statementToUse = statementToUse.setTimeout(queryOptions.getTimeout());
		}

		if (queryOptions.getTracing() != null) {
			// While the following statement is null-safe, avoid setting Statement tracing if the tracing query option
			// is null since Statements are immutable and the call creates a new object. Therefore keep the following
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
	@SuppressWarnings("NullAway")
	public static Insert addWriteOptions(Insert insert, WriteOptions writeOptions) {

		Assert.notNull(insert, "Insert must not be null");
		Assert.notNull(writeOptions, "WriteOptions must not be null");

		Insert insertToUse = insert;

		if (writeOptions.getTimestamp() != null) {
			insertToUse = insertToUse.usingTimestamp(writeOptions.getTimestamp());
		}

		if (hasTtl(writeOptions.getTtl())) {
			insertToUse = insertToUse.usingTtl(Math.toIntExact(writeOptions.getTtl().getSeconds()));
		}

		return insertToUse;
	}

	/**
	 * Add common {@link WriteOptions} options to {@link Update} CQL statements.
	 *
	 * @param update {@link Update} CQL statement, must not be {@literal null}.
	 * @param writeOptions write options (e.g. consistency level) to add to the CQL statement.
	 * @return the given {@link Update}.
	 */
	@SuppressWarnings("NullAway")
	public static Update addWriteOptions(Update update, WriteOptions writeOptions) {

		Assert.notNull(update, "Update must not be null");
		Assert.notNull(writeOptions, "WriteOptions must not be null");

		if (writeOptions.getTimestamp() != null) {
			update = (Update) ((UpdateStart) update).usingTimestamp(writeOptions.getTimestamp());
		}

		if (hasTtl(writeOptions.getTtl())) {
			update = (Update) ((UpdateStart) update).usingTtl(getTtlSeconds(writeOptions.getTtl()));
		}

		return update;
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
	 * Add common {@link WriteOptions} options to CQL statements through {@link CqlStatementOptionsAccessor}.
	 *
	 * @param accessor CQL statement accessor, must not be {@literal null}.
	 * @param writeOptions write options (e.g. consistency level) to add to the CQL statement.
	 * @return the resulting statement.
	 * @since 4.2
	 */
	@SuppressWarnings("NullAway")
	public static <T> T addWriteOptions(CqlStatementOptionsAccessor<T> accessor, WriteOptions writeOptions) {

		Assert.notNull(accessor, "CqlStatementOptionsAccessor must not be null");
		Assert.notNull(writeOptions, "WriteOptions must not be null");

		if (writeOptions.getTimestamp() != null) {
			accessor.usingTimestamp(writeOptions.getTimestamp());
		}

		if (hasTtl(writeOptions.getTtl())) {
			accessor.usingTtl(getTtlSeconds(writeOptions.getTtl()));
		}

		return accessor.getStatement();
	}

	private static int getTtlSeconds(Duration ttl) {
		return Math.toIntExact(ttl.getSeconds());
	}

	private static boolean hasTtl(@Nullable Duration ttl) {
		return ttl != null && !ttl.isNegative();
	}

	/**
	 * Wrapper for common options used with CQL statements that are represented in the CQL statement such as TTL and
	 * timestamp.
	 *
	 * @param <T>
	 * @since 4.2
	 */
	public static abstract class CqlStatementOptionsAccessor<T> {

		/**
		 * Set the timestamp to the underlying statement.
		 *
		 * @param timestamp the timestamp value to use.
		 */
		abstract void usingTimestamp(long timestamp);

		/**
		 * Set the TTL to the underlying statement.
		 *
		 * @param ttl the TTL value to use.
		 */
		abstract void usingTtl(int ttl);

		/**
		 * Returns the current statement instance.
		 *
		 * @return the current statement instance.
		 */
		abstract T getStatement();

		/**
		 * Creates an accessor variant that captures options through {@link BindMarker} for {@link Insert}.
		 *
		 * @param bindings
		 * @param statement
		 * @return
		 */
		public static CqlStatementOptionsAccessor<Insert> ofInsert(Bindings bindings, Insert statement) {
			return new BoundOptionsAccessor<>(bindings, statement, Insert::usingTimestamp, Insert::usingTtl);
		}

		/**
		 * Creates an accessor variant that applies options directly within the CQL statement for {@link Insert}.
		 *
		 * @param statement
		 * @return
		 */
		public static CqlStatementOptionsAccessor<Insert> ofInsert(Insert statement) {
			return new InlineOptionsAccessor<>(statement, Insert::usingTimestamp, Insert::usingTtl);
		}

		/**
		 * Creates an accessor variant that captures options through {@link BindMarker} for {@link Update}.
		 *
		 * @param bindings
		 * @param statement
		 * @return
		 */
		public static CqlStatementOptionsAccessor<UpdateStart> ofUpdate(Bindings bindings, UpdateStart statement) {
			return new BoundOptionsAccessor<>(bindings, statement, UpdateStart::usingTimestamp, UpdateStart::usingTtl);
		}

		/**
		 * Creates an accessor variant that applies options directly within the CQL statement for {@link Update}.
		 *
		 * @param statement
		 * @return
		 */
		public static CqlStatementOptionsAccessor<UpdateStart> ofUpdate(UpdateStart statement) {
			return new InlineOptionsAccessor<>(statement, UpdateStart::usingTimestamp, UpdateStart::usingTtl);
		}

		/**
		 * Creates an accessor variant that captures options through {@link BindMarker} for {@link Delete}.
		 *
		 * @param bindings
		 * @param statement
		 * @return
		 */
		public static CqlStatementOptionsAccessor<DeleteSelection> ofDelete(Bindings bindings, DeleteSelection statement) {
			return new BoundOptionsAccessor<>(bindings, statement, DeleteSelection::usingTimestamp,
					(deleteSelection, bindMarker) -> deleteSelection);
		}

		/**
		 * Creates an accessor variant that applies options directly within the CQL statement for {@link Delete}.
		 *
		 * @param statement
		 * @return
		 */
		public static CqlStatementOptionsAccessor<DeleteSelection> ofDelete(DeleteSelection statement) {
			return new InlineOptionsAccessor<>(statement, DeleteSelection::usingTimestamp,
					(deleteSelection, bindMarker) -> deleteSelection);
		}

	}

	/**
	 * Accessor variant that uses bind markers.
	 *
	 * @param <T>
	 */
	private static class BoundOptionsAccessor<T> extends CqlStatementOptionsAccessor<T> {

		private final Bindings bindings;
		private T instance;

		private final BiFunction<T, BindMarker, T> timestampFunction;

		private final BiFunction<T, BindMarker, T> ttlFunction;

		private BoundOptionsAccessor(Bindings bindings, T instance, BiFunction<T, BindMarker, T> timestampFunction,
				BiFunction<T, BindMarker, T> ttlFunction) {
			this.bindings = bindings;
			this.instance = instance;
			this.timestampFunction = timestampFunction;
			this.ttlFunction = ttlFunction;
		}

		@Override
		void usingTimestamp(long timestamp) {

			BindMarker bindMarker = bindings.bind(timestamp);
			instance = timestampFunction.apply(instance, bindMarker);
		}

		@Override
		void usingTtl(int ttl) {
			BindMarker bindMarker = bindings.bind(ttl);
			instance = ttlFunction.apply(instance, bindMarker);
		}

		@Override
		T getStatement() {
			return instance;
		}

	}

	/**
	 * Accessor variant that uses inline values.
	 *
	 * @param <T>
	 */
	private static class InlineOptionsAccessor<T> extends CqlStatementOptionsAccessor<T> {

		private T instance;

		private final TimestampFunction<T> timestampFunction;

		private final TtlFunction<T> ttlFunction;

		private InlineOptionsAccessor(T instance, TimestampFunction<T> timestampFunction, TtlFunction<T> ttlFunction) {
			this.instance = instance;
			this.timestampFunction = timestampFunction;
			this.ttlFunction = ttlFunction;
		}

		@Override
		void usingTimestamp(long timestamp) {
			instance = timestampFunction.apply(instance, timestamp);
		}

		@Override
		void usingTtl(int ttl) {
			instance = ttlFunction.apply(instance, ttl);
		}

		@Override
		T getStatement() {
			return instance;
		}

		/**
		 * Bi-function accepting a statement and {@code long} timestamp returning the modified statement.
		 *
		 * @param <T>
		 */
		interface TimestampFunction<T> {

			T apply(T statement, long timestamp);
		}

		/**
		 * Bi-function accepting a statement and {@code int} TTL returning the modified statement.
		 *
		 * @param <T>
		 */
		interface TtlFunction<T> {

			T apply(T statement, int ttl);
		}

	}

}
