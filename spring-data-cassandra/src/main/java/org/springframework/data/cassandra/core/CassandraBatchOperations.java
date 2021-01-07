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
package org.springframework.data.cassandra.core;

import org.springframework.data.cassandra.core.cql.WriteOptions;

/**
 * Batch operations for insert/update/delete actions on a table. {@link CassandraBatchOperations} use logged Cassandra
 * {@code BATCH}es for single entities and collections of entities. A {@link CassandraBatchOperations} instance cannot
 * be modified/used once it was executed.
 * <p>
 * Batches are atomic by default. In the context of a Cassandra batch operation, atomic means that if any of the batch
 * succeeds, all of it will. Statement order does not matter within a batch. {@link CassandraBatchOperations} applies
 * all rows using the same {@link #withTimestamp(long) timestamp} if supplied, otherwise Cassandra will generate a
 * timestamp.
 * <p>
 * Multi partition batches should only be used to achieve atomicity for a few writes on different tables. Apart from
 * this they should be avoided because they’re too expensive. Single partition batches can be used to get atomicity and
 * isolation, they're not much more expensive than normal writes.
 *
 * @author Mark Paluch
 * @author Anup Sabbi
 * @since 1.5
 */
public interface CassandraBatchOperations {

	/**
	 * Execute the batch. The batch can be executed only once.
	 *
	 * @return the {@link WriteResult} for the bulk operation.
	 * @throws IllegalStateException if the batch is executed after it was executed already.
	 */
	WriteResult execute();

	/**
	 * Apply a given {@code timestamp} to the whole batch.
	 *
	 * @param timestamp the timestamp to apply.
	 * @return {@code this} {@link CassandraBatchOperations}.
	 * @throws IllegalStateException if the batch was already executed.
	 */
	CassandraBatchOperations withTimestamp(long timestamp);

	/**
	 * Add an array of inserts to the batch.
	 *
	 * @param entities the entities to insert; must not be {@literal null}.
	 * @return {@code this} {@link CassandraBatchOperations}.
	 * @throws IllegalStateException if the batch was already executed.
	 */
	CassandraBatchOperations insert(Object... entities);

	/**
	 * Add a collection of inserts to the batch.
	 *
	 * @param entities the entities to insert; must not be {@literal null}.
	 * @return {@code this} {@link CassandraBatchOperations}.
	 * @throws IllegalStateException if the batch was already executed.
	 */
	CassandraBatchOperations insert(Iterable<?> entities);

	/**
	 * Add a collection of inserts with given {@link WriteOptions} to the batch.
	 *
	 * @param entities the entities to insert; must not be {@literal null}.
	 * @param options the WriteOptions to apply; must not be {@literal null}.
	 * @return {@code this} {@link CassandraBatchOperations}.
	 * @throws IllegalStateException if the batch was already executed.
	 * @since 2.0
	 * @see InsertOptions
	 */
	CassandraBatchOperations insert(Iterable<?> entities, WriteOptions options);

	/**
	 * Add an array of updates to the batch.
	 *
	 * @param entities the entities to update; must not be {@literal null}.
	 * @return {@code this} {@link CassandraBatchOperations}.
	 * @throws IllegalStateException if the batch was already executed.
	 */
	CassandraBatchOperations update(Object... entities);

	/**
	 * Add a collection of updates to the batch.
	 *
	 * @param entities the entities to update; must not be {@literal null}.
	 * @return {@code this} {@link CassandraBatchOperations}.
	 * @throws IllegalStateException if the batch was already executed.
	 */
	CassandraBatchOperations update(Iterable<?> entities);

	/**
	 * Add a collection of updates with given {@link WriteOptions} to the batch.
	 *
	 * @param entities the entities to update; must not be {@literal null}.
	 * @param options the WriteOptions to apply; must not be {@literal null}.
	 * @return {@code this} {@link CassandraBatchOperations}.
	 * @throws IllegalStateException if the batch was already executed.
	 * @since 2.0
	 * @see UpdateOptions
	 */
	CassandraBatchOperations update(Iterable<?> entities, WriteOptions options);

	/**
	 * Add an array of deletes to the batch.
	 *
	 * @param entities the entities to delete; must not be {@literal null}.
	 * @return {@code this} {@link CassandraBatchOperations}.
	 * @throws IllegalStateException if the batch was already executed.
	 */
	CassandraBatchOperations delete(Object... entities);

	/**
	 * Add a collection of deletes to the batch.
	 *
	 * @param entities the entities to delete; must not be {@literal null}.
	 * @return {@code this} {@link CassandraBatchOperations}.
	 * @throws IllegalStateException if the batch was already executed.
	 */
	CassandraBatchOperations delete(Iterable<?> entities);

	/**
	 * Add a collection of deletes with given {@link WriteOptions} to the batch.
	 *
	 * @param entities the entities to delete; must not be {@literal null}.
	 * @param options the WriteOptions to apply; must not be {@literal null}.
	 * @return {@code this} {@link CassandraBatchOperations}.
	 * @throws IllegalStateException if the batch was already executed.
	 * @since 2.2
	 * @see DeleteOptions
	 */
	CassandraBatchOperations delete(Iterable<?> entities, WriteOptions options);
}
