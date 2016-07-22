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
package org.springframework.data.cassandra.core;

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
 * @since 1.5
 */
public interface CassandraBatchOperations {

	/**
	 * Execute the batch. The batch can be executed only once.
	 * 
	 * @throws IllegalStateException if the batch is executed after it was executed once
	 */
	void execute();

	/**
	 * Apply a given {@code timestamp} to the whole batch.
	 *
	 * @param timestamp the timestamp to apply.
	 * @return {@code this} {@link CassandraBatchOperations}
	 * @throws IllegalStateException if the batch was already executed
	 */
	CassandraBatchOperations withTimestamp(long timestamp);

	/**
	 * Add a single insert to the batch.
	 *
	 * @param entity the entity to insert, must not be {@literal null}.
	 * @return {@code this} {@link CassandraBatchOperations}
	 * @throws IllegalStateException if the batch was already executed
	 */
	CassandraBatchOperations insert(Object entity);

	/**
	 * Add a collection of inserts to the batch.
	 * 
	 * @param entities the entities to insert, must not be {@literal null}.
	 * @return {@code this} {@link CassandraBatchOperations}
	 * @throws IllegalStateException if the batch was already executed
	 */
	CassandraBatchOperations insert(Iterable<? extends Object> entities);

	/**
	 * Add a single update to the batch.
	 * 
	 * @param entity the entity to update, must not be {@literal null}.
	 * @return {@code this} {@link CassandraBatchOperations}
	 * @throws IllegalStateException if the batch was already executed
	 */
	CassandraBatchOperations update(Object entity);

	/**
	 * Add a collection of updates to the batch.
	 * 
	 * @param entities the entities to insert, must not be {@literal null}.
	 * @return {@code this} {@link CassandraBatchOperations}
	 * @throws IllegalStateException if the batch was already executed
	 */
	CassandraBatchOperations update(Iterable<? extends Object> entities);

	/**
	 * Add a single delete to the batch.
	 * 
	 * @param entity the entity to delete, must not be {@literal null}.
	 * @return {@code this} {@link CassandraBatchOperations}
	 * @throws IllegalStateException if the batch was already executed
	 */
	CassandraBatchOperations delete(Object entity);

	/**
	 * Add a collection of deletes to the batch.
	 * 
	 * @param entities the entities to delete, must not be {@literal null}.
	 * @return {@code this} {@link CassandraBatchOperations}
	 * @throws IllegalStateException if the batch was already executed
	 */
	CassandraBatchOperations delete(Iterable<? extends Object> entities);
}
