/*
 * Copyright 2013-2016 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core;

import java.util.Iterator;
import java.util.List;

import org.springframework.cassandra.core.Cancellable;
import org.springframework.cassandra.core.CqlOperations;
import org.springframework.cassandra.core.QueryForObjectListener;
import org.springframework.cassandra.core.QueryOptions;
import org.springframework.cassandra.core.WriteOptions;
import org.springframework.cassandra.core.cql.CqlIdentifier;
import org.springframework.data.cassandra.convert.CassandraConverter;

import com.datastax.driver.core.querybuilder.Select;

/**
 * Operations for interacting with Cassandra. These operations are used by the Repository implementation, but can also
 * be used directly when that is desired by the developer.
 * <h3>Deprecation note</h3>
 * <p>
 * Methods accepting a {@link List} of entities perform batching operations (insert/update/delete). This can be fine for
 * entities sharing a partition key but leads in most cases to distributed batches across a Cassandra cluster which is
 * an anti-pattern. Please use {@link #batchOps()} if your intention is batching. As of Version 1.5, all methods
 * accepting a {@link List} of entities are deprecated because there is no alternative of inserting multiple rows in an
 * atomic way that guarantees not to harm Cassandra performance. These methods will be removed in Version 2.0. Please
 * issue multiple calls to the corresponding single-entity method.
 * <p>
 * {@link CassandraOperations} mixes synchronous and asynchronous methods so asynchronous methods are subject to be
 * moved into an asynchronous Cassandra template.
 * 
 * @author Alex Shvid
 * @author David Webb
 * @author Matthew Adams
 * @author Mark Paluch
 * @see CqlOperations
 * @see Select
 * @see WriteListener
 * @see DeletionListener
 * @see QueryForObjectListener
 */
public interface CassandraOperations extends CqlOperations {

	/**
	 * The table name used for the specified class by this template.
	 *
	 * @param entityClass The entity type must not be {@literal null}.
	 * @return the {@link CqlIdentifier}
	 */
	CqlIdentifier getTableName(Class<?> entityClass);

	/**
	 * Executes the given select {@code query} on the entity table of the specified {@code type} backed by a Cassandra
	 * {@link com.datastax.driver.core.ResultSet}.
	 * <p>
	 * Returns a {@link java.util.Iterator} that wraps the Cassandra {@link com.datastax.driver.core.ResultSet}.
	 *
	 * @param <T> element return type.
	 * @param query query to execute. Must not be empty or {@literal null}.
	 * @param entityClass Class type of the elements in the {@link Iterator} stream. Must not be {@literal null}.
	 * @return an {@link Iterator} (stream) over the elements in the query result set.
	 * @since 1.5
	 */
	<T> Iterator<T> stream(String query, Class<T> entityClass);

	/**
	 * Execute query and convert ResultSet to the list of entities.
	 *
	 * @param cql must not be {@literal null}.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @return the converted results
	 */
	<T> List<T> select(String cql, Class<T> entityClass);

	/**
	 * Execute the Select Query and convert to the list of entities.
	 *
	 * @param select must not be {@literal null}.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @return the converted results
	 */
	<T> List<T> select(Select select, Class<T> entityClass);

	/**
	 * Select objects for the given {@code entityClass} and {@code ids}.
	 *
	 * @param entityClass The entity type must not be {@literal null}.
	 * @param ids must not be {@literal null}.
	 * @return the converted results
	 */
	<T> List<T> selectBySimpleIds(Class<T> entityClass, Iterable<?> ids);

	/**
	 * @deprecated Calling this method could result in {@link OutOfMemoryError}, as this is a brute force selection.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @return A list of all entities of type <code>T</code>.
	 */
	@Deprecated
	<T> List<T> selectAll(Class<T> entityClass);

	/**
	 * Execute the Select by {@code id} for the given {@code entityClass}.
	 *
	 * @param entityClass The entity type must not be {@literal null}.
	 * @param id must not be {@literal null}.
	 * @return the converted object or {@literal null}.
	 */
	<T> T selectOneById(Class<T> entityClass, Object id);

	/**
	 * Execute CQL and convert ResultSet to the entity
	 *
	 * @param cql must not be {@literal null}.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @return the converted object or {@literal null}.
	 */
	<T> T selectOne(String cql, Class<T> entityClass);

	/**
	 * Execute Select query and convert ResultSet to the entity
	 *
	 * @param select must not be {@literal null}.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @return the converted object or {@literal null}.
	 */
	<T> T selectOne(Select select, Class<T> entityClass);

	/**
	 * Executes the {@link Select} query asynchronously.
	 *
	 * @param select The {@link Select} query to execute.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @return A {@link Cancellable} that can be used to cancel the query.
	 */
	<T> Cancellable selectOneAsynchronously(Select select, Class<T> entityClass, QueryForObjectListener<T> listener);

	/**
	 * Executes the string CQL query asynchronously.
	 *
	 * @param cql The string query CQL to execute.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @return A {@link Cancellable} that can be used to cancel the query.
	 */
	<T> Cancellable selectOneAsynchronously(String cql, Class<T> entityClass, QueryForObjectListener<T> listener);

	/**
	 * Executes the {@link Select} query asynchronously.
	 *
	 * @param select The {@link Select} query to execute.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @param options The {@link QueryOptions} to use.
	 * @return A {@link Cancellable} that can be used to cancel the query.
	 */
	<T> Cancellable selectOneAsynchronously(Select select, Class<T> entityClass, QueryForObjectListener<T> listener,
			QueryOptions options);

	/**
	 * Executes the string CQL query asynchronously.
	 *
	 * @param cql The string query CQL to execute.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @param options The {@link QueryOptions} to use.
	 * @return A {@link Cancellable} that can be used to cancel the query.
	 */
	<T> Cancellable selectOneAsynchronously(String cql, Class<T> entityClass, QueryForObjectListener<T> listener,
			QueryOptions options);

	/**
	 * Determine whether the row {@code entityClass} with the given {@code id} exists.
	 *
	 * @param entityClass The entity type must not be {@literal null}.
	 * @param id must not be {@literal null}.
	 * @return true, if the object exists
	 */
	boolean exists(Class<?> entityClass, Object id);

	/**
	 * Returns the number of rows for the given {@code entityClass} by querying the table of the given entity class.
	 *
	 * @param entityClass The entity type must not be {@literal null}.
	 * @return number of rows
	 */
	long count(Class<?> entityClass);

	/**
	 * Insert the given entity.
	 *
	 * @param entity The entity to insert
	 * @return The entity given
	 */
	<T> T insert(T entity);

	/**
	 * Insert the given entity.
	 *
	 * @param entity The entity to insert
	 * @param options The {@link WriteOptions} to use.
	 * @return The entity given
	 */
	<T> T insert(T entity, WriteOptions options);

	/**
	 * Insert the given list of entities.
	 *
	 * @param entities The entities to insert.
	 * @return The entities given.
	 * @deprecated as of 1.5. This method accepts a {@link List} of entities and inserts all entities in a batch. That's
	 *             not transparent to users and a Cassandra anti-pattern if used with multiple partition keys. Please use
	 *             {@link #batchOps()} for if your intent is batching or issue multiple calls to {@link #insert(Object)}
	 *             as that's the preferred approach. This method will be removed in Version 2.0.
	 */
	@Deprecated
	<T> List<T> insert(List<T> entities);

	/**
	 * Insert the given list of entities.
	 *
	 * @param entities The entities to insert.
	 * @param options The {@link WriteOptions} to use.
	 * @return The entities given.
	 * @deprecated as of 1.5. This method accepts a {@link List} of entities and inserts all entities in a batch. That's
	 *             not transparent to users and a Cassandra anti-pattern if used with multiple partition keys. Please use
	 *             {@link #batchOps()} for if your intent is batching or issue multiple calls to
	 *             {@link #insert(Object, WriteOptions)} as that's the preferred approach. This method will be removed in
	 *             Version 2.0.
	 */
	@Deprecated
	<T> List<T> insert(List<T> entities, WriteOptions options);

	/**
	 * Inserts the given entity asynchronously.
	 *
	 * @param entity The entity to insert
	 * @return The entity given
	 * @see #insertAsynchronously(Object, WriteListener)
	 * @deprecated as of 1.2, this method does not allow for query cancellation or notification of completion. Favor
	 *             {@link #insertAsynchronously(Object, WriteListener)}.
	 */
	@Deprecated
	<T> T insertAsynchronously(T entity);

	/**
	 * Inserts the given entity asynchronously.
	 *
	 * @param entity The entity to insert
	 * @return The entity given
	 * @see #insertAsynchronously(Object, WriteOptions)
	 * @deprecated as of 1.2, this method does not allow for query cancellation or notification of completion. Favor
	 *             {@link #insertAsynchronously(Object, WriteListener, WriteOptions)}.
	 */
	@Deprecated
	<T> T insertAsynchronously(T entity, WriteOptions options);

	/**
	 * Inserts the given entity asynchronously.
	 *
	 * @param entity The entity to insert
	 * @param listener The listener to receive notification of completion
	 * @return A {@link Cancellable} enabling the cancellation of the operation
	 */
	<T> Cancellable insertAsynchronously(T entity, WriteListener<T> listener);

	/**
	 * Inserts the given entity asynchronously.
	 *
	 * @param entity The entity to insert
	 * @param listener The listener to receive notification of completion
	 * @param options The {@link WriteOptions} to use
	 * @return A {@link Cancellable} enabling the cancellation of the operation
	 */
	<T> Cancellable insertAsynchronously(T entity, WriteListener<T> listener, WriteOptions options);

	/**
	 * Inserts the given entities asynchronously in a batch.
	 *
	 * @param entities The entities to insert
	 * @return The entities given
	 * @see #insertAsynchronously(List, WriteListener)
	 * @deprecated as of 1.2, this method does not allow for query cancellation or notification of completion. Favor
	 *             {@link #insertAsynchronously(List, WriteListener)}.
	 */
	@Deprecated
	<T> List<T> insertAsynchronously(List<T> entities);

	/**
	 * Inserts the given entities asynchronously in a batch.
	 *
	 * @param entities The entities to insert
	 * @return The entities given
	 * @see #insertAsynchronously(List, WriteListener, WriteOptions)
	 * @deprecated as of 1.2, this method does not allow for query cancellation or notification of completion. Favor
	 *             {@link #insertAsynchronously(List, WriteListener, WriteOptions)}.
	 */
	@Deprecated
	<T> List<T> insertAsynchronously(List<T> entities, WriteOptions options);

	/**
	 * Inserts the given entities asynchronously in a batch.
	 *
	 * @param entities The entities to insert
	 * @param listener The listener to receive notification of completion
	 * @return A {@link Cancellable} enabling the cancellation of the operation
	 * @deprecated as of 1.5. This method accepts a {@link List} of entities and deletes all entities in a batch. That's
	 *             not transparent to users and a Cassandra anti-pattern if used with multiple partition keys. This method
	 *             will be removed in Version 2.0.
	 */
	@Deprecated
	<T> Cancellable insertAsynchronously(List<T> entities, WriteListener<T> listener);

	/**
	 * Inserts the given entities asynchronously in a batch.
	 *
	 * @param entities The entities to insert
	 * @param listener The listener to receive notification of completion
	 * @param options The {@link WriteOptions} to use
	 * @return A {@link Cancellable} enabling the cancellation of the operation
	 * @deprecated as of 1.5. This method accepts a {@link List} of entities and deletes all entities in a batch. That's
	 *             not transparent to users and a Cassandra anti-pattern if used with multiple partition keys. This method
	 *             will be removed in Version 2.0.
	 */
	@Deprecated
	<T> Cancellable insertAsynchronously(List<T> entities, WriteListener<T> listener, WriteOptions options);

	/**
	 * Update the given entity.
	 *
	 * @param entity The entity to update
	 * @return The entity given
	 */
	<T> T update(T entity);

	/**
	 * Update the given entity.
	 *
	 * @param entity The entity to update
	 * @param options The {@link WriteOptions} to use.
	 * @return The entity given
	 */
	<T> T update(T entity, WriteOptions options);

	/**
	 * Update the given list of entities.
	 *
	 * @param entities The entities to update.
	 * @return The entities given.
	 * @deprecated as of 1.5. This method accepts a {@link List} of entities and updates all entities in a batch. That's
	 *             not transparent to users and a Cassandra anti-pattern if used with multiple partition keys. Please use
	 *             {@link #batchOps()} for if your intent is batching or issue multiple calls to {@link #update(Object)}
	 *             as that's the preferred approach. This method will be removed in Version 2.0.
	 */
	@Deprecated
	<T> List<T> update(List<T> entities);

	/**
	 * Update the given list of entities.
	 *
	 * @param entities The entities to update.
	 * @param options The {@link WriteOptions} to use.
	 * @return The entities given.
	 * @deprecated as of 1.5. This method accepts a {@link List} of entities and updates all entities in a batch. That's
	 *             not transparent to users and a Cassandra anti-pattern if used with multiple partition keys. Please use
	 *             {@link #batchOps()} for if your intent is batching or issue multiple calls to
	 *             {@link #update(Object, WriteOptions)} as that's the preferred approach. This method will be removed in
	 *             Version 2.0.
	 */
	@Deprecated
	<T> List<T> update(List<T> entities, WriteOptions options);

	/**
	 * Updates the given entity asynchronously.
	 *
	 * @param entity The entity to update
	 * @return The entity given
	 * @see #updateAsynchronously(Object, WriteListener)
	 * @deprecated as of 1.2, this method does not allow for query cancellation or notification of completion. Favor
	 *             {@link #updateAsynchronously(Object, WriteListener)}.
	 */
	@Deprecated
	<T> T updateAsynchronously(T entity);

	/**
	 * Updates the given entity asynchronously.
	 *
	 * @param entity The entity to update
	 * @return The entity given
	 * @see #updateAsynchronously(Object, WriteOptions)
	 * @deprecated as of 1.2, this method does not allow for query cancellation or notification of completion. Favor
	 *             {@link #updateAsynchronously(Object, WriteListener, WriteOptions)}.
	 */
	@Deprecated
	<T> T updateAsynchronously(T entity, WriteOptions options);

	/**
	 * Updates the given entity asynchronously.
	 *
	 * @param entity The entity to update
	 * @param listener The listener to receive notification of completion
	 * @return A {@link Cancellable} enabling the cancellation of the operation
	 */
	<T> Cancellable updateAsynchronously(T entity, WriteListener<T> listener);

	/**
	 * Updates the given entity asynchronously.
	 *
	 * @param entity The entity to update
	 * @param listener The listener to receive notification of completion
	 * @param options The {@link WriteOptions} to use
	 * @return A {@link Cancellable} enabling the cancellation of the operation
	 */
	<T> Cancellable updateAsynchronously(T entity, WriteListener<T> listener, WriteOptions options);

	/**
	 * Updates the given entities asynchronously in a batch.
	 *
	 * @param entities The entities to update
	 * @return The entities given
	 * @see #updateAsynchronously(List, WriteListener)
	 * @deprecated as of 1.2, this method does not allow for query cancellation or notification of completion. Favor
	 *             {@link #updateAsynchronously(List, WriteListener)}.
	 */
	@Deprecated
	<T> List<T> updateAsynchronously(List<T> entities);

	/**
	 * Updates the given entities asynchronously in a batch.
	 *
	 * @param entities The entities to update
	 * @return The entities given
	 * @see #updateAsynchronously(List, WriteListener, WriteOptions)
	 * @deprecated as of 1.2, this method does not allow for query cancellation or notification of completion. Favor
	 *             {@link #updateAsynchronously(List, WriteListener, WriteOptions)}.
	 */
	@Deprecated
	<T> List<T> updateAsynchronously(List<T> entities, WriteOptions options);

	/**
	 * Updates the given entities asynchronously in a batch.
	 *
	 * @param entities The entities to update
	 * @param listener The listener to receive notification of completion
	 * @return A {@link Cancellable} enabling the cancellation of the operation
	 * @deprecated as of 1.5. This method accepts a {@link List} of entities and deletes all entities in a batch. That's
	 *             not transparent to users and a Cassandra anti-pattern if used with multiple partition keys. This method
	 *             will be removed in Version 2.0.
	 */
	@Deprecated
	<T> Cancellable updateAsynchronously(List<T> entities, WriteListener<T> listener);

	/**
	 * Updates the given entities asynchronously in a batch.
	 *
	 * @param entities The entities to update
	 * @param listener The listener to receive notification of completion
	 * @param options The {@link WriteOptions} to use
	 * @return A {@link Cancellable} enabling the cancellation of the operation
	 * @deprecated as of 1.5. This method accepts a {@link List} of entities and deletes all entities in a batch. That's
	 *             not transparent to users and a Cassandra anti-pattern if used with multiple partition keys. This method
	 *             will be removed in Version 2.0.
	 */
	@Deprecated
	<T> Cancellable updateAsynchronously(List<T> entities, WriteListener<T> listener, WriteOptions options);

	/**
	 * Remove the given object from the table by id.
	 *
	 * @param entityClass The entity type must not be {@literal null}.
	 * @param id must not be {@literal null}.
	 */
	void deleteById(Class<?> entityClass, Object id);

	/**
	 * Remove the given object from the table by id.
	 *
	 * @param entity must not be {@literal null}.
	 */
	<T> void delete(T entity);

	/**
	 * Remove the given object from the table by id.
	 *
	 * @param entity must not be {@literal null}.
	 * @param options may be {@literal null}.
	 */
	<T> void delete(T entity, QueryOptions options);

	/**
	 * Remove the given objects from the table by id.
	 *
	 * @param entities must not be {@literal null}.
	 * @deprecated as of 1.5. This method accepts a {@link List} of entities and deletes all entities in a batch. That's
	 *             not transparent to users and a Cassandra anti-pattern if used with multiple partition keys. Please use
	 *             {@link #batchOps()} for if your intent is batching or issue multiple calls to {@link #delete(Object)}
	 *             as that's the preferred approach. This method will be removed in Version 2.0.
	 */
	@Deprecated
	<T> void delete(List<T> entities);

	/**
	 * Remove the given objects from the table by id.
	 *
	 * @param entities must not be {@literal null}.
	 * @param options may be {@literal null}.
	 * @deprecated as of 1.5. This method accepts a {@link List} of entities and deletes all entities in a batch. That's
	 *             not transparent to users and a Cassandra anti-pattern if used with multiple partition keys. Please use
	 *             {@link #batchOps()} for if your intent is batching or issue multiple calls to
	 *             {@link #delete(Object, WriteOptions)} as that's the preferred approach. This method will be removed in
	 *             Version 2.0.
	 */
	@Deprecated
	<T> void delete(List<T> entities, QueryOptions options);

	/**
	 * Deletes all entities of a given class.
	 * 
	 * @param entityClass The entity type must not be {@literal null}.
	 */
	<T> void deleteAll(Class<T> entityClass);

	/**
	 * Remove the given object from the table by id.
	 *
	 * @param entity The object to delete
	 */
	<T> Cancellable deleteAsynchronously(T entity);

	/**
	 * Remove the given object from the table by id.
	 *
	 * @param entity The object to delete
	 * @param options The {@link QueryOptions} to use
	 */
	<T> Cancellable deleteAsynchronously(T entity, QueryOptions options);

	/**
	 * Remove the given object from the table by id.
	 *
	 * @param entity The object to delete
	 * @param listener The {@link DeletionListener} to receive notification upon completion
	 */
	<T> Cancellable deleteAsynchronously(T entity, DeletionListener<T> listener);

	/**
	 * Remove the given object from the table by id.
	 *
	 * @param entity The object to delete
	 * @param listener The {@link DeletionListener} to receive notification upon completion
	 * @param options The {@link QueryOptions} to use
	 */
	<T> Cancellable deleteAsynchronously(T entity, DeletionListener<T> listener, QueryOptions options);

	/**
	 * Remove the given objects from the table by id.
	 *
	 * @param entities The objects to delete
	 * @deprecated as of 1.5. This method accepts a {@link List} of entities and deletes all entities in a batch. That's
	 *             not transparent to users and a Cassandra anti-pattern if used with multiple partition keys. This method
	 *             will be removed in Version 2.0.
	 */
	@Deprecated
	<T> Cancellable deleteAsynchronously(List<T> entities);

	/**
	 * Remove the given objects from the table by id.
	 *
	 * @param entities The objects to delete
	 * @param listener The {@link DeletionListener} to receive notification upon completion
	 * @deprecated as of 1.5. This method accepts a {@link List} of entities and deletes all entities in a batch. That's
	 *             not transparent to users and a Cassandra anti-pattern if used with multiple partition keys. This method
	 *             will be removed in Version 2.0.
	 */
	@Deprecated
	<T> Cancellable deleteAsynchronously(List<T> entities, DeletionListener<T> listener);

	/**
	 * Remove the given objects from the table by id.
	 *
	 * @param entities The objects to delete
	 * @param options The {@link QueryOptions} to use
	 * @deprecated as of 1.5. This method accepts a {@link List} of entities and deletes all entities in a batch. That's
	 *             not transparent to users and a Cassandra anti-pattern if used with multiple partition keys. This method
	 *             will be removed in Version 2.0.
	 */
	@Deprecated
	<T> Cancellable deleteAsynchronously(List<T> entities, QueryOptions options);

	/**
	 * Remove the given objects from the table by id.
	 *
	 * @param entities The objects to delete
	 * @param listener The {@link DeletionListener} to receive notification upon completion
	 * @param options The {@link QueryOptions} to use
	 * @deprecated as of 1.5. This method accepts a {@link List} of entities and deletes all entities in a batch. That's
	 *             not transparent to users and a Cassandra anti-pattern if used with multiple partition keys. This method
	 *             will be removed in Version 2.0.
	 */
	@Deprecated
	<T> Cancellable deleteAsynchronously(List<T> entities, DeletionListener<T> listener, QueryOptions options);

	/**
	 * Returns a new {@link CassandraBatchOperations}. Each {@link CassandraBatchOperations} instance can be executed only
	 * once so you might want to obtain new {@link CassandraBatchOperations} instances for each batch.
	 *
	 * @return a new {@link CassandraBatchOperations} associated with the given entity class.
	 */
	CassandraBatchOperations batchOps();

	/**
	 * Returns the underlying {@link CassandraConverter}.
	 *
	 * @return the underlying {@link CassandraConverter}.
	 */
	CassandraConverter getConverter();

}
