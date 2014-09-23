/*
 * Copyright 2013-2014 the original author or authors
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

import java.util.List;

import org.springframework.cassandra.core.CqlOperations;
import org.springframework.cassandra.core.Cancellable;
import org.springframework.cassandra.core.QueryForObjectListener;
import org.springframework.cassandra.core.QueryOptions;
import org.springframework.cassandra.core.WriteOptions;
import org.springframework.cassandra.core.cql.CqlIdentifier;
import org.springframework.data.cassandra.convert.CassandraConverter;

import com.datastax.driver.core.querybuilder.Select;

/**
 * Operations for interacting with Cassandra. These operations are used by the Repository implementation, but can also
 * be used directly when that is desired by the developer.
 * 
 * @author Alex Shvid
 * @author David Webb
 * @author Matthew Adams
 */
public interface CassandraOperations extends CqlOperations {

	/**
	 * The table name used for the specified class by this template.
	 * 
	 * @param entityClass must not be {@literal null}.
	 * @return
	 */
	CqlIdentifier getTableName(Class<?> entityClass);

	/**
	 * Execute query and convert ResultSet to the list of entities
	 * 
	 * @param query must not be {@literal null}.
	 * @param type must not be {@literal null}, mapped entity type.
	 * @return
	 */
	<T> List<T> select(String cql, Class<T> type);

	/**
	 * Execute the Select Query and convert to the list of entities
	 * 
	 * @param select must not be {@literal null}.
	 * @param type must not be {@literal null}, mapped entity type.
	 * @return
	 */
	<T> List<T> select(Select select, Class<T> type);

	<T> T selectOneById(Class<T> type, Object id);

	/**
	 * Execute CQL and convert ResultSet to the entity
	 * 
	 * @param query must not be {@literal null}.
	 * @param type must not be {@literal null}, mapped entity type.
	 * @return
	 */
	<T> T selectOne(String cql, Class<T> type);

	/**
	 * Executes the {@link Select} query asynchronously.
	 * 
	 * @param select The {@link Select} query to execute.
	 * @param type The type of entity to retrieve.
	 * @return A {@link Cancellable} that can be used to cancel the query.
	 */
	<T> Cancellable selectOneAsynchronously(Select select, Class<T> type, QueryForObjectListener<T> listener);

	/**
	 * Executes the string CQL query asynchronously.
	 * 
	 * @param select The string query CQL to execute.
	 * @param type The type of entity to retrieve.
	 * @return A {@link Cancellable} that can be used to cancel the query.
	 */
	<T> Cancellable selectOneAsynchronously(String cql, Class<T> type, QueryForObjectListener<T> listener);

	/**
	 * Executes the {@link Select} query asynchronously.
	 * 
	 * @param select The {@link Select} query to execute.
	 * @param type The type of entity to retrieve.
	 * @param options The {@link QueryOptions} to use.
	 * @return A {@link Cancellable} that can be used to cancel the query.
	 */
	<T> Cancellable selectOneAsynchronously(Select select, Class<T> type, QueryForObjectListener<T> listener,
			QueryOptions options);

	/**
	 * Executes the string CQL query asynchronously.
	 * 
	 * @param select The string query CQL to execute.
	 * @param type The type of entity to retrieve.
	 * @param options The {@link QueryOptions} to use.
	 * @return A {@link Cancellable} that can be used to cancel the query.
	 */
	<T> Cancellable selectOneAsynchronously(String cql, Class<T> type, QueryForObjectListener<T> listener,
			QueryOptions options);

	/**
	 * Execute Select query and convert ResultSet to the entity
	 * 
	 * @param query must not be {@literal null}.
	 * @param type must not be {@literal null}, mapped entity type.
	 * @return
	 */
	<T> T selectOne(Select select, Class<T> type);

	boolean exists(Class<?> type, Object id);

	long count(Class<?> type);

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
	 */
	<T> List<T> insert(List<T> entities);

	/**
	 * Insert the given list of entities.
	 * 
	 * @param entities The entities to insert.
	 * @param options The {@link WriteOptions} to use.
	 * @return The entities given.
	 */
	<T> List<T> insert(List<T> entities, WriteOptions options);

	/**
	 * Inserts the given entity asynchronously.
	 * 
	 * @param entity The entity to insert
	 * @return The entity given
	 * @see #insertAsynchronously(Object, WriteListener)
	 * @deprecated This method does not allow for query cancellation or notification of completion. Favor
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
	 * @deprecated This method does not allow for query cancellation or notification of completion. Favor
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
	 * @param entity The entities to insert
	 * @return The entities given
	 * @see #insertAsynchronously(List, WriteListener)
	 * @deprecated This method does not allow for query cancellation or notification of completion. Favor
	 *             {@link #insertAsynchronously(List, WriteListener)}.
	 */
	@Deprecated
	<T> List<T> insertAsynchronously(List<T> entities);

	/**
	 * Inserts the given entities asynchronously in a batch.
	 * 
	 * @param entity The entities to insert
	 * @return The entities given
	 * @see #insertAsynchronously(List, WriteListener, WriteOptions)
	 * @deprecated This method does not allow for query cancellation or notification of completion. Favor
	 *             {@link #insertAsynchronously(List, WriteListener, WriteOptions)}.
	 */
	@Deprecated
	<T> List<T> insertAsynchronously(List<T> entities, WriteOptions options);

	/**
	 * Inserts the given entities asynchronously in a batch.
	 * 
	 * @param entity The entities to insert
	 * @param listener The listener to receive notification of completion
	 * @return A {@link Cancellable} enabling the cancellation of the operation
	 */
	<T> Cancellable insertAsynchronously(List<T> entities, WriteListener<T> listener);

	/**
	 * Inserts the given entities asynchronously in a batch.
	 * 
	 * @param entity The entities to insert
	 * @param listener The listener to receive notification of completion
	 * @param options The {@link WriteOptions} to use
	 * @return A {@link Cancellable} enabling the cancellation of the operation
	 */
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
	 */
	<T> List<T> update(List<T> entities);

	/**
	 * Update the given list of entities.
	 * 
	 * @param entities The entities to update.
	 * @param options The {@link WriteOptions} to use.
	 * @return The entities given.
	 */
	<T> List<T> update(List<T> entities, WriteOptions options);

	/**
	 * Updates the given entity asynchronously.
	 * 
	 * @param entity The entity to update
	 * @return The entity given
	 * @see #updateAsynchronously(Object, WriteListener)
	 * @deprecated This method does not allow for query cancellation or notification of completion. Favor
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
	 * @deprecated This method does not allow for query cancellation or notification of completion. Favor
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
	 * @param entity The entities to update
	 * @return The entities given
	 * @see #updateAsynchronously(List, WriteListener)
	 * @deprecated This method does not allow for query cancellation or notification of completion. Favor
	 *             {@link #updateAsynchronously(List, WriteListener)}.
	 */
	@Deprecated
	<T> List<T> updateAsynchronously(List<T> entities);

	/**
	 * Updates the given entities asynchronously in a batch.
	 * 
	 * @param entity The entities to update
	 * @return The entities given
	 * @see #updateAsynchronously(List, WriteListener, WriteOptions)
	 * @deprecated This method does not allow for query cancellation or notification of completion. Favor
	 *             {@link #updateAsynchronously(List, WriteListener, WriteOptions)}.
	 */
	@Deprecated
	<T> List<T> updateAsynchronously(List<T> entities, WriteOptions options);

	/**
	 * Updates the given entities asynchronously in a batch.
	 * 
	 * @param entity The entities to update
	 * @param listener The listener to receive notification of completion
	 * @return A {@link Cancellable} enabling the cancellation of the operation
	 */
	<T> Cancellable updateAsynchronously(List<T> entities, WriteListener<T> listener);

	/**
	 * Updates the given entities asynchronously in a batch.
	 * 
	 * @param entity The entities to update
	 * @param listener The listener to receive notification of completion
	 * @param options The {@link WriteOptions} to use
	 * @return A {@link Cancellable} enabling the cancellation of the operation
	 */
	<T> Cancellable updateAsynchronously(List<T> entities, WriteListener<T> listener, WriteOptions options);

	/**
	 * Remove the given object from the table by id.
	 * 
	 * @param object
	 */
	<T> void delete(T entity);

	/**
	 * @param entity
	 * @param tableName
	 * @param options
	 */
	<T> void delete(T entity, QueryOptions options);

	/**
	 * Remove the given object from the table by id.
	 * 
	 * @param object
	 */
	<T> void delete(List<T> entities);

	<T> void delete(List<T> entities, QueryOptions options);

	/**
	 * Deletes all entities of a given class.
	 */
	<T> void deleteAll(Class<T> clazz);

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
	 */
	<T> Cancellable deleteAsynchronously(List<T> entities);

	/**
	 * Remove the given objects from the table by id.
	 * 
	 * @param entities The objects to delete
	 * @param listener The {@link DeletionListener} to receive notification upon completion
	 */
	<T> Cancellable deleteAsynchronously(List<T> entities, DeletionListener<T> listener);

	/**
	 * Remove the given objects from the table by id.
	 * 
	 * @param entities The objects to delete
	 * @param options The {@link QueryOptions} to use
	 */
	<T> Cancellable deleteAsynchronously(List<T> entities, QueryOptions options);

	/**
	 * Remove the given objects from the table by id.
	 * 
	 * @param entities The objects to delete
	 * @param listener The {@link DeletionListener} to receive notification upon completion
	 * @param options The {@link QueryOptions} to use
	 */
	<T> Cancellable deleteAsynchronously(List<T> entities, DeletionListener<T> listener, QueryOptions options);

	/**
	 * Returns the underlying {@link CassandraConverter}.
	 * 
	 * @return
	 */
	CassandraConverter getConverter();

	void deleteById(Class<?> type, Object id);

	<T> List<T> selectBySimpleIds(Class<T> type, Iterable<?> ids);

	/**
	 * @deprecated Calling this method could result in {@link OutOfMemoryError}, as this is a brute force selection.
	 * @param type The type of entity to select.
	 * @return A list of all entities of type <code>T</code>.
	 */
	@Deprecated
	<T> List<T> selectAll(Class<T> type);
}
