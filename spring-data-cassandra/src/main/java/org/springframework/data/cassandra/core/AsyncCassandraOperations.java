/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
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

import java.util.List;
import java.util.function.Consumer;

import org.springframework.cassandra.core.AsyncCqlOperations;
import org.springframework.cassandra.core.QueryOptions;
import org.springframework.cassandra.core.WriteOptions;
import org.springframework.dao.DataAccessException;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.util.concurrent.ListenableFuture;

import com.datastax.driver.core.Statement;

/**
 * Interface specifying a basic set of asynchronous Cassandra operations. Implemented by {@link AsyncCassandraTemplate}.
 * Not often used directly, but a useful option to enhance testability, as it can easily be mocked or stubbed.
 *
 * @author Mark Paluch
 * @since 2.0
 * @see AsyncCassandraTemplate
 */
public interface AsyncCassandraOperations {

	// -------------------------------------------------------------------------
	// Methods dealing with static CQL
	// -------------------------------------------------------------------------

	/**
	 * Execute a {@code SELECT} query and convert the resulting items to a {@link List} of entities.
	 *
	 * @param cql must not be {@literal null}.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @return the converted results
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	<T> ListenableFuture<List<T>> select(String cql, Class<T> entityClass) throws DataAccessException;

	/**
	 * Execute a {@code SELECT} query and convert the resulting items notifying {@link Consumer} for each entity.
	 *
	 * @param cql must not be {@literal null}.
	 * @param entityConsumer object that will be notified on each entity, one object at a time, must not be
	 *          {@literal null}.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @return the completion handle
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	<T> ListenableFuture<Void> select(String cql, Consumer<T> entityConsumer, Class<T> entityClass)
			throws DataAccessException;

	/**
	 * Execute a {@code SELECT} query and convert the resulting item to an entity.
	 *
	 * @param cql must not be {@literal null}.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @return the converted object or {@literal null}.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	<T> ListenableFuture<T> selectOne(String cql, Class<T> entityClass) throws DataAccessException;

	// -------------------------------------------------------------------------
	// Methods dealing with com.datastax.driver.core.Statement
	// -------------------------------------------------------------------------

	/**
	 * Execute a {@code SELECT} query and convert the resulting items to a {@link List} of entities.
	 *
	 * @param statement must not be {@literal null}.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @return the converted results
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	<T> ListenableFuture<List<T>> select(Statement statement, Class<T> entityClass) throws DataAccessException;

	/**
	 * Execute a {@code SELECT} query and convert the resulting items notifying {@link Consumer} for each entity.
	 *
	 * @param statement must not be {@literal null}.
	 * @param entityConsumer object that will be notified on each entity, one object at a time, must not be
	 *          {@literal null}.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @return the completion handle
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	<T> ListenableFuture<Void> select(Statement statement, Consumer<T> entityConsumer, Class<T> entityClass)
			throws DataAccessException;

	/**
	 * Execute a {@code SELECT} query and convert the resulting item to an entity.
	 *
	 * @param statement must not be {@literal null}.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @return the converted object or {@literal null}.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	<T> ListenableFuture<T> selectOne(Statement statement, Class<T> entityClass) throws DataAccessException;

	// -------------------------------------------------------------------------
	// Methods dealing with entities
	// -------------------------------------------------------------------------

	/**
	 * Execute the Select by {@code id} for the given {@code entityClass}.
	 *
	 * @param id must not be {@literal null}.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @return the converted object or {@literal null}.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	<T> ListenableFuture<T> selectOneById(Object id, Class<T> entityClass) throws DataAccessException;

	/**
	 * Determine whether the row {@code entityClass} with the given {@code id} exists.
	 *
	 * @param id must not be {@literal null}.
	 * @param entityClass must not be {@literal null}.
	 * @return {@literal true}, if the object exists.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	ListenableFuture<Boolean> exists(Object id, Class<?> entityClass) throws DataAccessException;

	/**
	 * Returns the number of rows for the given entity class.
	 *
	 * @param entityClass must not be {@literal null}.
	 * @return the number of existing entities.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	ListenableFuture<Long> count(Class<?> entityClass) throws DataAccessException;

	/**
	 * Insert the given entity and return the entity if the insert was applied.
	 *
	 * @param entity The entity to insert, must not be {@literal null}.
	 * @return the inserted entity.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	<T> ListenableFuture<T> insert(T entity) throws DataAccessException;

	/**
	 * Insert the given entity applying {@link WriteOptions} and return the entity if the insert was applied.
	 *
	 * @param entity The entity to insert, must not be {@literal null}.
	 * @param options may be {@literal null}.
	 * @return the inserted entity.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	<T> ListenableFuture<T> insert(T entity, WriteOptions options) throws DataAccessException;

	/**
	 * Update the given entity and return the entity if the update was applied.
	 *
	 * @param entity The entity to update, must not be {@literal null}.
	 * @return the updated entity.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	<T> ListenableFuture<T> update(T entity) throws DataAccessException;

	/**
	 * Update the given entity applying {@link WriteOptions} and return the entity if the update was applied.
	 *
	 * @param entity The entity to update, must not be {@literal null}.
	 * @param options may be {@literal null}.
	 * @return the updated entity.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	<T> ListenableFuture<T> update(T entity, WriteOptions options) throws DataAccessException;

	/**
	 * Remove the given object from the table by id.
	 *
	 * @param id must not be {@literal null}.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @return {@literal true} if the deletion was applied.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	ListenableFuture<Boolean> deleteById(Object id, Class<?> entityClass) throws DataAccessException;

	/**
	 * Delete the given entity and return the entity if the delete was applied.
	 *
	 * @param entity must not be {@literal null}.
	 * @return the deleted entity.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	<T> ListenableFuture<T> delete(T entity) throws DataAccessException;

	/**
	 * Delete the given entity applying {@link QueryOptions} and return the entity if the delete was applied.
	 *
	 * @param entity must not be {@literal null}.
	 * @param options may be {@literal null}.
	 * @return the deleted entity.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	<T> ListenableFuture<T> delete(T entity, QueryOptions options) throws DataAccessException;

	/**
	 * Execute a {@code TRUNCATE} query to remove all entities of a given class.
	 * 
	 * @param entityClass The entity type must not be {@literal null}.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	ListenableFuture<Void> truncate(Class<?> entityClass) throws DataAccessException;

	/**
	 * Returns the underlying {@link CassandraConverter}.
	 *
	 * @return the underlying {@link CassandraConverter}.
	 */
	CassandraConverter getConverter();

	/**
	 * Expose the underlying {@link AsyncCqlOperationsOperations} to allow asynchronous CQL operations.
	 *
	 * @return the underlying {@link AsyncCqlOperations}.
	 * @see AsyncCqlOperations
	 */
	AsyncCqlOperations getAsyncCqlOperations();
}
