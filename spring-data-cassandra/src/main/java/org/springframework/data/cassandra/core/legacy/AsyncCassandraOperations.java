/*
 * Copyright 2016-2025 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
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
package org.springframework.data.cassandra.core.legacy;

import java.util.List;
import java.util.function.Consumer;

import org.springframework.dao.DataAccessException;
import org.springframework.data.cassandra.core.DeleteOptions;
import org.springframework.data.cassandra.core.EntityWriteResult;
import org.springframework.data.cassandra.core.InsertOptions;
import org.springframework.data.cassandra.core.UpdateOptions;
import org.springframework.data.cassandra.core.WriteResult;
import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.cql.QueryOptions;
import org.springframework.data.cassandra.core.cql.WriteOptions;
import org.springframework.data.cassandra.core.cql.legacy.AsyncCqlOperations;
import org.springframework.data.cassandra.core.query.CassandraPageRequest;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.cassandra.core.query.Update;
import org.springframework.data.domain.Slice;
import org.springframework.util.concurrent.ListenableFuture;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.Statement;

/**
 * Interface specifying a basic set of asynchronous Cassandra operations. Implemented by {@link AsyncCassandraTemplate}.
 * Not often used directly, but a useful option to enhance testability, as it can easily be mocked or stubbed.
 *
 * @author Mark Paluch
 * @author John Blum
 * @since 4.0
 * @see AsyncCassandraTemplate
 * @see AsyncCqlOperations
 * @see Statement
 * @see InsertOptions
 * @see UpdateOptions
 * @deprecated since 4.0, use the {@link java.util.concurrent.CompletableFuture}-based variant
 *             {@link org.springframework.data.cassandra.core.AsyncCassandraTemplate}.
 */
@Deprecated(since = "4.0", forRemoval = true)
@SuppressWarnings("removal")
public interface AsyncCassandraOperations {

	/**
	 * Expose the underlying {@link AsyncCqlOperations} to allow asynchronous CQL operations.
	 *
	 * @return the underlying {@link AsyncCqlOperations}.
	 * @see AsyncCqlOperations
	 */
	AsyncCqlOperations getAsyncCqlOperations();

	/**
	 * Returns the underlying {@link CassandraConverter}.
	 *
	 * @return the underlying {@link CassandraConverter}.
	 */
	CassandraConverter getConverter();

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
	// Methods dealing with com.datastax.oss.driver.api.core.cql.Statement
	// -------------------------------------------------------------------------

	/**
	 * Execute the given Cassandra {@link Statement}. Any errors that result from executing this command will be converted
	 * into Spring's DAO exception hierarchy.
	 *
	 * @param statement a Cassandra {@link Statement}, must not be {@literal null}.
	 * @return the {@link AsyncResultSet}.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @since 3.2
	 */
	ListenableFuture<AsyncResultSet> execute(Statement<?> statement) throws DataAccessException;

	/**
	 * Execute a {@code SELECT} query and convert the resulting items to a {@link List} of entities.
	 *
	 * @param statement must not be {@literal null}.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @return the converted results
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	<T> ListenableFuture<List<T>> select(Statement<?> statement, Class<T> entityClass) throws DataAccessException;

	/**
	 * Execute a {@code SELECT} query with paging and convert the result set to a {@link Slice} of entities. A sliced
	 * query translates the effective {@link Statement#getFetchSize() fetch size} to the page size.
	 *
	 * @param statement the CQL statement, must not be {@literal null}.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @return the converted results
	 * @throws DataAccessException if there is any problem executing the query.
	 * @see CassandraPageRequest
	 */
	<T> ListenableFuture<Slice<T>> slice(Statement<?> statement, Class<T> entityClass) throws DataAccessException;

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
	<T> ListenableFuture<Void> select(Statement<?> statement, Consumer<T> entityConsumer, Class<T> entityClass)
			throws DataAccessException;

	/**
	 * Execute a {@code SELECT} query and convert the resulting item to an entity.
	 *
	 * @param statement must not be {@literal null}.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @return the converted object or {@literal null}.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	<T> ListenableFuture<T> selectOne(Statement<?> statement, Class<T> entityClass) throws DataAccessException;

	// -------------------------------------------------------------------------
	// Methods dealing with org.springframework.data.cassandra.core.query.Query
	// -------------------------------------------------------------------------

	/**
	 * Execute a {@code SELECT} query and convert the resulting items to a {@link List} of entities.
	 *
	 * @param query must not be {@literal null}.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @return the converted results
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	<T> ListenableFuture<List<T>> select(Query query, Class<T> entityClass) throws DataAccessException;

	/**
	 * Execute a {@code SELECT} query with paging and convert the result set to a {@link Slice} of entities.
	 *
	 * @param query the query object used to create a CQL statement, must not be {@literal null}.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @return the converted results
	 * @throws DataAccessException if there is any problem executing the query.
	 * @see CassandraPageRequest
	 */
	<T> ListenableFuture<Slice<T>> slice(Query query, Class<T> entityClass) throws DataAccessException;

	/**
	 * Execute a {@code SELECT} query and convert the resulting items notifying {@link Consumer} for each entity.
	 *
	 * @param query must not be {@literal null}.
	 * @param entityConsumer object that will be notified on each entity, one object at a time, must not be
	 *          {@literal null}.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @return the completion handle
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	<T> ListenableFuture<Void> select(Query query, Consumer<T> entityConsumer, Class<T> entityClass)
			throws DataAccessException;

	/**
	 * Execute a {@code SELECT} query and convert the resulting item to an entity.
	 *
	 * @param query must not be {@literal null}.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @return the converted object or {@literal null}.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	<T> ListenableFuture<T> selectOne(Query query, Class<T> entityClass) throws DataAccessException;

	/**
	 * Update the queried entities and return {@literal true} if the update was applied.
	 *
	 * @param query must not be {@literal null}.
	 * @param update must not be {@literal null}.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	ListenableFuture<Boolean> update(Query query, Update update, Class<?> entityClass) throws DataAccessException;

	/**
	 * Remove entities (rows)/columns from the table by {@link Query}.
	 *
	 * @param query must not be {@literal null}.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @return {@literal true} if the deletion was applied.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	ListenableFuture<Boolean> delete(Query query, Class<?> entityClass) throws DataAccessException;

	// -------------------------------------------------------------------------
	// Methods dealing with entities
	// -------------------------------------------------------------------------

	/**
	 * Returns the number of rows for the given entity class.
	 *
	 * @param entityClass {@link Class type} of the entity; must not be {@literal null}.
	 * @return the number of existing entities.
	 * @throws DataAccessException if any problem occurs while executing the query.
	 */
	ListenableFuture<Long> count(Class<?> entityClass) throws DataAccessException;

	/**
	 * Returns the number of rows for the given entity class applying {@link Query}. This overridden method allows users
	 * to further refine the selection criteria using a {@link Query} predicate to determine how many entities of the
	 * given {@link Class type} match the criteria.
	 *
	 * @param query user-provided count {@link Query} to execute; must not be {@literal null}.
	 * @param entityClass {@link Class type} of the entity; must not be {@literal null}.
	 * @return the number of existing entities.
	 * @throws DataAccessException if any problem occurs while executing the query.
	 * @since 2.1
	 */
	ListenableFuture<Long> count(Query query, Class<?> entityClass) throws DataAccessException;

	/**
	 * Determine whether a row of {@code entityClass} with the given {@code id} exists.
	 *
	 * @param id Id value. For single primary keys it's the plain value. For composite primary keys either, it's an
	 *          instance of either {@link org.springframework.data.cassandra.core.mapping.PrimaryKeyClass} or
	 *          {@link org.springframework.data.cassandra.core.mapping.MapId}. Must not be {@literal null}.
	 * @param entityClass {@link Class type} of the entity; must not be {@literal null}.
	 * @return {@literal true} if the object exists.
	 * @throws DataAccessException if any problem occurs while executing the query.
	 */
	ListenableFuture<Boolean> exists(Object id, Class<?> entityClass) throws DataAccessException;

	/**
	 * Determine whether the result for {@code entityClass} {@link Query} yields at least one row.
	 *
	 * @param query user-provided exists {@link Query} to execute; must not be {@literal null}.
	 * @param entityClass {@link Class type} of the entity; must not be {@literal null}.
	 * @return {@literal true} if the object exists.
	 * @throws DataAccessException if any problem occurs while executing the query.
	 * @since 2.1
	 */
	ListenableFuture<Boolean> exists(Query query, Class<?> entityClass) throws DataAccessException;

	/**
	 * Execute the Select by {@code id} for the given {@code entityClass}.
	 *
	 * @param id the Id value. For single primary keys it's the plain value. For composite primary keys either the
	 *          {@link org.springframework.data.cassandra.core.mapping.PrimaryKeyClass} or
	 *          {@link org.springframework.data.cassandra.core.mapping.MapId}. Must not be {@literal null}.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @return the converted object or {@literal null}.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	<T> ListenableFuture<T> selectOneById(Object id, Class<T> entityClass) throws DataAccessException;

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
	 * @param options must not be {@literal null}.
	 * @return the {@link EntityWriteResult} for this operation.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @see InsertOptions#empty()
	 */
	<T> ListenableFuture<EntityWriteResult<T>> insert(T entity, InsertOptions options) throws DataAccessException;

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
	 * @param options must not be {@literal null}.
	 * @return the {@link EntityWriteResult} for this operation.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @see UpdateOptions#empty()
	 */
	<T> ListenableFuture<EntityWriteResult<T>> update(T entity, UpdateOptions options) throws DataAccessException;

	/**
	 * Delete the given entity and return the entity if the delete statement was applied.
	 *
	 * @param entity must not be {@literal null}.
	 * @return the deleted entity.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	<T> ListenableFuture<T> delete(T entity) throws DataAccessException;

	/**
	 * Delete the given entity applying {@link QueryOptions} and return the entity if the delete statement was applied.
	 *
	 * @param entity must not be {@literal null}.
	 * @param options must not be {@literal null}.
	 * @return the {@link WriteResult} for this operation.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @see QueryOptions#empty()
	 */
	ListenableFuture<WriteResult> delete(Object entity, QueryOptions options) throws DataAccessException;

	/**
	 * Delete the given entity applying {@link DeleteOptions} and return the entity if the delete statement was applied.
	 *
	 * @param entity must not be {@literal null}.
	 * @param options must not be {@literal null}.
	 * @return the {@link WriteResult} for this operation.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @see DeleteOptions#empty()
	 * @since 2.2
	 */
	default ListenableFuture<WriteResult> delete(Object entity, DeleteOptions options) throws DataAccessException {
		return delete(entity, (QueryOptions) options);
	}

	/**
	 * Remove the given object from the table by id.
	 *
	 * @param id the Id value. For single primary keys it's the plain value. For composite primary keys either the
	 *          {@link org.springframework.data.cassandra.core.mapping.PrimaryKeyClass} or
	 *          {@link org.springframework.data.cassandra.core.mapping.MapId}. Must not be {@literal null}.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @return {@literal true} if the deletion was applied.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	ListenableFuture<Boolean> deleteById(Object id, Class<?> entityClass) throws DataAccessException;

	/**
	 * Execute a {@code TRUNCATE} query to remove all entities of a given class.
	 *
	 * @param entityClass The entity type must not be {@literal null}.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	ListenableFuture<Void> truncate(Class<?> entityClass) throws DataAccessException;

}
