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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.dao.DataAccessException;
import org.springframework.data.cassandra.ReactiveResultSet;
import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.cql.QueryOptions;
import org.springframework.data.cassandra.core.cql.ReactiveCqlOperations;
import org.springframework.data.cassandra.core.cql.WriteOptions;
import org.springframework.data.cassandra.core.query.CassandraPageRequest;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.cassandra.core.query.Update;
import org.springframework.data.domain.Slice;

import com.datastax.oss.driver.api.core.cql.Statement;

/**
 * Interface specifying a basic set of reactive Cassandra operations. Implemented by {@link ReactiveCassandraTemplate}.
 * Not often used directly, but a useful option to enhance testability, as it can easily be mocked or stubbed.
 *
 * @author Mark Paluch
 * @author Hleb Albau
 * @author Oleh Dokuka
 * @since 2.0
 * @see ReactiveCassandraTemplate
 * @see ReactiveCqlOperations
 * @see Statement
 * @see InsertOptions
 * @see UpdateOptions
 * @see Flux
 * @see Mono
 */
public interface ReactiveCassandraOperations extends ReactiveFluentCassandraOperations {

	/**
	 * Returns a new {@link ReactiveCassandraBatchOperations}. Each {@link ReactiveCassandraBatchOperations} instance can
	 * be executed only once so you might want to obtain new {@link ReactiveCassandraBatchOperations} instances for each
	 * batch.
	 *
	 * @return a new {@link ReactiveCassandraBatchOperations} associated with the given entity class.
	 * @since 2.1
	 */
	ReactiveCassandraBatchOperations batchOps();

	/**
	 * Expose the underlying {@link ReactiveCqlOperations} to allow CQL operations.
	 *
	 * @return the underlying {@link ReactiveCqlOperations}.
	 * @see ReactiveCqlOperations
	 */
	ReactiveCqlOperations getReactiveCqlOperations();

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
	 * Execute a {@code SELECT} query and convert the resulting items to a stream of entities.
	 *
	 * @param cql must not be {@literal null}.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @return the converted results
	 * @throws DataAccessException if there is any problem issuing the execution.
	 */
	<T> Flux<T> select(String cql, Class<T> entityClass) throws DataAccessException;

	/**
	 * Execute a {@code SELECT} query and convert the resulting item to an entity.
	 *
	 * @param cql must not be {@literal null}.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @return the result object returned by the action or {@link Mono#empty()}
	 * @throws DataAccessException if there is any problem issuing the execution.
	 */
	<T> Mono<T> selectOne(String cql, Class<T> entityClass) throws DataAccessException;

	// -------------------------------------------------------------------------
	// Methods dealing with com.datastax.oss.driver.api.core.cql.Statement
	// -------------------------------------------------------------------------

	/**
	 * Execute a {@code SELECT} query and convert the resulting items to a stream of entities.
	 *
	 * @param statement must not be {@literal null}.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @return the result objects returned by the action.
	 * @throws DataAccessException if there is any problem issuing the execution.
	 */
	<T> Flux<T> select(Statement<?> statement, Class<T> entityClass) throws DataAccessException;

	/**
	 * Execute a {@code SELECT} query with paging and convert the result set to a {@link Slice} of entities. A sliced
	 * query translates the effective {@link Statement#getFetchSize() fetch size} to the page size.
	 *
	 * @param statement the CQL statement, must not be {@literal null}.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @return the result object returned by the action or {@link Mono#just(Object)} of an empty {@link Slice}.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @since 2.1
	 */
	<T> Mono<Slice<T>> slice(Statement<?> statement, Class<T> entityClass) throws DataAccessException;

	/**
	 * Execute a {@code SELECT} query and convert the resulting item to an entity.
	 *
	 * @param statement must not be {@literal null}.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @return the result object returned by the action or {@link Mono#empty()}
	 * @throws DataAccessException if there is any problem issuing the execution.
	 */
	<T> Mono<T> selectOne(Statement<?> statement, Class<T> entityClass) throws DataAccessException;

	// -------------------------------------------------------------------------
	// Methods dealing with org.springframework.data.cassandra.core.query.Query
	// -------------------------------------------------------------------------

	/**
	 * Execute the a Cassandra {@link Statement}. Any errors that result from executing this command will be converted
	 * into Spring's DAO exception hierarchy.
	 *
	 * @param statement a Cassandra {@link Statement}, must not be {@literal null}.
	 * @return the {@link ReactiveResultSet}.
	 * @throws DataAccessException if there is any problem issuing the execution.
	 * @since 3.2
	 */
	Mono<ReactiveResultSet> execute(Statement<?> statement) throws DataAccessException;

	/**
	 * Execute a {@code SELECT} query and convert the resulting items to a stream of entities.
	 *
	 * @param query must not be {@literal null}.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @return the result objects returned by the action.
	 * @throws DataAccessException if there is any problem issuing the execution.
	 */
	<T> Flux<T> select(Query query, Class<T> entityClass) throws DataAccessException;

	/**
	 * Execute a {@code SELECT} query with paging and convert the result set to a {@link Slice} of entities.
	 *
	 * @param query the query object used to create a CQL statement, must not be {@literal null}.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @return the result object returned by the action or {@link Mono#just(Object)} of an empty {@link Slice}.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @since 2.1
	 * @see CassandraPageRequest
	 */
	<T> Mono<Slice<T>> slice(Query query, Class<T> entityClass) throws DataAccessException;

	/**
	 * Execute a {@code SELECT} query and convert the resulting item to an entity.
	 *
	 * @param query must not be {@literal null}.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @return the result object returned by the action or {@link Mono#empty()}.
	 * @throws DataAccessException if there is any problem issuing the execution.
	 */
	<T> Mono<T> selectOne(Query query, Class<T> entityClass) throws DataAccessException;

	/**
	 * Update the queried entities and return {@literal true} if the update was applied.
	 *
	 * @param query must not be {@literal null}.
	 * @param update must not be {@literal null}.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	Mono<Boolean> update(Query query, Update update, Class<?> entityClass) throws DataAccessException;

	/**
	 * Remove entities (rows)/columns from the table by {@link Query}.
	 *
	 * @param query must not be {@literal null}.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @return {@literal true} if the deletion was applied.
	 * @throws DataAccessException if there is any problem issuing the execution.
	 */
	Mono<Boolean> delete(Query query, Class<?> entityClass) throws DataAccessException;

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
	Mono<Long> count(Class<?> entityClass) throws DataAccessException;

	/**
	 * Returns the number of rows for the given entity class applying {@link Query}. This overridden method allows users
	 * to further refine the selection criteria using a {@link Query} predicate to determine how many entities of the
	 * given {@link Class type} match the criteria.
	 *
	 * @param query user-defined count {@link Query} to execute; must not be {@literal null}.
	 * @param entityClass {@link Class type} of the entity; must not be {@literal null}.
	 * @return the number of existing entities.
	 * @throws DataAccessException if any problem occurs while executing the query.
	 * @since 2.1
	 */
	Mono<Long> count(Query query, Class<?> entityClass) throws DataAccessException;

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
	Mono<Boolean> exists(Object id, Class<?> entityClass) throws DataAccessException;

	/**
	 * Determine whether the result for {@code entityClass} {@link Query} yields at least one row.
	 *
	 * @param query user-defined exists {@link Query} to execute; must not be {@literal null}.
	 * @param entityClass {@link Class type} of the entity; must not be {@literal null}.
	 * @return {@literal true} if the object exists.
	 * @throws DataAccessException if any problem occurs while executing the query.
	 * @since 2.1
	 */
	Mono<Boolean> exists(Query query, Class<?> entityClass) throws DataAccessException;

	/**
	 * Execute the Select by {@code id} for the given {@code entityClass}.
	 *
	 * @param id the Id value. For single primary keys it's the plain value. For composite primary keys either the
	 *          {@link org.springframework.data.cassandra.core.mapping.PrimaryKeyClass} or
	 *          {@link org.springframework.data.cassandra.core.mapping.MapId}. Must not be {@literal null}.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @return the result object returned by the action or {@link Mono#empty()}
	 * @throws DataAccessException if there is any problem issuing the execution.
	 */
	<T> Mono<T> selectOneById(Object id, Class<T> entityClass) throws DataAccessException;

	/**
	 * Insert the given entity and emit the entity if the insert was applied.
	 *
	 * @param entity The entity to insert, must not be {@literal null}.
	 * @return the inserted entity.
	 * @throws DataAccessException if there is any problem issuing the execution.
	 */
	<T> Mono<T> insert(T entity) throws DataAccessException;

	/**
	 * Insert the given entity applying {@link WriteOptions} and emit the entity if the insert was applied.
	 *
	 * @param entity The entity to insert, must not be {@literal null}.
	 * @param options must not be {@literal null}.
	 * @return the {@link EntityWriteResult} for this operation.
	 * @throws DataAccessException if there is any problem issuing the execution.
	 * @see InsertOptions#empty()
	 */
	<T> Mono<EntityWriteResult<T>> insert(T entity, InsertOptions options) throws DataAccessException;

	/**
	 * Update the given entity and emit the entity if the update was applied.
	 *
	 * @param entity The entity to update, must not be {@literal null}.
	 * @return the updated entity.
	 * @throws DataAccessException if there is any problem issuing the execution.
	 */
	<T> Mono<T> update(T entity) throws DataAccessException;

	/**
	 * Update the given entity applying {@link WriteOptions} and emit the entity if the update was applied.
	 *
	 * @param entity The entity to update, must not be {@literal null}.
	 * @param options must not be {@literal null}.
	 * @return the {@link EntityWriteResult} for this operation.
	 * @throws DataAccessException if there is any problem issuing the execution.
	 * @see UpdateOptions#empty()
	 */
	<T> Mono<EntityWriteResult<T>> update(T entity, UpdateOptions options) throws DataAccessException;

	/**
	 * Delete the given entity and emit the entity if the delete was applied.
	 *
	 * @param entity must not be {@literal null}.
	 * @return the deleted entity.
	 * @throws DataAccessException if there is any problem issuing the execution.
	 */
	<T> Mono<T> delete(T entity) throws DataAccessException;

	/**
	 * Delete the given entity applying {@link QueryOptions} and emit the entity if the delete was applied.
	 *
	 * @param entity must not be {@literal null}.
	 * @param options must not be {@literal null}.
	 * @return the {@link WriteResult} for this operation.
	 * @throws DataAccessException if there is any problem issuing the execution.
	 * @see QueryOptions#empty()
	 */
	Mono<WriteResult> delete(Object entity, QueryOptions options) throws DataAccessException;

	/**
	 * Delete the given entity applying {@link QueryOptions} and emit the entity if the delete was applied.
	 *
	 * @param entity must not be {@literal null}.
	 * @param options must not be {@literal null}.
	 * @return the {@link WriteResult} for this operation.
	 * @throws DataAccessException if there is any problem issuing the execution.
	 * @see DeleteOptions#empty()
	 * @since 2.2
	 */
	default Mono<WriteResult> delete(Object entity, DeleteOptions options) throws DataAccessException {
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
	 * @throws DataAccessException if there is any problem issuing the execution.
	 */
	Mono<Boolean> deleteById(Object id, Class<?> entityClass) throws DataAccessException;

	/**
	 * Execute a {@code TRUNCATE} query to remove all entities of a given class.
	 *
	 * @param entityClass The entity type must not be {@literal null}.
	 * @throws DataAccessException if there is any problem issuing the execution.
	 */
	Mono<Void> truncate(Class<?> entityClass) throws DataAccessException;
}
