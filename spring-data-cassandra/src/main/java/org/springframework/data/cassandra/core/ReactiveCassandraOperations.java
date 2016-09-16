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

import org.reactivestreams.Publisher;
import org.springframework.cassandra.core.QueryOptions;
import org.springframework.cassandra.core.ReactiveCqlOperations;
import org.springframework.cassandra.core.WriteOptions;
import org.springframework.dao.DataAccessException;
import org.springframework.data.cassandra.convert.CassandraConverter;

import com.datastax.driver.core.Statement;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Interface specifying a basic set of reactive Cassandra operations. Implemented by {@link ReactiveCassandraTemplate}.
 * Not often used directly, but a useful option to enhance testability, as it can easily be mocked or stubbed.
 *
 * @author Mark Paluch
 * @since 2.0
 * @see ReactiveCassandraTemplate
 * @see Flux
 * @see Mono
 */
public interface ReactiveCassandraOperations {

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
	// Methods dealing with com.datastax.driver.core.Statement
	// -------------------------------------------------------------------------

	/**
	 * Execute a {@code SELECT} query and convert the resulting items to a stream of entities.
	 *
	 * @param statement must not be {@literal null}.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @return the result objects returned by the action.
	 * @throws DataAccessException if there is any problem issuing the execution.
	 */
	<T> Flux<T> select(Statement statement, Class<T> entityClass) throws DataAccessException;

	/**
	 * Execute a {@code SELECT} query and convert the resulting item to an entity.
	 *
	 * @param statement must not be {@literal null}.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @return the result object returned by the action or {@link Mono#empty()}
	 * @throws DataAccessException if there is any problem issuing the execution.
	 */
	<T> Mono<T> selectOne(Statement statement, Class<T> entityClass) throws DataAccessException;

	// -------------------------------------------------------------------------
	// Methods dealing with entities
	// -------------------------------------------------------------------------

	/**
	 * Execute the Select by {@code id} for the given {@code entityClass}.
	 *
	 * @param id must not be {@literal null}.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @return the result object returned by the action or {@link Mono#empty()}
	 * @throws DataAccessException if there is any problem issuing the execution.
	 */
	<T> Mono<T> selectOneById(Object id, Class<T> entityClass) throws DataAccessException;

	/**
	 * Determine whether the row {@code entityClass} with the given {@code id} exists.
	 *
	 * @param id must not be {@literal null}.
	 * @param entityClass must not be {@literal null}.
	 * @return {@literal true} if the object exists.
	 * @throws DataAccessException if there is any problem issuing the execution.
	 */
	Mono<Boolean> exists(Object id, Class<?> entityClass) throws DataAccessException;

	/**
	 * Returns the number of rows for the given entity class.
	 *
	 * @param entityClass must not be {@literal null}.
	 * @return the number of existing entities.
	 * @throws DataAccessException if there is any problem issuing the execution.
	 */
	Mono<Long> count(Class<?> entityClass) throws DataAccessException;

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
	 * @param options may be {@literal null}.
	 * @@return the inserted entity.
	 * @throws DataAccessException if there is any problem issuing the execution.
	 */
	<T> Mono<T> insert(T entity, WriteOptions options) throws DataAccessException;

	/**
	 * Insert the given entities and emit the entity if the insert was applied.
	 *
	 * @param entities The entities to insert, must not be {@literal null}.
	 * @return the inserted entities.
	 * @throws DataAccessException if there is any problem issuing the execution.
	 */
	<T> Flux<T> insert(Iterable<? extends T> entities) throws DataAccessException;

	/**
	 * Insert the given entities applying {@link WriteOptions} and emit the entity if the insert was applied.
	 *
	 * @param entities The entities to insert, must not be {@literal null}.
	 * @param options may be {@literal null}.
	 * @return the inserted entities.
	 * @throws DataAccessException if there is any problem issuing the execution.
	 */
	<T> Flux<T> insert(Iterable<? extends T> entities, WriteOptions options) throws DataAccessException;

	/**
	 * Insert the given entities and emit the entity if the insert was applied.
	 *
	 * @param entities The entities to insert, must not be {@literal null}.
	 * @return the inserted entities.
	 * @throws DataAccessException if there is any problem issuing the execution.
	 */
	<T> Flux<T> insert(Publisher<? extends T> entities) throws DataAccessException;

	/**
	 * Insert the given entities applying {@link WriteOptions} and emit the entity if the insert was applied.
	 *
	 * @param entities The entities to insert, must not be {@literal null}.
	 * @param options may be {@literal null}.
	 * @return the inserted entities.
	 * @throws DataAccessException if there is any problem issuing the execution.
	 */
	<T> Flux<T> insert(Publisher<? extends T> entities, WriteOptions options) throws DataAccessException;

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
	 * @param options may be {@literal null}.
	 * @return the updated entity.
	 * @throws DataAccessException if there is any problem issuing the execution.
	 */
	<T> Mono<T> update(T entity, WriteOptions options) throws DataAccessException;

	/**
	 * Update the given entities and emit the entity if the update was applied.
	 *
	 * @param entities The entities to update, must not be {@literal null}.
	 * @return the updated entities.
	 * @throws DataAccessException if there is any problem issuing the execution.
	 */
	<T> Flux<T> update(Iterable<? extends T> entities) throws DataAccessException;

	/**
	 * Update the given entities applying {@link WriteOptions} and emit the entity if the update was applied.
	 *
	 * @param entities The entities to update, must not be {@literal null}.
	 * @param options may be {@literal null}.
	 * @return the updated entities.
	 * @throws DataAccessException if there is any problem issuing the execution.
	 */
	<T> Flux<T> update(Iterable<? extends T> entities, WriteOptions options) throws DataAccessException;

	/**
	 * Update the given entities and emit the entity if the update was applied.
	 *
	 * @param entities The entities to update, must not be {@literal null}.
	 * @return the updated entities.
	 * @throws DataAccessException if there is any problem issuing the execution.
	 */
	<T> Flux<T> update(Publisher<? extends T> entities) throws DataAccessException;

	/**
	 * Update the given entities applying {@link WriteOptions} and emit the entity if the update was applied.
	 *
	 * @param entities The entities to update.
	 * @param options may be {@literal null}.
	 * @return the updated entities.
	 * @throws DataAccessException if there is any problem issuing the execution.
	 */
	<T> Flux<T> update(Publisher<? extends T> entities, WriteOptions options) throws DataAccessException;

	/**
	 * Remove the given object from the table by id.
	 *
	 * @param id must not be {@literal null}.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @return {@literal true} if the deletion was applied.
	 * @throws DataAccessException if there is any problem issuing the execution.
	 */
	Mono<Boolean> deleteById(Object id, Class<?> entityClass) throws DataAccessException;

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
	 * @param options may be {@literal null}.
	 * @return the deleted entity.
	 * @throws DataAccessException if there is any problem issuing the execution.
	 */
	<T> Mono<T> delete(T entity, QueryOptions options) throws DataAccessException;

	/**
	 * Delete the given entities and emit the entity if the delete was applied.
	 *
	 * @param entities must not be {@literal null}.
	 * @return the deleted entities.
	 * @throws DataAccessException if there is any problem issuing the execution.
	 */
	<T> Flux<T> delete(Iterable<? extends T> entities) throws DataAccessException;

	/**
	 * Delete the given entities applying {@link QueryOptions} and emit the entity if the delete was applied.
	 *
	 * @param entities must not be {@literal null}.
	 * @param options may be {@literal null}.
	 * @return the deleted entities.
	 * @throws DataAccessException if there is any problem issuing the execution.
	 */
	<T> Flux<T> delete(Iterable<? extends T> entities, QueryOptions options) throws DataAccessException;

	/**
	 * Delete the given entities and emit the entity if the delete was applied.
	 *
	 * @param entities must not be {@literal null}.
	 * @return the deleted entities.
	 * @throws DataAccessException if there is any problem issuing the execution.
	 */
	<T> Flux<T> delete(Publisher<? extends T> entities) throws DataAccessException;

	/**
	 * Delete the given entities applying {@link QueryOptions} and emit the entity if the delete was applied.
	 *
	 * @param entities must not be {@literal null}.
	 * @param options may be {@literal null}.
	 * @return the deleted entities.
	 * @throws DataAccessException if there is any problem issuing the execution.
	 */
	<T> Flux<T> delete(Publisher<? extends T> entities, QueryOptions options) throws DataAccessException;

	/**
	 * Execute a {@code TRUNCATE} query to remove all entities of a given class.
	 * 
	 * @param entityClass The entity type must not be {@literal null}.
	 * @throws DataAccessException if there is any problem issuing the execution.
	 */
	Mono<Void> truncate(Class<?> entityClass) throws DataAccessException;

	/**
	 * Returns the underlying {@link CassandraConverter}.
	 *
	 * @return the underlying {@link CassandraConverter}.
	 */
	CassandraConverter getConverter();

	/**
	 * Expose the underlying {@link ReactiveCqlOperations} to allow CQL operations.
	 * 
	 * @return the underlying {@link ReactiveCqlOperations}.
	 * @see ReactiveCqlOperations
	 */
	ReactiveCqlOperations getReactiveCqlOperations();
}
