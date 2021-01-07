/*
 * Copyright 2016-2021 the original author or authors.
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
package org.springframework.data.cassandra.core;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import org.springframework.dao.DataAccessException;
import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.cql.CqlOperations;
import org.springframework.data.cassandra.core.cql.QueryOptions;
import org.springframework.data.cassandra.core.cql.WriteOptions;
import org.springframework.data.cassandra.core.query.CassandraPageRequest;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.cassandra.core.query.Update;
import org.springframework.data.domain.Slice;
import org.springframework.lang.Nullable;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Statement;

/**
 * Interface specifying a basic set of Cassandra operations. Implemented by {@link CassandraTemplate}. Not often used
 * directly, but a useful option to enhance testability, as it can easily be mocked or stubbed.
 *
 * @author Alex Shvid
 * @author David Webb
 * @author Matthew Adams
 * @author Mark Paluch
 * @see CassandraTemplate
 * @see CqlOperations
 * @see Statement
 * @see InsertOptions
 * @see UpdateOptions
 */
public interface CassandraOperations extends FluentCassandraOperations {

	/**
	 * Returns a new {@link CassandraBatchOperations}. Each {@link CassandraBatchOperations} instance can be executed only
	 * once so you might want to obtain new {@link CassandraBatchOperations} instances for each batch.
	 *
	 * @return a new {@link CassandraBatchOperations} associated with the given entity class.
	 */
	CassandraBatchOperations batchOps();

	/**
	 * Expose the underlying {@link CqlOperations} to allow CQL operations.
	 *
	 * @return the underlying {@link CqlOperations}.
	 * @see CqlOperations
	 */
	CqlOperations getCqlOperations();

	/**
	 * Returns the underlying {@link CassandraConverter}.
	 *
	 * @return the underlying {@link CassandraConverter}.
	 */
	CassandraConverter getConverter();

	/**
	 * The table name used for the specified class by this template.
	 *
	 * @param entityClass The entity type must not be {@literal null}.
	 * @return the {@link CqlIdentifier}
	 */
	CqlIdentifier getTableName(Class<?> entityClass);

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
	<T> List<T> select(String cql, Class<T> entityClass) throws DataAccessException;

	/**
	 * Execute a {@code SELECT} query and convert the resulting items to a {@link Iterator} of entities.
	 * <p>
	 * Returns a {@link Iterator} that wraps the Cassandra {@link com.datastax.driver.core.ResultSet}.
	 *
	 * @param <T> element return type.
	 * @param cql query to execute. Must not be empty or {@literal null}.
	 * @param entityClass Class type of the elements in the {@link Iterator} stream. Must not be {@literal null}.
	 * @return an {@link Iterator} (stream) over the elements in the query result set.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @since 1.5
	 */
	<T> Stream<T> stream(String cql, Class<T> entityClass) throws DataAccessException;

	/**
	 * Execute a {@code SELECT} query and convert the resulting item to an entity.
	 *
	 * @param cql must not be {@literal null}.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @return the converted object or {@literal null}.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	@Nullable
	<T> T selectOne(String cql, Class<T> entityClass) throws DataAccessException;

	// -------------------------------------------------------------------------
	// Methods dealing with com.datastax.oss.driver.api.core.cql.Statement
	// -------------------------------------------------------------------------

	/**
	 * Execute the a Cassandra {@link Statement}. Any errors that result from executing this command will be converted
	 * into Spring's DAO exception hierarchy.
	 *
	 * @param statement a Cassandra {@link Statement}, must not be {@literal null}.
	 * @return the {@link ResultSet}.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @since 3.2
	 */
	ResultSet execute(Statement<?> statement) throws DataAccessException;

	/**
	 * Execute a {@code SELECT} query and convert the resulting items to a {@link List} of entities.
	 *
	 * @param statement must not be {@literal null}.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @return the converted results
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	<T> List<T> select(Statement<?> statement, Class<T> entityClass) throws DataAccessException;

	/**
	 * Execute a {@code SELECT} query with paging and convert the result set to a {@link Slice} of entities. A sliced
	 * query translates the effective {@link Statement#getFetchSize() fetch size} to the page size.
	 *
	 * @param statement the CQL statement, must not be {@literal null}.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @return the converted results
	 * @throws DataAccessException if there is any problem executing the query.
	 * @since 2.0
	 */
	<T> Slice<T> slice(Statement<?> statement, Class<T> entityClass) throws DataAccessException;

	/**
	 * Execute a {@code SELECT} query and convert the resulting items to a {@link Iterator} of entities.
	 * <p>
	 * Returns a {@link Iterator} that wraps the Cassandra {@link com.datastax.driver.core.ResultSet}.
	 *
	 * @param <T> element return type.
	 * @param statement query to execute. Must not be empty or {@literal null}.
	 * @param entityClass Class type of the elements in the {@link Iterator} stream. Must not be {@literal null}.
	 * @return an {@link Iterator} (stream) over the elements in the query result set.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @since 1.5
	 */
	<T> Stream<T> stream(Statement<?> statement, Class<T> entityClass) throws DataAccessException;

	/**
	 * Execute a {@code SELECT} query and convert the resulting item to an entity.
	 *
	 * @param statement must not be {@literal null}.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @return the converted object or {@literal null}.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	@Nullable
	<T> T selectOne(Statement<?> statement, Class<T> entityClass) throws DataAccessException;

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
	 * @since 2.0
	 */
	<T> List<T> select(Query query, Class<T> entityClass) throws DataAccessException;

	/**
	 * Execute a {@code SELECT} query with paging and convert the result set to a {@link Slice} of entities.
	 *
	 * @param query the query object used to create a CQL statement, must not be {@literal null}.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @return the converted results
	 * @throws DataAccessException if there is any problem executing the query.
	 * @since 2.0
	 * @see CassandraPageRequest
	 */
	<T> Slice<T> slice(Query query, Class<T> entityClass) throws DataAccessException;

	/**
	 * Execute a {@code SELECT} query and convert the resulting items to a {@link Iterator} of entities.
	 * <p>
	 * Returns a {@link Iterator} that wraps the Cassandra {@link com.datastax.driver.core.ResultSet}.
	 *
	 * @param <T> element return type.
	 * @param query query to execute. Must not be empty or {@literal null}.
	 * @param entityClass Class type of the elements in the {@link Iterator} stream. Must not be {@literal null}.
	 * @return an {@link Iterator} (stream) over the elements in the query result set.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @since 2.0
	 */
	<T> Stream<T> stream(Query query, Class<T> entityClass) throws DataAccessException;

	/**
	 * Execute a {@code SELECT} query and convert the resulting item to an entity.
	 *
	 * @param query must not be {@literal null}.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @return the converted object or {@literal null}.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @since 2.0
	 */
	@Nullable
	<T> T selectOne(Query query, Class<T> entityClass) throws DataAccessException;

	/**
	 * Update the queried entities and return {@literal true} if the update was applied.
	 *
	 * @param query must not be {@literal null}.
	 * @param update must not be {@literal null}.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	boolean update(Query query, Update update, Class<?> entityClass) throws DataAccessException;

	/**
	 * Remove entities (rows)/columns from the table by {@link Query}.
	 *
	 * @param query must not be {@literal null}.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	boolean delete(Query query, Class<?> entityClass) throws DataAccessException;

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
	long count(Class<?> entityClass) throws DataAccessException;

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
	long count(Query query, Class<?> entityClass) throws DataAccessException;

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
	boolean exists(Object id, Class<?> entityClass) throws DataAccessException;

	/**
	 * Determine whether the result for {@code entityClass} {@link Query} yields at least one row.
	 *
	 * @param query user-defined exists {@link Query} to execute; must not be {@literal null}.
	 * @param entityClass {@link Class type} of the entity; must not be {@literal null}.
	 * @return {@literal true} if the object exists.
	 * @throws DataAccessException if any problem occurs while executing the query.
	 * @since 2.1
	 */
	boolean exists(Query query, Class<?> entityClass) throws DataAccessException;

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
	@Nullable
	<T> T selectOneById(Object id, Class<T> entityClass) throws DataAccessException;

	/**
	 * Insert the given entity and return the entity if the insert was applied.
	 *
	 * @param entity The entity to insert, must not be {@literal null}.
	 * @return the inserted entity.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	<T> T insert(T entity) throws DataAccessException;

	/**
	 * Insert the given entity applying {@link WriteOptions} and return the entity if the insert was applied.
	 *
	 * @param entity The entity to insert, must not be {@literal null}.
	 * @param options must not be {@literal null}.
	 * @return the {@link EntityWriteResult} for this operation.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @see InsertOptions#empty()
	 */
	<T> EntityWriteResult<T> insert(T entity, InsertOptions options) throws DataAccessException;

	/**
	 * Update the given entity and return the entity if the update was applied.
	 *
	 * @param entity The entity to update, must not be {@literal null}.
	 * @return the updated entity.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	<T> T update(T entity) throws DataAccessException;

	/**
	 * Update the given entity applying {@link WriteOptions} and return the entity if the update was applied.
	 *
	 * @param entity The entity to update, must not be {@literal null}.
	 * @param options must not be {@literal null}.
	 * @return the {@link EntityWriteResult} for this operation.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @see UpdateOptions#empty()
	 */
	<T> EntityWriteResult<T> update(T entity, UpdateOptions options) throws DataAccessException;

	/**
	 * Delete the given entity and return the entity if the delete was applied.
	 *
	 * @param entity must not be {@literal null}.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	void delete(Object entity) throws DataAccessException;

	/**
	 * Delete the given entity applying {@link QueryOptions} and return the entity if the delete was applied.
	 *
	 * @param entity must not be {@literal null}.
	 * @param options must not be {@literal null}.
	 * @return the {@link WriteResult} for this operation.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @see QueryOptions#empty()
	 */
	WriteResult delete(Object entity, QueryOptions options) throws DataAccessException;

	/**
	 * Delete the given entity applying {@link DeleteOptions} and return the entity if the delete was applied.
	 *
	 * @param entity must not be {@literal null}.
	 * @param options must not be {@literal null}.
	 * @return the {@link WriteResult} for this operation.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @see DeleteOptions#empty()
	 * @since 2.2
	 */
	default WriteResult delete(Object entity, DeleteOptions options) throws DataAccessException {
		return delete(entity, (QueryOptions) options);
	}

	/**
	 * Remove the given object from the table by id.
	 *
	 * @param id the Id value. For single primary keys it's the plain value. For composite primary keys either the
	 *          {@link org.springframework.data.cassandra.core.mapping.PrimaryKeyClass} or
	 *          {@link org.springframework.data.cassandra.core.mapping.MapId}. Must not be {@literal null}.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	boolean deleteById(Object id, Class<?> entityClass) throws DataAccessException;

	/**
	 * Execute a {@code TRUNCATE} query to remove all entities of a given class.
	 *
	 * @param entityClass The entity type must not be {@literal null}.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	void truncate(Class<?> entityClass) throws DataAccessException;

}
