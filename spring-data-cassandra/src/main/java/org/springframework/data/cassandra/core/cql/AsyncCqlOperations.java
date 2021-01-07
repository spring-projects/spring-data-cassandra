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
package org.springframework.data.cassandra.core.cql;

import java.util.List;
import java.util.Map;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Statement;

import org.springframework.dao.DataAccessException;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.lang.Nullable;
import org.springframework.util.concurrent.ListenableFuture;

/**
 * Interface specifying a basic set of CQL asynchronously executed operations. Exposes similar methods as
 * {@link CqlTemplate}, but returns result handles or accepts callbacks as opposed to concrete results. Implemented by
 * {@link AsyncCqlTemplate}. Not often used directly, but a useful option to enhance testability, as it can easily be
 * mocked or stubbed.
 *
 * @author Mark Paluch
 * @author John Blum
 * @since 2.0
 * @see AsyncCqlTemplate
 * @see CqlOperations
 */
public interface AsyncCqlOperations {

	// -------------------------------------------------------------------------
	// Methods dealing with a plain com.datastax.oss.driver.api.core.CqlSession
	// -------------------------------------------------------------------------

	/**
	 * Execute a CQL data access operation, implemented as callback action working on a
	 * {@link com.datastax.oss.driver.api.core.CqlSession}. This allows for implementing arbitrary data access operations,
	 * within Spring's managed CQL environment: that is, converting CQL
	 * {@link com.datastax.oss.driver.api.core.DriverException}s into Spring's {@link DataAccessException} hierarchy.
	 * <p>
	 * The callback action can return a result object, for example a domain object or a collection of domain objects.
	 *
	 * @param action the callback object that specifies the action.
	 * @return a result object returned by the action, or {@literal null}.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	<T> ListenableFuture<T> execute(AsyncSessionCallback<T> action) throws DataAccessException;

	// -------------------------------------------------------------------------
	// Methods dealing with static CQL
	// -------------------------------------------------------------------------

	/**
	 * Issue a single CQL execute, typically a DDL statement, insert, update or delete statement.
	 *
	 * @param cql static CQL to execute, must not be {@literal null} or empty.
	 * @return boolean value whether the statement was applied.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	ListenableFuture<Boolean> execute(String cql) throws DataAccessException;

	/**
	 * Issue a single CQL operation (such as an insert, update or delete statement) via a prepared statement, binding the
	 * given arguments.
	 *
	 * @param cql static CQL to execute, must not be {@literal null} or empty.
	 * @param args arguments to bind to the query (leaving it to the {@link PreparedStatement} to guess the corresponding
	 *          CQL type).
	 * @return boolean value whether the statement was applied.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	ListenableFuture<Boolean> execute(String cql, Object... args) throws DataAccessException;

	/**
	 * Issue an statement using a {@link PreparedStatementBinder} to set bind parameters, with given CQL. Simpler than
	 * using a {@link AsyncPreparedStatementCreator} as this method will create the {@link PreparedStatement}: The
	 * {@link PreparedStatementBinder} just needs to set parameters.
	 *
	 * @param cql static CQL to execute, must not be {@literal null} or empty.
	 * @param psb object that knows how to set values on the prepared statement. If this is {@literal null}, the CQL will
	 *          be assumed to contain no bind parameters. Even if there are no bind parameters, this object may be used to
	 *          set fetch size and other performance options.
	 * @return boolean value whether the statement was applied.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	ListenableFuture<Boolean> execute(String cql, @Nullable PreparedStatementBinder psb) throws DataAccessException;

	/**
	 * Execute a CQL data access operation, implemented as callback action working on a CQL {@link PreparedStatement}.
	 * This allows for implementing arbitrary data access operations on a single Statement, within Spring's managed CQL
	 * environment: that is, participating in Spring-managed transactions and converting CQL
	 * {@link com.datastax.oss.driver.api.core.DriverException}s into Spring's {@link DataAccessException} hierarchy.
	 * <p>
	 * The callback action can return a result object, for example a domain object or a collection of domain objects.
	 *
	 * @param cql static CQL to execute, must not be {@literal null} or empty.
	 * @param action callback object that specifies the action, must not be {@literal null}.
	 * @return a result object returned by the action, or {@literal null}
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	<T> ListenableFuture<T> execute(String cql, PreparedStatementCallback<T> action) throws DataAccessException;

	/**
	 * Execute a query given static CQL, reading the {@link ResultSet} with a {@link AsyncResultSetExtractor}.
	 * <p>
	 * Uses a CQL Statement, not a {@link PreparedStatement}. If you want to execute a static query with a
	 * {@link PreparedStatement}, use the overloaded {@code query} method with {@literal null} as argument array.
	 *
	 * @param cql static CQL to execute, must not be {@literal null} or empty.
	 * @param resultSetExtractor object that will extract all rows of results, must not be {@literal null}.
	 * @return an arbitrary result object, as returned by the AsyncResultSetExtractor.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @see #query(String, AsyncResultSetExtractor, Object...)
	 */
	<T> ListenableFuture<T> query(String cql, AsyncResultSetExtractor<T> resultSetExtractor) throws DataAccessException;

	/**
	 * Execute a query given static CQL, reading the {@link ResultSet} on a per-row basis with a
	 * {@link RowCallbackHandler}.
	 * <p>
	 * Uses a CQL Statement, not a {@link PreparedStatement}. If you want to execute a static query with a
	 * {@link PreparedStatement}, use the overloaded {@code query} method with {@literal null} as argument array.
	 *
	 * @param cql static CQL to execute, must not be {@literal null} or empty.
	 * @param rowCallbackHandler object that will extract results, one row at a time, must not be {@literal null}.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @see #query(String, RowCallbackHandler, Object[])
	 */
	ListenableFuture<Void> query(String cql, RowCallbackHandler rowCallbackHandler) throws DataAccessException;

	/**
	 * Execute a query given static CQL, mapping each row to a Java object via a {@link RowMapper}.
	 * <p>
	 * Uses a CQL Statement, not a {@link PreparedStatement}. If you want to execute a static query with a
	 * {@link PreparedStatement}, use the overloaded {@code query} method with {@literal null} as argument array.
	 *
	 * @param cql static CQL to execute, must not be {@literal null} or empty.
	 * @param rowMapper object that will map one object per row, must not be {@literal null}.
	 * @return the result {@link List}, containing mapped objects.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @see #query(String, RowMapper, Object[])
	 */
	<T> ListenableFuture<List<T>> query(String cql, RowMapper<T> rowMapper) throws DataAccessException;

	/**
	 * Query given CQL to create a prepared statement from CQL and a list of arguments to bind to the query, reading the
	 * {@link ResultSet} with a {@link AsyncResultSetExtractor}.
	 *
	 * @param cql static CQL to execute, must not be {@literal null} or empty.
	 * @param resultSetExtractor object that will extract results, must not be {@literal null}.
	 * @param args arguments to bind to the query (leaving it to the {@link PreparedStatement} to guess the corresponding
	 *          CQL type).
	 * @return an arbitrary result object, as returned by the {@link AsyncResultSetExtractor}
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	<T> ListenableFuture<T> query(String cql, AsyncResultSetExtractor<T> resultSetExtractor, Object... args)
			throws DataAccessException;

	/**
	 * Query given CQL to create a prepared statement from CQL and a list of arguments to bind to the query, reading the
	 * {@link ResultSet} on a per-row basis with a {@link RowCallbackHandler}.
	 *
	 * @param cql static CQL to execute, must not be {@literal null} or empty.
	 * @param rowCallbackHandler object that will extract results, one row at a time, must not be {@literal null}.
	 * @param args arguments to bind to the query (leaving it to the {@link PreparedStatement} to guess the corresponding
	 *          CQL type)
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	ListenableFuture<Void> query(String cql, RowCallbackHandler rowCallbackHandler, Object... args)
			throws DataAccessException;

	/**
	 * Query given CQL to create a prepared statement from CQL and a list of arguments to bind to the query, mapping each
	 * row to a Java object via a {@link RowMapper}.
	 *
	 * @param cql static CQL to execute, must not be {@literal null} or empty.
	 * @param rowMapper object that will map one object per row
	 * @param args arguments to bind to the query (leaving it to the {@link PreparedStatement} to guess the corresponding
	 *          CQL type)
	 * @return the result {@link List}, containing mapped objects
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	<T> ListenableFuture<List<T>> query(String cql, RowMapper<T> rowMapper, Object... args) throws DataAccessException;

	/**
	 * Query using a prepared statement, reading the {@link ResultSet} with a {@link AsyncResultSetExtractor}.
	 *
	 * @param cql static CQL to execute, must not be {@literal null} or empty.
	 * @param psb object that knows how to set values on the prepared statement. If this is {@literal null}, the CQL will
	 *          be assumed to contain no bind parameters. Even if there are no bind parameters, this object may be used to
	 *          set fetch size and other performance options.
	 * @param resultSetExtractor object that will extract results, must not be {@literal null}.
	 * @return an arbitrary result object, as returned by the {@link AsyncResultSetExtractor}.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	<T> ListenableFuture<T> query(String cql, @Nullable PreparedStatementBinder psb,
			AsyncResultSetExtractor<T> resultSetExtractor) throws DataAccessException;

	/**
	 * Query given CQL to create a prepared statement from CQL and a {@link PreparedStatementBinder} implementation that
	 * knows how to bind values to the query, reading the {@link ResultSet} on a per-row basis with a
	 * {@link RowCallbackHandler}.
	 *
	 * @param cql static CQL to execute, must not be {@literal null} or empty.
	 * @param psb object that knows how to set values on the prepared statement. If this is {@literal null}, the CQL will
	 *          be assumed to contain no bind parameters. Even if there are no bind parameters, this object may be used to
	 *          set fetch size and other performance options.
	 * @param rowCallbackHandler object that will extract results, one row at a time, must not be {@literal null}.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	ListenableFuture<Void> query(String cql, @Nullable PreparedStatementBinder psb, RowCallbackHandler rowCallbackHandler)
			throws DataAccessException;

	/**
	 * Query given CQL to create a prepared statement from CQL and a {@link PreparedStatementBinder} implementation that
	 * knows how to bind values to the query, mapping each row to a Java object via a {@link RowMapper}.
	 *
	 * @param cql static CQL to execute, must not be {@literal null} or empty.
	 * @param psb object that knows how to set values on the prepared statement. If this is {@literal null}, the CQL will
	 *          be assumed to contain no bind parameters. Even if there are no bind parameters, this object may be used to
	 *          set fetch size and other performance options.
	 * @param rowMapper object that will map one object per row, must not be {@literal null}.
	 * @return the result {@link List}, containing mapped objects.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	<T> ListenableFuture<List<T>> query(String cql, @Nullable PreparedStatementBinder psb, RowMapper<T> rowMapper)
			throws DataAccessException;

	/**
	 * Execute a query for a result {@link List}, given static CQL.
	 * <p>
	 * Uses a CQL Statement, not a {@link PreparedStatement}. If you want to execute a static query with a
	 * {@link PreparedStatement}, use the overloaded {@code queryForList} method with {@literal null} as argument array.
	 * <p>
	 * The results will be mapped to a {@link List} (one item for each row) of {@link Map}s (one entry for each column
	 * using the column name as the key). Each item in the {@link List} will be of the form returned by this interface's
	 * queryForMap() methods.
	 *
	 * @param cql static CQL to execute, must not be {@literal null} or empty.
	 * @return a {@link List} that contains a {@link Map} per row.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @see #queryForList(String, Object[])
	 */
	ListenableFuture<List<Map<String, Object>>> queryForList(String cql) throws DataAccessException;

	/**
	 * Query given CQL to create a prepared statement from CQL and a list of arguments to bind to the query, expecting a
	 * result {@link List}.
	 * <p>
	 * The results will be mapped to a {@link List} (one item for each row) of {@link Map}s (one entry for each column,
	 * using the column name as the key). Each item in the {@link List} will be of the form returned by this interface's
	 * queryForMap() methods.
	 *
	 * @param cql static CQL to execute, must not be {@literal null} or empty.
	 * @param args arguments to bind to the query (leaving it to the {@link PreparedStatement} to guess the corresponding
	 *          CQL type).
	 * @return a {@link List} that contains a {@link Map} per row
	 * @throws DataAccessException if there is any problem executing the query.
	 * @see #queryForList(String)
	 */
	ListenableFuture<List<Map<String, Object>>> queryForList(String cql, Object... args) throws DataAccessException;

	/**
	 * Execute a query for a result {@link List}, given static CQL.
	 * <p>
	 * Uses a CQL Statement, not a {@link PreparedStatement}. If you want to execute a static query with a
	 * {@link PreparedStatement}, use the overloaded {@code queryForList} method with {@literal null} as argument array.
	 * <p>
	 * The results will be mapped to a {@link List} (one item for each row) of result objects, each of them matching the
	 * specified element type.
	 *
	 * @param cql static CQL to execute, must not be {@literal null} or empty.
	 * @param elementType the required type of element in the result {@link List} (for example, {@code Integer.class}),
	 *          must not be {@literal null}.
	 * @return a {@link List} of objects that match the specified element type.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @see #queryForList(String, Class, Object[])
	 * @see SingleColumnRowMapper
	 */
	<T> ListenableFuture<List<T>> queryForList(String cql, Class<T> elementType) throws DataAccessException;

	/**
	 * Query given CQL to create a prepared statement from CQL and a list of arguments to bind to the query, expecting a
	 * result {@link List}.
	 * <p>
	 * The results will be mapped to a {@link List} (one item for each row) of result objects, each of them matching the
	 * specified element type.
	 *
	 * @param cql static CQL to execute, must not be {@literal null} or empty.
	 * @param elementType the required type of element in the result {@link List} (for example, {@code Integer.class}),
	 *          must not be {@literal null}.
	 * @param args arguments to bind to the query (leaving it to the {@link PreparedStatement} to guess the corresponding
	 *          CQL type).
	 * @return a {@link List} of objects that match the specified element type.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @see #queryForList(String, Class)
	 * @see SingleColumnRowMapper
	 */
	<T> ListenableFuture<List<T>> queryForList(String cql, Class<T> elementType, Object... args)
			throws DataAccessException;

	/**
	 * Execute a query for a result Map, given static CQL.
	 * <p>
	 * Uses a CQL Statement, not a {@link PreparedStatement}. If you want to execute a static query with a
	 * {@link PreparedStatement}, use the overloaded {@link #queryForMap(String, Object...)} method with {@literal null}
	 * as argument array.
	 * <p>
	 * The query is expected to be a single row query; the result row will be mapped to a Map (one entry for each column,
	 * using the column name as the key).
	 *
	 * @param cql static CQL to execute, must not be {@literal null} or empty.
	 * @return the result Map (one entry for each column, using the column name as the key), must not be {@literal null}.
	 * @throws IncorrectResultSizeDataAccessException if the query does not return exactly one row.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @see #queryForMap(String, Object[])
	 * @see ColumnMapRowMapper
	 */
	ListenableFuture<Map<String, Object>> queryForMap(String cql) throws DataAccessException;

	/**
	 * Query given CQL to create a prepared statement from CQL and a list of arguments to bind to the query, expecting a
	 * result Map. The queryForMap() methods defined by this interface are appropriate when you don't have a domain model.
	 * Otherwise, consider using one of the queryForObject() methods.
	 * <p>
	 * The query is expected to be a single row query; the result row will be mapped to a Map (one entry for each column,
	 * using the column name as the key).
	 *
	 * @param cql static CQL to execute, must not be {@literal null} or empty.
	 * @param args arguments to bind to the query (leaving it to the {@link PreparedStatement} to guess the corresponding
	 *          CQL type).
	 * @return the result Map (one entry for each column, using the column name as the key).
	 * @throws IncorrectResultSizeDataAccessException if the query does not return exactly one row
	 * @throws DataAccessException if there is any problem executing the query.
	 * @see #queryForMap(String)
	 * @see ColumnMapRowMapper
	 */
	ListenableFuture<Map<String, Object>> queryForMap(String cql, Object... args) throws DataAccessException;

	/**
	 * Execute a query for a result object, given static CQL.
	 * <p>
	 * Uses a CQL Statement, not a {@link PreparedStatement}. If you want to execute a static query with a
	 * {@link PreparedStatement}, use the overloaded {@link #queryForObject(String, Class, Object...)} method with
	 * {@literal null} as argument array.
	 * <p>
	 * This method is useful for running static CQL with a known outcome. The query is expected to be a single row/single
	 * column query; the returned result will be directly mapped to the corresponding object type.
	 *
	 * @param cql static CQL to execute, must not be {@literal null} or empty.
	 * @param requiredType the type that the result object is expected to match, must not be {@literal null}.
	 * @return the result object of the required type, or {@literal null} in case of CQL NULL.
	 * @throws IncorrectResultSizeDataAccessException if the query does not return exactly one row, or does not return
	 *           exactly one column in that row.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @see #queryForObject(String, Class, Object[])
	 */
	<T> ListenableFuture<T> queryForObject(String cql, Class<T> requiredType) throws DataAccessException;

	/**
	 * Query given CQL to create a prepared statement from CQL and a list of arguments to bind to the query, expecting a
	 * result object.
	 * <p>
	 * The query is expected to be a single row/single column query; the returned result will be directly mapped to the
	 * corresponding object type.
	 *
	 * @param cql static CQL to execute, must not be {@literal null} or empty.
	 * @param requiredType the type that the result object is expected to match, must not be {@literal null}.
	 * @param args arguments to bind to the query (leaving it to the PreparedStatement to guess the corresponding CQL
	 *          type)
	 * @return the result object of the required type, or {@literal null} in case of CQL NULL.
	 * @throws IncorrectResultSizeDataAccessException if the query does not return exactly one row, or does not return
	 *           exactly one column in that row.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @see #queryForObject(String, Class)
	 */
	<T> ListenableFuture<T> queryForObject(String cql, Class<T> requiredType, Object... args) throws DataAccessException;

	/**
	 * Execute a query given static CQL, mapping a single result row to a Java object via a {@link RowMapper}.
	 * <p>
	 * Uses a CQL Statement, not a {@link PreparedStatement}. If you want to execute a static query with a
	 * {@link PreparedStatement}, use the overloaded {@link #queryForObject(String, RowMapper, Object...)} method with
	 * {@literal null} as argument array.
	 *
	 * @param cql static CQL to execute, must not be {@literal null} or empty.
	 * @param rowMapper object that will map one object per row, must not be {@literal null}.
	 * @return the single mapped object.
	 * @throws IncorrectResultSizeDataAccessException if the query does not return exactly one row.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @see #queryForObject(String, RowMapper, Object[])
	 */
	<T> ListenableFuture<T> queryForObject(String cql, RowMapper<T> rowMapper) throws DataAccessException;

	/**
	 * Query given CQL to create a prepared statement from CQL and a list of arguments to bind to the query, mapping a
	 * single result row to a Java object via a {@link RowMapper}.
	 *
	 * @param cql static CQL to execute, must not be {@literal null} or empty.
	 * @param rowMapper object that will map one object per row, must not be {@literal null}.
	 * @param args arguments to bind to the query (leaving it to the {@link PreparedStatement} to guess the corresponding
	 *          CQL type)
	 * @return the single mapped object
	 * @throws IncorrectResultSizeDataAccessException if the query does not return exactly one row.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	<T> ListenableFuture<T> queryForObject(String cql, RowMapper<T> rowMapper, Object... args) throws DataAccessException;

	/**
	 * Execute a query for a ResultSet, given static CQL.
	 * <p>
	 * Uses a CQL Statement, not a {@link PreparedStatement}. If you want to execute a static query with a
	 * {@link PreparedStatement}, use the overloaded {@code queryForResultSet} method with {@literal null} as argument
	 * array.
	 * <p>
	 * The results will be mapped to an {@link ResultSet}.
	 *
	 * @param cql static CQL to execute, must not be {@literal null} or empty.
	 * @return a {@link ResultSet} representation.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @see #queryForResultSet(String, Object[])
	 */
	ListenableFuture<AsyncResultSet> queryForResultSet(String cql) throws DataAccessException;

	/**
	 * Query given CQL to create a prepared statement from CQL and a list of arguments to bind to the query, expecting a
	 * ResultSet.
	 * <p>
	 * The results will be mapped to an {@link ResultSet}.
	 *
	 * @param cql static CQL to execute, must not be {@literal null} or empty.
	 * @param args arguments to bind to the query (leaving it to the {@link PreparedStatement} to guess the corresponding
	 *          CQL type).
	 * @return a {@link ResultSet} representation.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @see #queryForResultSet(String)
	 */
	ListenableFuture<AsyncResultSet> queryForResultSet(String cql, Object... args) throws DataAccessException;

	// -------------------------------------------------------------------------
	// Methods dealing with com.datastax.oss.driver.api.core.cql.Statement
	// -------------------------------------------------------------------------

	/**
	 * Issue a single CQL execute, typically a DDL statement, insert, update or delete statement.
	 *
	 * @param statement static CQL {@link Statement}, must not be {@literal null}.
	 * @return boolean value whether the statement was applied.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	ListenableFuture<Boolean> execute(Statement<?> statement) throws DataAccessException;

	/**
	 * Execute a query given static CQL, reading the {@link ResultSet} with a {@link AsyncResultSetExtractor}.
	 * <p>
	 * Uses a CQL Statement, not a {@link PreparedStatement}. If you want to execute a static query with a
	 * {@link PreparedStatement}, use the overloaded {@code query} method with {@literal null} as argument array.
	 *
	 * @param statement static CQL {@link Statement}, must not be {@literal null}.
	 * @param resultSetExtractor object that will extract all rows of results, must not be {@literal null}.
	 * @return an arbitrary result object, as returned by the AsyncResultSetExtractor.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @see #query(String, AsyncResultSetExtractor, Object...)
	 */
	<T> ListenableFuture<T> query(Statement<?> statement, AsyncResultSetExtractor<T> resultSetExtractor)
			throws DataAccessException;

	/**
	 * Execute a query given static CQL, reading the {@link ResultSet} on a per-row basis with a
	 * {@link RowCallbackHandler}.
	 * <p>
	 * Uses a CQL Statement, not a {@link PreparedStatement}. If you want to execute a static query with a
	 * {@link PreparedStatement}, use the overloaded {@code query} method with {@literal null} as argument array.
	 *
	 * @param statement static CQL {@link Statement}, must not be {@literal null}.
	 * @param rowCallbackHandler object that will extract results, one row at a time, must not be {@literal null}.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @see #query(String, RowCallbackHandler, Object[])
	 */
	ListenableFuture<Void> query(Statement<?> statement, RowCallbackHandler rowCallbackHandler)
			throws DataAccessException;

	/**
	 * Execute a query given static CQL, mapping each row to a Java object via a {@link RowMapper}.
	 * <p>
	 * Uses a CQL Statement, not a {@link PreparedStatement}. If you want to execute a static query with a
	 * {@link PreparedStatement}, use the overloaded {@code query} method with {@literal null} as argument array.
	 *
	 * @param statement static CQL {@link Statement}, must not be {@literal null}.
	 * @param rowMapper object that will map one object per row, must not be {@literal null}.
	 * @return the result {@link List}, containing mapped objects.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @see #query(String, RowMapper, Object[])
	 */
	<T> ListenableFuture<List<T>> query(Statement<?> statement, RowMapper<T> rowMapper) throws DataAccessException;

	/**
	 * Execute a query for a result {@link List}, given static CQL.
	 * <p>
	 * Uses a CQL Statement, not a {@link PreparedStatement}. If you want to execute a static query with a
	 * {@link PreparedStatement}, use the overloaded {@code queryForList} method with {@literal null} as argument array.
	 * <p>
	 * The results will be mapped to a {@link List} (one item for each row) of {@link Map}s (one entry for each column
	 * using the column name as the key). Each item in the {@link List} will be of the form returned by this interface's
	 * queryForMap() methods.
	 *
	 * @param statement static CQL {@link Statement} to execute, must not be {@literal null} or empty.
	 * @return a {@link List} that contains a {@link Map} per row.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @see #queryForList(String, Object[])
	 */
	ListenableFuture<List<Map<String, Object>>> queryForList(Statement<?> statement) throws DataAccessException;

	/**
	 * Execute a query for a result {@link List}, given static CQL.
	 * <p>
	 * Uses a CQL Statement, not a {@link PreparedStatement}. If you want to execute a static query with a
	 * {@link PreparedStatement}, use the overloaded {@code queryForList} method with {@literal null} as argument array.
	 * <p>
	 * The results will be mapped to a {@link List} (one item for each row) of result objects, each of them matching the
	 * specified element type.
	 *
	 * @param statement static CQL {@link Statement}, must not be {@literal null}.
	 * @param elementType the required type of element in the result {@link List} (for example, {@code Integer.class}),
	 *          must not be {@literal null}.
	 * @return a {@link List} of objects that match the specified element type.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @see #queryForList(String, Class, Object[])
	 * @see SingleColumnRowMapper
	 */
	<T> ListenableFuture<List<T>> queryForList(Statement<?> statement, Class<T> elementType) throws DataAccessException;

	/**
	 * Execute a query for a result Map, given static CQL.
	 * <p>
	 * Uses a CQL Statement, not a {@link PreparedStatement}. If you want to execute a static query with a
	 * {@link PreparedStatement}, use the overloaded {@link #queryForMap(String, Object...)} method with {@literal null}
	 * as argument array.
	 * <p>
	 * The query is expected to be a single row query; the result row will be mapped to a Map (one entry for each column,
	 * using the column name as the key).
	 *
	 * @param statement static CQL {@link Statement}, must not be {@literal null}.
	 * @return the result Map (one entry for each column, using the column name as the key), must not be {@literal null}.
	 * @throws IncorrectResultSizeDataAccessException if the query does not return exactly one row.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @see #queryForMap(String, Object[])
	 * @see ColumnMapRowMapper
	 */
	ListenableFuture<Map<String, Object>> queryForMap(Statement<?> statement) throws DataAccessException;

	/**
	 * Execute a query for a result object, given static CQL.
	 * <p>
	 * Uses a CQL Statement, not a {@link PreparedStatement}. If you want to execute a static query with a
	 * {@link PreparedStatement}, use the overloaded {@link #queryForObject(String, Class, Object...)} method with
	 * {@literal null} as argument array.
	 * <p>
	 * This method is useful for running static CQL with a known outcome. The query is expected to be a single row/single
	 * column query; the returned result will be directly mapped to the corresponding object type.
	 *
	 * @param statement static CQL {@link Statement}, must not be {@literal null}.
	 * @param requiredType the type that the result object is expected to match, must not be {@literal null}.
	 * @return the result object of the required type, or {@literal null} in case of CQL NULL.
	 * @throws IncorrectResultSizeDataAccessException if the query does not return exactly one row, or does not return
	 *           exactly one column in that row.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @see #queryForObject(String, Class, Object[])
	 */
	<T> ListenableFuture<T> queryForObject(Statement<?> statement, Class<T> requiredType) throws DataAccessException;

	/**
	 * Execute a query given static CQL, mapping a single result row to a Java object via a {@link RowMapper}.
	 * <p>
	 * Uses a CQL Statement, not a {@link PreparedStatement}. If you want to execute a static query with a
	 * {@link PreparedStatement}, use the overloaded {@link #queryForObject(String, RowMapper, Object...)} method with
	 * {@literal null} as argument array.
	 *
	 * @param statement static CQL {@link Statement}, must not be {@literal null}.
	 * @param rowMapper object that will map one object per row, must not be {@literal null}.
	 * @return the single mapped object.
	 * @throws IncorrectResultSizeDataAccessException if the query does not return exactly one row.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @see #queryForObject(String, RowMapper, Object[])
	 */
	<T> ListenableFuture<T> queryForObject(Statement<?> statement, RowMapper<T> rowMapper) throws DataAccessException;

	/**
	 * Execute a query for a ResultSet, given static CQL.
	 * <p>
	 * Uses a CQL Statement, not a {@link PreparedStatement}. If you want to execute a static query with a
	 * {@link PreparedStatement}, use the overloaded {@code queryForResultSet} method with {@literal null} as argument
	 * array.
	 * <p>
	 * The results will be mapped to an {@link ResultSet}.
	 *
	 * @param statement static CQL {@link Statement}, must not be {@literal null}.
	 * @return a {@link ResultSet} representation.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @see #queryForResultSet(String, Object[])
	 */
	ListenableFuture<AsyncResultSet> queryForResultSet(Statement<?> statement) throws DataAccessException;

	// -------------------------------------------------------------------------
	// Methods dealing with com.datastax.oss.driver.api.core.cql.PreparedStatement
	// -------------------------------------------------------------------------

	/**
	 * Issue a single CQL execute operation (such as an insert, update or delete statement) using a
	 * {@link AsyncPreparedStatementCreator} to provide CQL and any required parameters.
	 *
	 * @param preparedStatementCreator object that provides CQL and any necessary parameters, must not be {@literal null}.
	 * @return boolean value whether the statement was applied.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	ListenableFuture<Boolean> execute(AsyncPreparedStatementCreator preparedStatementCreator) throws DataAccessException;

	/**
	 * Execute a CQL data access operation, implemented as callback action working on a CQL {@link PreparedStatement}.
	 * This allows for implementing arbitrary data access operations on a single {@link PreparedStatement}, within
	 * Spring's managed CQL environment: that is, participating in Spring-managed transactions and converting CQL
	 * {@link com.datastax.oss.driver.api.core.DriverException}s into Spring's {@link DataAccessException} hierarchy.
	 * <p>
	 * The callback action can return a result object, for example a domain object or a collection of domain objects.
	 *
	 * @param preparedStatementCreator object that can create a {@link PreparedStatement} given a
	 *          {@link com.datastax.oss.driver.api.core.CqlSession}, must not be {@literal null}.
	 * @param action callback object that specifies the action, must not be {@literal null}.
	 * @return a result object returned by the action, or {@literal null}.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	<T> ListenableFuture<T> execute(AsyncPreparedStatementCreator preparedStatementCreator,
			PreparedStatementCallback<T> action) throws DataAccessException;

	/**
	 * Query using a prepared statement, reading the {@link ResultSet} with a {@link AsyncResultSetExtractor}.
	 *
	 * @param preparedStatementCreator object that can create a {@link PreparedStatement} given a
	 *          {@link com.datastax.oss.driver.api.core.CqlSession}, must not be {@literal null}.
	 * @param resultSetExtractor object that will extract results, must not be {@literal null}.
	 * @return an arbitrary result object, as returned by the {@link AsyncResultSetExtractor}
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	<T> ListenableFuture<T> query(AsyncPreparedStatementCreator preparedStatementCreator,
			AsyncResultSetExtractor<T> resultSetExtractor) throws DataAccessException;

	/**
	 * Query using a prepared statement, reading the {@link ResultSet} on a per-row basis with a
	 * {@link RowCallbackHandler}.
	 *
	 * @param preparedStatementCreator object that can create a {@link PreparedStatement} given a
	 *          {@link com.datastax.oss.driver.api.core.CqlSession}, must not be {@literal null}.
	 * @param rowCallbackHandler object that will extract results, one row at a time, must not be {@literal null}.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	ListenableFuture<Void> query(AsyncPreparedStatementCreator preparedStatementCreator,
			RowCallbackHandler rowCallbackHandler) throws DataAccessException;

	/**
	 * Query using a prepared statement, mapping each row to a Java object via a {@link RowMapper}.
	 *
	 * @param preparedStatementCreator object that can create a {@link PreparedStatement} given a
	 *          {@link com.datastax.oss.driver.api.core.CqlSession}, must not be {@literal null}.
	 * @param rowMapper object that will map one object per row, must not be {@literal null}.
	 * @return the result {@link List}, containing mapped objects.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	<T> ListenableFuture<List<T>> query(AsyncPreparedStatementCreator preparedStatementCreator, RowMapper<T> rowMapper)
			throws DataAccessException;

	/**
	 * Query using a prepared statement and a {@link PreparedStatementBinder} implementation that knows how to bind values
	 * to the query, reading the {@link ResultSet} with a {@link AsyncResultSetExtractor}.
	 *
	 * @param preparedStatementCreator object that can create a {@link PreparedStatement} given a
	 *          {@link com.datastax.oss.driver.api.core.CqlSession}, must not be {@literal null}.
	 * @param psb object that knows how to set values on the prepared statement. If this is {@literal null}, the CQL will
	 *          be assumed to contain no bind parameters. Even if there are no bind parameters, this object may be used to
	 *          set fetch size and other performance options.
	 * @param resultSetExtractor object that will extract results, must not be {@literal null}.
	 * @return an arbitrary result object, as returned by the {@link AsyncResultSetExtractor}.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	<T> ListenableFuture<T> query(AsyncPreparedStatementCreator preparedStatementCreator,
			@Nullable PreparedStatementBinder psb, AsyncResultSetExtractor<T> resultSetExtractor) throws DataAccessException;

	/**
	 * Query using a prepared statement and a {@link PreparedStatementBinder} implementation that knows how to bind values
	 * to the query, reading the {@link ResultSet} on a per-row basis with a {@link RowCallbackHandler}.
	 *
	 * @param preparedStatementCreator object that can create a {@link PreparedStatement} given a
	 *          {@link com.datastax.oss.driver.api.core.CqlSession}, must not be {@literal null}.
	 * @param psb object that knows how to set values on the prepared statement. If this is {@literal null}, the CQL will
	 *          be assumed to contain no bind parameters. Even if there are no bind parameters, this object may be used to
	 *          set fetch size and other performance options.
	 * @param rowCallbackHandler object that will extract results, one row at a time, must not be {@literal null}.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	ListenableFuture<Void> query(AsyncPreparedStatementCreator preparedStatementCreator,
			@Nullable PreparedStatementBinder psb, RowCallbackHandler rowCallbackHandler) throws DataAccessException;

	/**
	 * Query using a prepared statement and a {@link PreparedStatementBinder} implementation that knows how to bind values
	 * to the query, mapping each row to a Java object via a {@link RowMapper}.
	 *
	 * @param preparedStatementCreator object that can create a {@link PreparedStatement} given a
	 *          {@link com.datastax.oss.driver.api.core.CqlSession}, must not be {@literal null}.
	 * @param psb object that knows how to set values on the prepared statement. If this is {@literal null}, the CQL will
	 *          be assumed to contain no bind parameters. Even if there are no bind parameters, this object may be used to
	 *          set fetch size and other performance options.
	 * @param rowMapper object that will map one object per row, must not be {@literal null}.
	 * @return the result {@link List}, containing mapped objects.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	<T> ListenableFuture<List<T>> query(AsyncPreparedStatementCreator preparedStatementCreator,
			@Nullable PreparedStatementBinder psb, RowMapper<T> rowMapper) throws DataAccessException;
}
