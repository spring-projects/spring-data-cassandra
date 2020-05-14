/*
 * Copyright 2016-2020 the original author or authors.
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

import java.util.Map;

import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.dao.DataAccessException;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.data.cassandra.ReactiveResultSet;
import org.springframework.lang.Nullable;

/**
 * Interface specifying a basic set of CQL operations executed in a reactive fashion. Implemented by
 * {@link ReactiveCqlTemplate}. Not often used directly, but a useful option to enhance testability, as it can easily be
 * mocked or stubbed.
 *
 * @author Mark Paluch
 * @since 2.0
 * @see ReactiveCqlTemplate
 * @see Mono
 * @see Flux
 */
public interface ReactiveCqlOperations {

	// -------------------------------------------------------------------------
	// Methods dealing with a plain ReactiveSession
	// -------------------------------------------------------------------------

	/**
	 * Execute a CQL data access operation, implemented as callback action working on a
	 * {@link org.springframework.data.cassandra.ReactiveSession}. This allows for implementing arbitrary data access
	 * operations, within Spring's managed CQL environment: that is, converting CQL
	 * {@link com.datastax.oss.driver.api.core.DriverException}s into Spring's {@link DataAccessException} hierarchy.
	 * <p>
	 * The callback action can return a result object, for example a domain object or a collection of domain objects.
	 *
	 * @param action the callback object that specifies the action.
	 * @return a result object returned by the action, or {@literal null}.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	<T> Flux<T> execute(ReactiveSessionCallback<T> action) throws DataAccessException;

	// -------------------------------------------------------------------------
	// Methods dealing with static CQL
	// -------------------------------------------------------------------------

	/**
	 * Issue a single CQL execute, typically a DDL statement, insert, update or delete statement.
	 *
	 * @param cql static CQL to execute, must not be empty or {@literal null}.
	 * @return boolean value whether the statement was applied.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	Mono<Boolean> execute(String cql) throws DataAccessException;

	/**
	 * Execute a query given static CQL, reading the {@link ReactiveResultSet} with a {@link ReactiveResultSetExtractor}.
	 * <p>
	 * Uses a CQL Statement, not a {@link PreparedStatement}. If you want to execute a static query with a
	 * {@link PreparedStatement}, use the overloaded {@code query} method with {@literal null} as argument array.
	 *
	 * @param cql static CQL to execute, must not be empty or {@literal null}.
	 * @param rse object that will extract all rows of results, must not be {@literal null}.
	 * @return an arbitrary result object, as returned by the ReactiveResultSetExtractor.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @see #query(String, ReactiveResultSetExtractor, Object...)
	 */
	<T> Flux<T> query(String cql, ReactiveResultSetExtractor<T> rse) throws DataAccessException;

	/**
	 * Execute a query given static CQL, mapping each row to a Java object via a {@link RowMapper}.
	 * <p>
	 * Uses a CQL Statement, not a {@link PreparedStatement}. If you want to execute a static query with a
	 * {@link PreparedStatement}, use the overloaded {@code query} method with {@literal null} as argument array.
	 *
	 * @param cql static CQL to execute, must not be empty or {@literal null}.
	 * @param rowMapper object that will map one object per row, must not be {@literal null}.
	 * @return the result {@link Flux}, containing mapped objects.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @see #query(String, RowMapper, Object[])
	 */
	<T> Flux<T> query(String cql, RowMapper<T> rowMapper) throws DataAccessException;

	/**
	 * Execute a query given static CQL, mapping a single result row to a Java object via a {@link RowMapper}.
	 * <p>
	 * Uses a CQL Statement, not a {@link PreparedStatement}. If you want to execute a static query with a
	 * {@link PreparedStatement}, use the overloaded {@link #queryForObject(String, RowMapper, Object...)} method with
	 * {@literal null} as argument array.
	 *
	 * @param cql static CQL to execute, must not be empty or {@literal null}.
	 * @param rowMapper object that will map one object per row, must not be {@literal null}.
	 * @return the single mapped object.
	 * @throws IncorrectResultSizeDataAccessException if the query does not return exactly one row.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @see #queryForObject(String, RowMapper, Object[])
	 */
	<T> Mono<T> queryForObject(String cql, RowMapper<T> rowMapper) throws DataAccessException;

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
	 * @param cql static CQL to execute, must not be empty or {@literal null}.
	 * @param requiredType the type that the result object is expected to match, must not be {@literal null}.
	 * @return the result object of the required type, or {@link Mono#empty()} in case of CQL NULL.
	 * @throws IncorrectResultSizeDataAccessException if the query does not return exactly one row, or does not return
	 *           exactly one column in that row.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @see #queryForObject(String, Class, Object[])
	 */
	<T> Mono<T> queryForObject(String cql, Class<T> requiredType) throws DataAccessException;

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
	 * @param cql static CQL to execute, must not be empty or {@literal null}.
	 * @return the result Map (one entry for each column, using the column name as the key), must not be {@literal null}.
	 * @throws IncorrectResultSizeDataAccessException if the query does not return exactly one row.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @see #queryForMap(String, Object[])
	 * @see ColumnMapRowMapper
	 */
	Mono<Map<String, Object>> queryForMap(String cql) throws DataAccessException;

	/**
	 * Execute a query for a result {@link Flux}, given static CQL.
	 * <p>
	 * Uses a CQL Statement, not a {@link PreparedStatement}. If you want to execute a static query with a
	 * {@link PreparedStatement}, use the overloaded {@code queryForFlux} method with {@literal null} as argument array.
	 * <p>
	 * The results will be mapped to a {@link Flux} (one item for each row) of result objects, each of them matching the
	 * specified element type.
	 *
	 * @param cql static CQL to execute, must not be empty or {@literal null}.
	 * @param elementType the required type of element in the result {@link Flux} (for example, {@code Integer.class}),
	 *          must not be {@literal null}.
	 * @return a {@link Flux} of objects that match the specified element type.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @see #queryForFlux(String, Class, Object[])
	 * @see SingleColumnRowMapper
	 */
	<T> Flux<T> queryForFlux(String cql, Class<T> elementType) throws DataAccessException;

	/**
	 * Execute a query for a result {@link Flux}, given static CQL.
	 * <p>
	 * Uses a CQL Statement, not a {@link PreparedStatement}. If you want to execute a static query with a
	 * {@link PreparedStatement}, use the overloaded {@code queryForFlux} method with {@literal null} as argument array.
	 * <p>
	 * The results will be mapped to a {@link Flux} (one item for each row) of {@link Map}s (one entry for each column
	 * using the column name as the key). Each item in the {@link Flux} will be of the form returned by this interface's
	 * queryForMap() methods.
	 *
	 * @param cql static CQL to execute, must not be empty or {@literal null}.
	 * @return a {@link Flux} that contains a {@link Map} per row.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @see #queryForFlux(String, Object[])
	 */
	Flux<Map<String, Object>> queryForFlux(String cql) throws DataAccessException;

	/**
	 * Execute a query for a ResultSet, given static CQL.
	 * <p>
	 * Uses a CQL Statement, not a {@link PreparedStatement}. If you want to execute a static query with a
	 * {@link PreparedStatement}, use the overloaded {@code queryForResultSet} method with {@literal null} as argument
	 * array.
	 * <p>
	 * The results will be mapped to an {@link ReactiveResultSet}.
	 *
	 * @param cql static CQL to execute, must not be empty or {@literal null}.
	 * @return a {@link ReactiveResultSet} representation.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @see #queryForResultSet(String, Object[])
	 */
	Mono<ReactiveResultSet> queryForResultSet(String cql) throws DataAccessException;

	/**
	 * Execute a query for Rows, given static CQL.
	 * <p>
	 * Uses a CQL Statement, not a {@link PreparedStatement}. If you want to execute a static query with a
	 * {@link PreparedStatement}, use the overloaded {@code queryForResultSet} method with {@literal null} as argument
	 * array.
	 * <p>
	 * The results will be mapped to {@link Row}s.
	 *
	 * @param cql static CQL to execute, must not be empty or {@literal null}.
	 * @return a Row representation.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @see #queryForResultSet(String, Object[])
	 */
	Flux<Row> queryForRows(String cql) throws DataAccessException;

	/**
	 * Issue multiple CQL statements from a CQL statement {@link Publisher}.
	 *
	 * @param statementPublisher defining a {@link Publisher} of CQL statements that will be executed.
	 * @return an array of the number of rows affected by each statement
	 * @throws DataAccessException if there is any problem executing the batch.
	 */
	Flux<Boolean> execute(Publisher<String> statementPublisher) throws DataAccessException;

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
	Mono<Boolean> execute(Statement<?> statement) throws DataAccessException;

	/**
	 * Execute a query given static CQL, reading the {@link ReactiveResultSet} with a {@link ReactiveResultSetExtractor}.
	 * <p>
	 * Uses a CQL Statement, not a {@link PreparedStatement}. If you want to execute a static query with a
	 * {@link PreparedStatement}, use the overloaded {@code query} method with {@literal null} as argument array.
	 *
	 * @param statement static CQL {@link Statement}, must not be {@literal null}.
	 * @param rse object that will extract all rows of results, must not be {@literal null}.
	 * @return an arbitrary result object, as returned by the ReactiveResultSetExtractor.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @see #query(String, ReactiveResultSetExtractor, Object...)
	 */
	<T> Flux<T> query(Statement<?> statement, ReactiveResultSetExtractor<T> rse) throws DataAccessException;

	/**
	 * Execute a query given static CQL, mapping each row to a Java object via a {@link RowMapper}.
	 * <p>
	 * Uses a CQL Statement, not a {@link PreparedStatement}. If you want to execute a static query with a
	 * {@link PreparedStatement}, use the overloaded {@code query} method with {@literal null} as argument array.
	 *
	 * @param statement static CQL {@link Statement}, must not be {@literal null}.
	 * @param rowMapper object that will map one object per row, must not be {@literal null}.
	 * @return the result {@link Flux}, containing mapped objects.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @see #query(String, RowMapper, Object[])
	 */
	<T> Flux<T> query(Statement<?> statement, RowMapper<T> rowMapper) throws DataAccessException;

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
	<T> Mono<T> queryForObject(Statement<?> statement, RowMapper<T> rowMapper) throws DataAccessException;

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
	 * @return the result object of the required type, or {@link Mono#empty()} in case of CQL NULL.
	 * @throws IncorrectResultSizeDataAccessException if the query does not return exactly one row, or does not return
	 *           exactly one column in that row.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @see #queryForObject(String, Class, Object[])
	 */
	<T> Mono<T> queryForObject(Statement<?> statement, Class<T> requiredType) throws DataAccessException;

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
	Mono<Map<String, Object>> queryForMap(Statement<?> statement) throws DataAccessException;

	/**
	 * Execute a query for a result {@link Flux}, given static CQL.
	 * <p>
	 * Uses a CQL Statement, not a {@link PreparedStatement}. If you want to execute a static query with a
	 * {@link PreparedStatement}, use the overloaded {@code queryForFlux} method with {@literal null} as argument array.
	 * <p>
	 * The results will be mapped to a {@link Flux} (one item for each row) of result objects, each of them matching the
	 * specified element type.
	 *
	 * @param statement static CQL {@link Statement}, must not be {@literal null}.
	 * @param elementType the required type of element in the result {@link Flux} (for example, {@code Integer.class}),
	 *          must not be {@literal null}.
	 * @return a {@link Flux} of objects that match the specified element type.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @see #queryForFlux(String, Class, Object[])
	 * @see SingleColumnRowMapper
	 */
	<T> Flux<T> queryForFlux(Statement<?> statement, Class<T> elementType) throws DataAccessException;

	/**
	 * Execute a query for a result {@link Flux}, given static CQL.
	 * <p>
	 * Uses a CQL Statement, not a {@link PreparedStatement}. If you want to execute a static query with a
	 * {@link PreparedStatement}, use the overloaded {@code queryForFlux} method with {@literal null} as argument array.
	 * <p>
	 * The results will be mapped to a {@link Flux} (one item for each row) of {@link Map}s (one entry for each column
	 * using the column name as the key). Each item in the {@link Flux} will be of the form returned by this interface's
	 * queryForMap() methods.
	 *
	 * @param statement static CQL {@link Statement}, must not be {@literal null}.
	 * @return a {@link Flux} that contains a {@link Map} per row.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @see #queryForFlux(String, Object[])
	 */
	Flux<Map<String, Object>> queryForFlux(Statement<?> statement) throws DataAccessException;

	/**
	 * Execute a query for a ResultSet, given static CQL.
	 * <p>
	 * Uses a CQL Statement, not a {@link PreparedStatement}. If you want to execute a static query with a
	 * {@link PreparedStatement}, use the overloaded {@code queryForResultSet} method with {@literal null} as argument
	 * array.
	 * <p>
	 * The results will be mapped to an {@link ReactiveResultSet}.
	 *
	 * @param statement static CQL {@link Statement}, must not be {@literal null}.
	 * @return a {@link ReactiveResultSet} representation.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @see #queryForResultSet(String, Object[])
	 */
	Mono<ReactiveResultSet> queryForResultSet(Statement<?> statement) throws DataAccessException;

	/**
	 * Execute a query for Rows, given static CQL.
	 * <p>
	 * Uses a CQL Statement, not a {@link PreparedStatement}. If you want to execute a static query with a
	 * {@link PreparedStatement}, use the overloaded {@code queryForResultSet} method with {@literal null} as argument
	 * array.
	 * <p>
	 * The results will be mapped to {@link Row}s.
	 *
	 * @param statement static CQL {@link Statement}, must not be {@literal null}.
	 * @return a Row representation.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @see #queryForResultSet(String, Object[])
	 */
	Flux<Row> queryForRows(Statement<?> statement) throws DataAccessException;

	// -------------------------------------------------------------------------
	// Methods dealing with com.datastax.oss.driver.api.core.cql.PreparedStatement
	// -------------------------------------------------------------------------

	/**
	 * Execute a CQL data access operation, implemented as callback action working on a CQL {@link PreparedStatement}.
	 * This allows for implementing arbitrary data access operations on a single {@link PreparedStatement}, within
	 * Spring's managed CQL environment: that is, participating in Spring-managed transactions and converting CQL
	 * {@link com.datastax.oss.driver.api.core.DriverException}s into Spring's {@link DataAccessException} hierarchy.
	 * <p>
	 * The callback action can return a result object, for example a domain object or a collection of domain objects.
	 *
	 * @param psc object that can create a {@link PreparedStatement} given a
	 *          {@link org.springframework.data.cassandra.ReactiveSession}, must not be {@literal null}.
	 * @param action callback object that specifies the action, must not be {@literal null}.
	 * @return a result object returned by the action, or {@literal null}.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	<T> Flux<T> execute(ReactivePreparedStatementCreator psc, ReactivePreparedStatementCallback<T> action)
			throws DataAccessException;

	/**
	 * Execute a CQL data access operation, implemented as callback action working on a CQL {@link PreparedStatement}.
	 * This allows for implementing arbitrary data access operations on a single Statement, within Spring's managed CQL
	 * environment: that is, participating in Spring-managed transactions and converting CQL
	 * {@link com.datastax.oss.driver.api.core.DriverException}s into Spring's {@link DataAccessException} hierarchy.
	 * <p>
	 * The callback action can return a result object, for example a domain object or a collection of domain objects.
	 *
	 * @param cql static CQL to execute, must not be empty or {@literal null}.
	 * @param action callback object that specifies the action, must not be {@literal null}.
	 * @return a result object returned by the action, or {@literal null}
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	<T> Flux<T> execute(String cql, ReactivePreparedStatementCallback<T> action) throws DataAccessException;

	/**
	 * Query using a prepared statement, reading the {@link ReactiveResultSet} with a {@link ReactiveResultSetExtractor}.
	 *
	 * @param psc object that can create a {@link PreparedStatement} given a
	 *          {@link org.springframework.data.cassandra.ReactiveSession}, must not be {@literal null}.
	 * @param rse object that will extract results, must not be {@literal null}.
	 * @return an arbitrary result object, as returned by the {@link ReactiveResultSetExtractor}
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	<T> Flux<T> query(ReactivePreparedStatementCreator psc, ReactiveResultSetExtractor<T> rse) throws DataAccessException;

	/**
	 * Query using a prepared statement, reading the {@link ReactiveResultSet} with a {@link ReactiveResultSetExtractor}.
	 *
	 * @param cql static CQL to execute, must not be empty or {@literal null}.
	 * @param psb object that knows how to set values on the prepared statement. If this is {@literal null}, the CQL will
	 *          be assumed to contain no bind parameters. Even if there are no bind parameters, this object may be used to
	 *          set fetch size and other performance options.
	 * @param rse object that will extract results, must not be {@literal null}.
	 * @return an arbitrary result object, as returned by the {@link ReactiveResultSetExtractor}.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	<T> Flux<T> query(String cql, @Nullable PreparedStatementBinder psb, ReactiveResultSetExtractor<T> rse)
			throws DataAccessException;

	/**
	 * Query using a prepared statement and a {@link PreparedStatementBinder} implementation that knows how to bind values
	 * to the query, reading the {@link ReactiveResultSet} with a {@link ResultSetExtractor}.
	 *
	 * @param psc object that can create a {@link PreparedStatement} given a
	 *          {@link com.datastax.oss.driver.api.core.CqlSession}, must not be {@literal null}.
	 * @param psb object that knows how to set values on the prepared statement. If this is {@literal null}, the CQL will
	 *          be assumed to contain no bind parameters. Even if there are no bind parameters, this object may be used to
	 *          set fetch size and other performance options.
	 * @param rse object that will extract results, must not be {@literal null}.
	 * @return an arbitrary result object, as returned by the {@link ResultSetExtractor}.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	<T> Flux<T> query(ReactivePreparedStatementCreator psc, @Nullable PreparedStatementBinder psb,
			ReactiveResultSetExtractor<T> rse) throws DataAccessException;

	/**
	 * Query given CQL to create a prepared statement from CQL and a list of arguments to bind to the query, reading the
	 * {@link ReactiveResultSet} with a {@link ReactiveResultSetExtractor}.
	 *
	 * @param cql static CQL to execute, must not be empty or {@literal null}.
	 * @param rse object that will extract results, must not be {@literal null}.
	 * @param args arguments to bind to the query (leaving it to the {@link PreparedStatement} to guess the corresponding
	 *          CQL type).
	 * @return an arbitrary result object, as returned by the {@link ReactiveResultSetExtractor}
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	<T> Flux<T> query(String cql, ReactiveResultSetExtractor<T> rse, Object... args) throws DataAccessException;

	/**
	 * Query using a prepared statement, mapping each row to a Java object via a {@link RowMapper}.
	 *
	 * @param psc object that can create a {@link PreparedStatement} given a
	 *          {@link org.springframework.data.cassandra.ReactiveSession}, must not be {@literal null}.
	 * @param rowMapper object that will map one object per row, must not be {@literal null}.
	 * @return the result {@link Flux}, containing mapped objects.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	<T> Flux<T> query(ReactivePreparedStatementCreator psc, RowMapper<T> rowMapper) throws DataAccessException;

	/**
	 * Query given CQL to create a prepared statement from CQL and a {@link PreparedStatement}Binder implementation that
	 * knows how to bind values to the query, mapping each row to a Java object via a {@link RowMapper}.
	 *
	 * @param cql static CQL to execute, must not be empty or {@literal null}.
	 * @param psb object that knows how to set values on the prepared statement. If this is {@literal null}, the CQL will
	 *          be assumed to contain no bind parameters. Even if there are no bind parameters, this object may be used to
	 *          set fetch size and other performance options.
	 * @param rowMapper object that will map one object per row, must not be {@literal null}.
	 * @return the result {@link Flux}, containing mapped objects.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	<T> Flux<T> query(String cql, @Nullable PreparedStatementBinder psb, RowMapper<T> rowMapper)
			throws DataAccessException;

	/**
	 * Query using a prepared statement and a {@link PreparedStatementBinder} implementation that knows how to bind values
	 * to the query, mapping each row to a Java object via a {@link RowMapper}.
	 *
	 * @param psc object that can create a {@link PreparedStatement} given a
	 *          {@link com.datastax.oss.driver.api.core.CqlSession}, must not be {@literal null}.
	 * @param psb object that knows how to set values on the prepared statement. If this is {@literal null}, the CQL will
	 *          be assumed to contain no bind parameters. Even if there are no bind parameters, this object may be used to
	 *          set fetch size and other performance options.
	 * @param rowMapper object that will map one object per row, must not be {@literal null}.
	 * @return the result {@link Flux}, containing mapped objects.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	<T> Flux<T> query(ReactivePreparedStatementCreator psc, @Nullable PreparedStatementBinder psb, RowMapper<T> rowMapper)
			throws DataAccessException;

	/**
	 * Query given CQL to create a prepared statement from CQL and a list of arguments to bind to the query, mapping each
	 * row to a Java object via a {@link RowMapper}.
	 *
	 * @param cql static CQL to execute, must not be empty or {@literal null}.
	 * @param rowMapper object that will map one object per row
	 * @param args arguments to bind to the query (leaving it to the {@link PreparedStatement} to guess the corresponding
	 *          CQL type)
	 * @return the result {@link Flux}, containing mapped objects
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	<T> Flux<T> query(String cql, RowMapper<T> rowMapper, Object... args) throws DataAccessException;

	/**
	 * Query given CQL to create a prepared statement from CQL and a list of arguments to bind to the query, mapping a
	 * single result row to a Java object via a {@link RowMapper}.
	 *
	 * @param cql static CQL to execute, must not be empty or {@literal null}.
	 * @param rowMapper object that will map one object per row, must not be {@literal null}.
	 * @param args arguments to bind to the query (leaving it to the {@link PreparedStatement} to guess the corresponding
	 *          CQL type)
	 * @return the single mapped object
	 * @throws IncorrectResultSizeDataAccessException if the query does not return exactly one row.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	<T> Mono<T> queryForObject(String cql, RowMapper<T> rowMapper, Object... args) throws DataAccessException;

	/**
	 * Query given CQL to create a prepared statement from CQL and a list of arguments to bind to the query, expecting a
	 * result object.
	 * <p>
	 * The query is expected to be a single row/single column query; the returned result will be directly mapped to the
	 * corresponding object type.
	 *
	 * @param cql static CQL to execute, must not be empty or {@literal null}.
	 * @param requiredType the type that the result object is expected to match, must not be {@literal null}.
	 * @param args arguments to bind to the query (leaving it to the PreparedStatement to guess the corresponding CQL
	 *          type)
	 * @return the result object of the required type, or {@link Mono#empty()} in case of CQL NULL.
	 * @throws IncorrectResultSizeDataAccessException if the query does not return exactly one row, or does not return
	 *           exactly one column in that row.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @see #queryForObject(String, Class)
	 */
	<T> Mono<T> queryForObject(String cql, Class<T> requiredType, Object... args) throws DataAccessException;

	/**
	 * Query given CQL to create a prepared statement from CQL and a list of arguments to bind to the query, expecting a
	 * result Map. The queryForMap() methods defined by this interface are appropriate when you don't have a domain model.
	 * Otherwise, consider using one of the queryForObject() methods.
	 * <p>
	 * The query is expected to be a single row query; the result row will be mapped to a Map (one entry for each column,
	 * using the column name as the key).
	 *
	 * @param cql static CQL to execute, must not be empty or {@literal null}.
	 * @param args arguments to bind to the query (leaving it to the {@link PreparedStatement} to guess the corresponding
	 *          CQL type).
	 * @return the result Map (one entry for each column, using the column name as the key).
	 * @throws IncorrectResultSizeDataAccessException if the query does not return exactly one row
	 * @throws DataAccessException if there is any problem executing the query.
	 * @see #queryForMap(String)
	 * @see ColumnMapRowMapper
	 */
	Mono<Map<String, Object>> queryForMap(String cql, Object... args) throws DataAccessException;

	/**
	 * Query given CQL to create a prepared statement from CQL and a list of arguments to bind to the query, expecting a
	 * result {@link Flux}.
	 * <p>
	 * The results will be mapped to a {@link Flux} (one item for each row) of result objects, each of them matching the
	 * specified element type.
	 *
	 * @param cql static CQL to execute, must not be empty or {@literal null}.
	 * @param elementType the required type of element in the result {@link Flux} (for example, {@code Integer.class}),
	 *          must not be {@literal null}.
	 * @param args arguments to bind to the query (leaving it to the {@link PreparedStatement} to guess the corresponding
	 *          CQL type).
	 * @return a {@link Flux} of objects that match the specified element type.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @see #queryForFlux(String, Class)
	 * @see SingleColumnRowMapper
	 */
	<T> Flux<T> queryForFlux(String cql, Class<T> elementType, Object... args) throws DataAccessException;

	/**
	 * Query given CQL to create a prepared statement from CQL and a list of arguments to bind to the query, expecting a
	 * result {@link Flux}.
	 * <p>
	 * The results will be mapped to a {@link Flux} (one item for each row) of {@link Map}s (one entry for each column,
	 * using the column name as the key). Each item in the {@link Flux} will be of the form returned by this interface's
	 * queryForMap() methods.
	 *
	 * @param cql static CQL to execute, must not be empty or {@literal null}.
	 * @param args arguments to bind to the query (leaving it to the {@link PreparedStatement} to guess the corresponding
	 *          CQL type).
	 * @return a {@link Flux} that contains a {@link Map} per row
	 * @throws DataAccessException if there is any problem executing the query.
	 * @see #queryForFlux(String)
	 */
	Flux<Map<String, Object>> queryForFlux(String cql, Object... args) throws DataAccessException;

	/**
	 * Query given CQL to create a prepared statement from CQL and a list of arguments to bind to the query, expecting a
	 * ResultSet.
	 * <p>
	 * The results will be mapped to an {@link ReactiveResultSet}.
	 *
	 * @param cql static CQL to execute, must not be empty or {@literal null}.
	 * @param args arguments to bind to the query (leaving it to the {@link PreparedStatement} to guess the corresponding
	 *          CQL type).
	 * @return a {@link ReactiveResultSet} representation.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @see #queryForResultSet(String)
	 */
	Mono<ReactiveResultSet> queryForResultSet(String cql, Object... args) throws DataAccessException;

	/**
	 * Query given CQL to create a prepared statement from CQL and a list of arguments to bind to the query, expecting
	 * Rows.
	 * <p>
	 * The results will be mapped to {@link Row}s.
	 *
	 * @param cql static CQL to execute, must not be empty or {@literal null}.
	 * @param args arguments to bind to the query (leaving it to the {@link PreparedStatement} to guess the corresponding
	 *          CQL type).
	 * @return a {@link Row} representation.
	 * @throws DataAccessException if there is any problem executing the query.
	 * @see #queryForResultSet(String)
	 */
	Flux<Row> queryForRows(String cql, Object... args) throws DataAccessException;

	/**
	 * Issue a single CQL execute operation (such as an insert, update or delete statement) using a
	 * {@link ReactivePreparedStatementCreator} to provide CQL and any required parameters.
	 *
	 * @param psc object that provides CQL and any necessary parameters, must not be {@literal null}.
	 * @return boolean value whether the statement was applied.
	 * @throws DataAccessException if there is any problem issuing the execution.
	 */
	Mono<Boolean> execute(ReactivePreparedStatementCreator psc) throws DataAccessException;

	/**
	 * Issue an statement using a {@link PreparedStatementBinder} to set bind parameters, with given CQL. Simpler than
	 * using a {@link ReactivePreparedStatementCreator} as this method will create the {@link PreparedStatement}: The
	 * {@link PreparedStatementBinder} just needs to set parameters.
	 *
	 * @param cql static CQL to execute, must not be empty or {@literal null}.
	 * @param psb object that knows how to set values on the prepared statement. If this is {@literal null}, the CQL will
	 *          be assumed to contain no bind parameters. Even if there are no bind parameters, this object may be used to
	 *          set fetch size and other performance options.
	 * @return boolean value whether the statement was applied.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	Mono<Boolean> execute(String cql, @Nullable PreparedStatementBinder psb) throws DataAccessException;

	/**
	 * Issue a single CQL operation (such as an insert, update or delete statement) via a prepared statement, binding the
	 * given arguments.
	 *
	 * @param cql static CQL to execute, must not be empty or {@literal null}.
	 * @param args arguments to bind to the query (leaving it to the {@link PreparedStatement} to guess the corresponding
	 *          CQL type).
	 * @return boolean value whether the statement was applied.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	Mono<Boolean> execute(String cql, Object... args) throws DataAccessException;

	/**
	 * Issue a single CQL operation (such as an insert, update or delete statement) via a prepared statement, binding the
	 * given arguments.
	 *
	 * @param cql static CQL to execute containing bind parameters, must not be empty or {@literal null}.
	 * @param args arguments to bind to the query (leaving it to the {@link PreparedStatement} to guess the corresponding
	 *          CQL type).
	 * @return boolean value whether the statement was applied.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	Flux<Boolean> execute(String cql, Publisher<Object[]> args) throws DataAccessException;
}
