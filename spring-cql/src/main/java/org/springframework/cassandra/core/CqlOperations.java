/*
 * Copyright 2013-2014 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.cassandra.core;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.springframework.cassandra.core.cql.CqlIdentifier;
import org.springframework.cassandra.core.keyspace.AlterKeyspaceSpecification;
import org.springframework.cassandra.core.keyspace.AlterTableSpecification;
import org.springframework.cassandra.core.keyspace.CreateIndexSpecification;
import org.springframework.cassandra.core.keyspace.CreateKeyspaceSpecification;
import org.springframework.cassandra.core.keyspace.CreateTableSpecification;
import org.springframework.cassandra.core.keyspace.DropIndexSpecification;
import org.springframework.cassandra.core.keyspace.DropKeyspaceSpecification;
import org.springframework.cassandra.core.keyspace.DropTableSpecification;
import org.springframework.dao.DataAccessException;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Truncate;
import com.datastax.driver.core.querybuilder.Update;

/**
 * Operations for interacting with Cassandra at the lowest level. This interface provides Exception Translation.
 * 
 * @author David Webb
 * @author Matthew Adams
 */
public interface CqlOperations {

	/**
	 * Convenient method that delegates to {@link ResultSetFuture#getUninterruptibly()} but translates exceptions if any
	 * are thrown.
	 * 
	 * @param rsf The {@link ResultSetFuture} from which to get the {@link ResultSet}.
	 * @return The {@link ResultSet}
	 */
	ResultSet getResultSetUninterruptibly(ResultSetFuture rsf);

	/**
	 * Convenient method that delegates to {@link ResultSetFuture#getUninterruptibly()} but translates exceptions if any
	 * are thrown.
	 * 
	 * @param rsf The {@link ResultSetFuture} from which to get the {@link ResultSet}.
	 * @param timeout The timeout to wait in milliseconds. A nonpositive value means wait indefinitely.
	 * @return The {@link ResultSet}
	 */
	ResultSet getResultSetUninterruptibly(ResultSetFuture rsf, long millis);

	/**
	 * Convenient method that delegates to {@link ResultSetFuture#getUninterruptibly()} but translates exceptions if any
	 * are thrown.
	 * 
	 * @param rsf The {@link ResultSetFuture} from which to get the {@link ResultSet}.
	 * @param timeout The timeout to wait. A nonpositive value means wait indefinitely.
	 * @param unit The {@link TimeUnit} of the timeout.
	 * @return The {@link ResultSet}
	 */
	ResultSet getResultSetUninterruptibly(ResultSetFuture rsf, long timeout, TimeUnit unit);

	/**
	 * Executes the supplied {@link SessionCallback} in the current Template Session. The implementation of
	 * SessionCallback can decide whether or not to <code>execute()</code> or <code>executeAsync()</code> the operation.
	 * 
	 * @param sessionCallback
	 * @return Type<T> defined in the SessionCallback
	 */
	<T> T execute(SessionCallback<T> sessionCallback) throws DataAccessException;

	/**
	 * Executes the supplied CQL Query and returns nothing.
	 * 
	 * @param cql
	 */
	void execute(String cql) throws DataAccessException;

	/**
	 * Executes the supplied CQL Query and returns nothing.
	 * 
	 * @param cql
	 * @param options may be null
	 */
	void execute(String cql, QueryOptions options) throws DataAccessException;

	/**
	 * Executes the supplied Query and returns nothing.
	 * 
	 * @param query The {@link Statement} to execute
	 */
	void execute(Statement query) throws DataAccessException;

	/**
	 * Executes the supplied Delete Query and returns nothing.
	 * 
	 * @param query The {@link Delete} to execute
	 */
	void execute(Delete delete) throws DataAccessException;

	/**
	 * Executes the supplied Insert Query and returns nothing.
	 * 
	 * @param query The {@link Insert} to execute
	 */
	void execute(Insert insert) throws DataAccessException;

	/**
	 * Executes the supplied Update Query and returns nothing.
	 * 
	 * @param query The {@link Update} to execute
	 */
	void execute(Update update) throws DataAccessException;

	/**
	 * Executes the supplied Batch Query and returns nothing.
	 * 
	 * @param query The {@link Batch} to execute
	 */
	void execute(Batch batch) throws DataAccessException;

	/**
	 * Executes the supplied Truncate Query and returns nothing.
	 * 
	 * @param query The {@link Truncate} to execute
	 */
	void execute(Truncate truncate) throws DataAccessException;

	/**
	 * Executes the supplied Query Asynchronously and returns nothing.
	 * 
	 * @param cql The CQL String to execute
	 * @return A {@link ResultSetFuture} that can be used to cancel the query.
	 */
	ResultSetFuture executeAsynchronously(String cql) throws DataAccessException;

	/**
	 * Executes the supplied Query Asynchronously and returns nothing.
	 * 
	 * @param cql The CQL String to execute
	 * @param options The {@link QueryOptions} to use. Only applies to cql statements that can use QueryOptions.
	 * @return A {@link ResultSetFuture} that can be used to cancel the query.
	 */
	ResultSetFuture executeAsynchronously(String cql, QueryOptions options) throws DataAccessException;

	/**
	 * Executes the supplied Query Asynchronously and returns nothing.
	 * 
	 * @param cql The CQL String to execute
	 * @param listener The {@link Runnable} to register with the {@link ResultSetFuture}
	 * @return A {@link Cancellable} that can be used to cancel the query.
	 * @see queryAsyncronously for Reads
	 */
	Cancellable executeAsynchronously(String cql, Runnable listener) throws DataAccessException;

	/**
	 * Executes the supplied Query Asynchronously and returns nothing.
	 * 
	 * @param cql The CQL String to execute
	 * @param listener The {@link Runnable} to register with the {@link ResultSetFuture}
	 * @param executor The {@link Executor} to regsiter with the {@link ResultSetFuture}
	 * @return A {@link Cancellable} that can be used to cancel the query.
	 * @see queryAsyncronously for Reads
	 */
	Cancellable executeAsynchronously(String cql, Runnable listener, Executor executor) throws DataAccessException;

	/**
	 * Executes the supplied Query Asynchronously and returns nothing.
	 * 
	 * @param cql The CQL String to execute
	 * @param listener The {@link AsynchronousQueryListener} to register with the {@link ResultSetFuture}
	 * @return A {@link Cancellable} that can be used to cancel the query.
	 * @see queryAsyncronously for Reads
	 */
	Cancellable executeAsynchronously(String cql, AsynchronousQueryListener listener) throws DataAccessException;

	/**
	 * Executes the supplied Query Asynchronously and returns nothing.
	 * 
	 * @param cql The CQL String to execute
	 * @param listener The {@link AsynchronousQueryListener} to register with the {@link ResultSetFuture}
	 * @param executor The {@link Executor} to regsiter with the {@link ResultSetFuture}
	 * @return A {@link Cancellable} that can be used to cancel the query.
	 * @see queryAsyncronously for Reads
	 */
	Cancellable executeAsynchronously(String cql, AsynchronousQueryListener listener, Executor executor)
			throws DataAccessException;

	/**
	 * Executes the supplied CQL Truncate Asynchronously and returns nothing.
	 * 
	 * @param query The {@link Truncate} to execute
	 * @return A {@link ResultSetFuture} that can be used to cancel the query.
	 */
	ResultSetFuture executeAsynchronously(Truncate truncate) throws DataAccessException;

	/**
	 * Executes the supplied CQL Delete Asynchronously and returns nothing.
	 * 
	 * @param query The {@link Delete} to execute
	 * @return A {@link ResultSetFuture} that can be used to cancel the query.
	 */
	ResultSetFuture executeAsynchronously(Delete delete) throws DataAccessException;

	/**
	 * Executes the supplied CQL Insert Asynchronously and returns nothing.
	 * 
	 * @param query The {@link Insert} to execute
	 * @return A {@link ResultSetFuture} that can be used to cancel the query.
	 */
	ResultSetFuture executeAsynchronously(Insert insert) throws DataAccessException;

	/**
	 * Executes the supplied CQL Update Asynchronously and returns nothing.
	 * 
	 * @param query The {@link Update} to execute
	 * @return A {@link ResultSetFuture} that can be used to cancel the query.
	 */
	ResultSetFuture executeAsynchronously(Update update) throws DataAccessException;

	/**
	 * Executes the supplied CQL Batch Asynchronously and returns nothing.
	 * 
	 * @param query The {@link Batch} to execute
	 * @return A {@link ResultSetFuture} that can be used to cancel the query.
	 */
	ResultSetFuture executeAsynchronously(Batch batch) throws DataAccessException;

	Cancellable executeAsynchronously(Truncate truncate, AsynchronousQueryListener listener) throws DataAccessException;

	Cancellable executeAsynchronously(Delete delete, AsynchronousQueryListener listener) throws DataAccessException;

	Cancellable executeAsynchronously(Insert insert, AsynchronousQueryListener listener) throws DataAccessException;

	Cancellable executeAsynchronously(Update update, AsynchronousQueryListener listener) throws DataAccessException;

	Cancellable executeAsynchronously(Batch batch, AsynchronousQueryListener listener) throws DataAccessException;

	/**
	 * Executes the supplied CQL Query Asynchronously and returns nothing.
	 * 
	 * @param query The {@link Statement} to execute
	 * @return A {@link ResultSetFuture} that can be used to cancel the query.
	 */
	ResultSetFuture executeAsynchronously(Statement query) throws DataAccessException;

	/**
	 * Executes the supplied CQL Query Asynchronously and returns nothing.
	 * 
	 * @param query The {@link Statement} to execute
	 * @return A {@link Cancellable} that can be used to cancel the query.
	 */
	Cancellable executeAsynchronously(Statement query, Runnable runnable) throws DataAccessException;

	/**
	 * Executes the supplied CQL Query Asynchronously and returns nothing.
	 * 
	 * @param query The {@link Statement} to execute
	 * @return A {@link Cancellable} that can be used to cancel the query.
	 */
	Cancellable executeAsynchronously(Statement query, AsynchronousQueryListener listener) throws DataAccessException;

	/**
	 * Executes the supplied CQL Query Asynchronously and returns nothing.
	 * 
	 * @param query The {@link Statement} to execute
	 * @return A {@link Cancellable} that can be used to cancel the query.
	 */
	Cancellable executeAsynchronously(Statement query, Runnable runnable, Executor executor) throws DataAccessException;

	/**
	 * Executes the supplied CQL Query Asynchronously and returns nothing.
	 * 
	 * @param query The {@link Statement} to execute
	 * @return A {@link Cancellable} that can be used to cancel the query.
	 */
	Cancellable executeAsynchronously(Statement query, AsynchronousQueryListener listener, Executor executor)
			throws DataAccessException;

	/**
	 * Executes the provided CQL Query, and extracts the results with the ResultSetExtractor. This uses default Query
	 * Options when extracting the ResultSet.
	 * 
	 * @param cql The Query
	 * @param rse The implementation for extracting the ResultSet
	 * @param timeout Time to wait for results
	 * @param timeUnit Time unit to wait for results
	 * @return
	 */
	<T> T queryAsynchronously(String cql, ResultSetExtractor<T> rse, Long timeout, TimeUnit timeUnit);

	/**
	 * Executes the provided CQL Query, and extracts the results with the ResultSetExtractor.
	 * 
	 * @param cql The Query
	 * @param rse The implementation for extracting the ResultSet
	 * @param timeout Time to wait for results
	 * @param timeUnit Time unit to wait for results
	 * @param options Query Options
	 * @return
	 */
	<T> T queryAsynchronously(String cql, ResultSetExtractor<T> rse, Long timeout, TimeUnit timeUnit, QueryOptions options);

	/**
	 * Executes the provided CQL Query and returns the ResultSetFuture for user processing.
	 * 
	 * @param cql The Query
	 * @return
	 */
	ResultSetFuture queryAsynchronously(String cql);

	/**
	 * Executes the provided CQL Select and returns the ResultSetFuture for user processing.
	 * 
	 * @param cql The {@link Select}
	 * @return
	 */
	ResultSetFuture queryAsynchronously(Select select);

	/**
	 * Executes the provided CQL Query and returns the ResultSetFuture for user processing.
	 * 
	 * @param cql The Query
	 * @param options Query Options
	 * @return
	 */
	ResultSetFuture queryAsynchronously(String cql, QueryOptions options);

	/**
	 * Executes the provided CQL Query with the provided {@link Runnable}, which is started after the query has completed.
	 * <p/>
	 * A more useful method than this one is {@link #queryAsynchronously(String, AsynchronousQueryListener)}, where you're
	 * given the {@link ResultSetFuture} after the query has been executed.
	 * 
	 * @param cql The Query
	 * @param listener {@link Runnable} listener for handling the query in a separate thread
	 * @return A {@link Cancellable} that can be used to cancel the query.
	 * @see #queryAsynchronously(String, AsynchronousQueryListener)
	 */
	Cancellable queryAsynchronously(String cql, Runnable listener);

	/**
	 * Executes the provided CQL Select with the provided {@link Runnable}, which is started after the query has
	 * completed.
	 * <p/>
	 * A more useful method than this one is {@link #queryAsynchronously(Select, AsynchronousQueryListener)}, where you're
	 * given the {@link ResultSetFuture} after the query has been executed.
	 * 
	 * @param select The Select Query
	 * @param listener {@link Runnable} listener for handling the query in a separate thread
	 * @return A {@link Cancellable} that can be used to cancel the query.
	 * @see #queryAsynchronously(Select, AsynchronousQueryListener)
	 */
	Cancellable queryAsynchronously(Select select, Runnable listener);

	/**
	 * Executes the provided CQL Query with the provided listener. This is preferred over the same method that takes a
	 * {@link Runnable}. The {@link AsynchronousQueryListener} gives you access to the {@link ResultSetFuture} once the
	 * query is completed for optimal flexibility.
	 * 
	 * @param cql The Query
	 * @param listener {@link AsynchronousQueryListener} for handling the query's {@link ResultSetFuture} in a separate
	 *          thread
	 * @return A {@link Cancellable} that can be used to cancel the query.
	 */
	Cancellable queryAsynchronously(String cql, AsynchronousQueryListener listener);

	/**
	 * Executes the provided CQL Select with the provided listener. This is preferred over the same method that takes a
	 * {@link Runnable}. The {@link AsynchronousQueryListener} gives you access to the {@link ResultSetFuture} once the
	 * query is completed for optimal flexibility.
	 * 
	 * @param select The Select
	 * @param listener {@link AsynchronousQueryListener} for handling the query's {@link ResultSetFuture} in a separate
	 *          thread
	 * @return A {@link Cancellable} that can be used to cancel the query.
	 */
	Cancellable queryAsynchronously(Select select, AsynchronousQueryListener listener);

	/**
	 * Executes the provided CQL Query with the Runnable implementations using the Query Options.
	 * 
	 * @param cql The Query
	 * @param options Query Option
	 * @param listener Runnable Listener for handling the query in a separate thread
	 * @return A {@link Cancellable} that can be used to cancel the query.
	 */
	Cancellable queryAsynchronously(String cql, Runnable listener, QueryOptions options);

	/**
	 * Executes the provided CQL Query with the provided Listener and Query Options. This is preferred over the same
	 * method that takes a plain Runnable. The {@link AsynchronousQueryListener} gives you access to the
	 * {@link ResultSetFuture} once the query is completed for optimal flexibility.
	 * 
	 * @param cql The Query
	 * @param options Query Option
	 * @param listener Runnable Listener for handling the query in a separate thread
	 * @return A {@link Cancellable} that can be used to cancel the query.
	 */
	Cancellable queryAsynchronously(String cql, AsynchronousQueryListener listener, QueryOptions options);

	/**
	 * Executes the provided CQL Query with the provided Executor and Runnable implementations.
	 * 
	 * @param cql The Query
	 * @param listener Runnable Listener for handling the query in a separate thread
	 * @param executor To execute the Runnable Listener
	 * @return A {@link Cancellable} that can be used to cancel the query.
	 */
	Cancellable queryAsynchronously(String cql, Runnable listener, Executor executor);

	/**
	 * Executes the provided CQL Select with the provided Executor and Runnable implementations.
	 * 
	 * @param select The Select Query
	 * @param listener Runnable Listener for handling the query in a separate thread
	 * @param executor To execute the Runnable Listener
	 * @return A {@link Cancellable} that can be used to cancel the query.
	 */
	Cancellable queryAsynchronously(Select select, Runnable listener, Executor executor);

	/**
	 * Executes the provided CQL Query with the provided listener and executor. This is preferred over the same method
	 * that takes a plain Runnable. The {@link AsynchronousQueryListener} gives you access to the {@link ResultSetFuture}
	 * once the query is completed for optimal flexibility.
	 * 
	 * @param cql The Query
	 * @param options Query Option
	 * @param listener Runnable Listener for handling the query in a separate thread
	 * @param executor To execute the Runnable Listener
	 * @return A {@link Cancellable} that can be used to cancel the query.
	 */
	Cancellable queryAsynchronously(String cql, AsynchronousQueryListener listener, Executor executor);

	/**
	 * Executes the provided Select Query with the provided listener and executor. This is preferred over the same method
	 * that takes a plain Runnable. The {@link AsynchronousQueryListener} gives you access to the {@link ResultSetFuture}
	 * once the query is completed for optimal flexibility.
	 * 
	 * @param select The Select Query
	 * @param listener Runnable Listener for handling the query in a separate thread
	 * @param executor To execute the Runnable Listener
	 * @return A {@link Cancellable} that can be used to cancel the query.
	 */
	Cancellable queryAsynchronously(Select select, AsynchronousQueryListener listener, Executor executor);

	/**
	 * Executes the provided CQL Query with the provided Executor and Runnable implementations.
	 * 
	 * @param cql The Query
	 * @param options Query Option
	 * @param listener Runnable Listener for handling the query in a separate thread
	 * @param executor To execute the Runnable Listener
	 * @return A {@link Cancellable} that can be used to cancel the query.
	 */
	Cancellable queryAsynchronously(String cql, Runnable listener, QueryOptions options, Executor executor);

	/**
	 * Executes the provided CQL Query with the provided Listener, Executor and Query Options. This is preferred over the
	 * same method that takes a plain Runnable. The {@link AsynchronousQueryListener} gives you access to the
	 * {@link ResultSetFuture} once the query is completed for optimal flexibility.
	 * 
	 * @param cql
	 * @param listener
	 * @param options
	 * @param executor
	 * @return A {@link Cancellable} that can be used to cancel the query.
	 */
	Cancellable queryAsynchronously(String cql, AsynchronousQueryListener listener, QueryOptions options,
			Executor executor);

	/**
	 * Executes the provided CQL query and returns the {@link ResultSet}.
	 * 
	 * @param cql The query
	 * @return The {@link ResultSet}
	 */
	ResultSet query(String cql);

	/**
	 * Executes the provided Select query and returns the {@link ResultSet}.
	 * 
	 * @param select The Select Query
	 * @return The {@link ResultSet}
	 */
	ResultSet query(Select select);

	/**
	 * Executes the provided CQL query with the given {@link QueryOptions} and returns the {@link ResultSet}.
	 * 
	 * @param cql The query
	 * @param options The {@link QueryOptions}; may be null.
	 * @return The {@link ResultSet}
	 */
	ResultSet query(String cql, QueryOptions options);

	/**
	 * Executes the provided CQL Query, and extracts the results with the ResultSetExtractor.
	 * 
	 * @param cql The Query
	 * @param rse The implementation for extracting the ResultSet
	 * @return Type <T> specified in the ResultSetExtractor
	 * @throws DataAccessException
	 */
	<T> T query(String cql, ResultSetExtractor<T> rse) throws DataAccessException;

	/**
	 * Executes the provided Select Query, and extracts the results with the ResultSetExtractor.
	 * 
	 * @param select The SelectQuery
	 * @param rse The implementation for extracting the ResultSet
	 * @return Type <T> specified in the ResultSetExtractor
	 * @throws DataAccessException
	 */
	<T> T query(Select select, ResultSetExtractor<T> rse) throws DataAccessException;

	/**
	 * Executes the provided CQL Query, and extracts the results with the ResultSetExtractor.
	 * 
	 * @param cql The Query
	 * @param rse The implementation for extracting the ResultSet
	 * @param options Query Options
	 * @return
	 * @throws DataAccessException
	 */
	<T> T query(String cql, ResultSetExtractor<T> rse, QueryOptions options) throws DataAccessException;

	/**
	 * Executes the provided CQL Query, and then processes the results with the <code>RowCallbackHandler</code>.
	 * 
	 * @param cql The Query
	 * @param rch The implementation for processing the rows returned.
	 * @throws DataAccessException
	 */
	void query(String cql, RowCallbackHandler rch) throws DataAccessException;

	/**
	 * Executes the provided Select Query, and then processes the results with the <code>RowCallbackHandler</code>.
	 * 
	 * @param select The Select Query
	 * @param rch The implementation for processing the rows returned.
	 * @throws DataAccessException
	 */
	void query(Select select, RowCallbackHandler rch) throws DataAccessException;

	/**
	 * Executes the provided CQL Query, and then processes the results with the <code>RowCallbackHandler</code>.
	 * 
	 * @param cql The Query
	 * @param rch The implementation for processing the rows returned.
	 * @param options Query Options Object
	 * @throws DataAccessException
	 */
	void query(String cql, RowCallbackHandler rch, QueryOptions options) throws DataAccessException;

	/**
	 * Processes the ResultSet through the RowCallbackHandler and return nothing. This is used internal to the Template
	 * for core operations, but is made available through Operations in the event you have a ResultSet to process. The
	 * ResultsSet could come from a ResultSetFuture after an asynchronous query.
	 * 
	 * @param resultSet Results to process
	 * @param rch RowCallbackHandler with the processing implementation
	 * @throws DataAccessException
	 */
	void process(ResultSet resultSet, RowCallbackHandler rch) throws DataAccessException;

	/**
	 * Executes the provided CQL Query, and maps all Rows returned with the supplied RowMapper.
	 * 
	 * @param cql The Query
	 * @param rowMapper The implementation for mapping all rows
	 * @return List of <T> processed by the RowMapper
	 * @throws DataAccessException
	 */
	<T> List<T> query(String cql, RowMapper<T> rowMapper) throws DataAccessException;

	/**
	 * Executes the provided Select Query, and maps all Rows returned with the supplied RowMapper.
	 * 
	 * @param select The Select Query
	 * @param rowMapper The implementation for mapping all rows
	 * @return List of <T> processed by the RowMapper
	 * @throws DataAccessException
	 */
	<T> List<T> query(Select select, RowMapper<T> rowMapper) throws DataAccessException;

	/**
	 * Executes the provided CQL Query, and maps all Rows returned with the supplied RowMapper.
	 * 
	 * @param cql The Query
	 * @param rowMapper The implementation for mapping all rows
	 * @param options Query Options Object
	 * @return List of <T> processed by the RowMapper
	 * @throws DataAccessException
	 */
	<T> List<T> query(String cql, RowMapper<T> rowMapper, QueryOptions options) throws DataAccessException;

	/**
	 * Processes the ResultSet through the RowMapper and returns the List of mapped Rows. This is used internal to the
	 * Template for core operations, but is made available through Operations in the event you have a ResultSet to
	 * process. The ResultsSet could come from a ResultSetFuture after an asynchronous query.
	 * 
	 * @param resultSet Results to process
	 * @param rowMapper RowMapper with the processing implementation
	 * @return List of <T> generated by the RowMapper
	 * @throws DataAccessException
	 */
	<T> List<T> process(ResultSet resultSet, RowMapper<T> rowMapper) throws DataAccessException;

	/**
	 * Executes the provided string CQL query, and maps the first row returned with the supplied {@link RowMapper}.
	 * 
	 * @param cql The string query CQL.
	 * @param rowMapper The {@link RowMapper} to convert the row into an object of type <code>T</code>.
	 * @param listener The listener that receives the results upon completion.
	 * @return A {@link Cancellable} that can be used to cancel the query.
	 * @throws DataAccessException
	 */
	<T> Cancellable queryForObjectAsynchronously(String cql, RowMapper<T> rowMapper, QueryForObjectListener<T> listener)
			throws DataAccessException;

	/**
	 * Executes the provided string CQL query, and maps the first row returned with the supplied {@link RowMapper}.
	 * 
	 * @param cql The string query CQL.
	 * @param rowMapper The {@link RowMapper} to convert the row into an object of type <code>T</code>.
	 * @param listener The listener that receives the results upon completion.
	 * @param options The {@link QueryOptions} to use. May be <code>null</code>.
	 * @return A {@link Cancellable} that can be used to cancel the query.
	 * @throws DataAccessException
	 */
	<T> Cancellable queryForObjectAsynchronously(String cql, RowMapper<T> rowMapper, QueryForObjectListener<T> listener,
			QueryOptions options) throws DataAccessException;

	/**
	 * Executes the provided {@link Select} query, and maps the first row returned with the supplied {@link RowMapper}.
	 * 
	 * @param select The {@link Select} query to execute.
	 * @param rowMapper The {@link RowMapper} to convert the row into an object of type <code>T</code>.
	 * @param listener The listener that receives the results upon completion.
	 * @return A {@link Cancellable} that can be used to cancel the query.
	 * @throws DataAccessException
	 */
	<T> Cancellable queryForObjectAsynchronously(Select select, RowMapper<T> rowMapper, QueryForObjectListener<T> listener)
			throws DataAccessException;

	/**
	 * Executes the provided CQL Query, and maps <b>ONE</b> Row returned with the supplied RowMapper.
	 * <p>
	 * This expects only ONE row to be returned. More than one Row will cause an Exception to be thrown.
	 * </p>
	 * 
	 * @param cql The Query
	 * @param rowMapper The implementation for convert the Row to <T>
	 * @return Object<T>
	 * @throws DataAccessException
	 */
	<T> T queryForObject(String cql, RowMapper<T> rowMapper) throws DataAccessException;

	/**
	 * Executes the provided Select Query, and maps <b>ONE</b> Row returned with the supplied RowMapper.
	 * <p>
	 * This expects only ONE row to be returned. More than one Row will cause an Exception to be thrown.
	 * </p>
	 * 
	 * @param select The Select Query
	 * @param rowMapper The implementation for convert the Row to <T>
	 * @return Object<T>
	 * @throws DataAccessException
	 */
	<T> T queryForObject(Select select, RowMapper<T> rowMapper) throws DataAccessException;

	/**
	 * Process a ResultSet through a RowMapper. This is used internal to the Template for core operations, but is made
	 * available through Operations in the event you have a ResultSet to process. The ResultsSet could come from a
	 * ResultSetFuture after an asynchronous query.
	 * 
	 * @param resultSet
	 * @param rowMapper
	 * @return
	 * @throws DataAccessException
	 */
	<T> T processOne(ResultSet resultSet, RowMapper<T> rowMapper) throws DataAccessException;

	/**
	 * Executes the provided query and tries to return the first column of the first Row as a Class<T>.
	 * 
	 * @param cql The Query
	 * @param requiredType Valid Class that Cassandra Data Types can be converted to.
	 * @return The Object<T> - item [0,0] in the result table of the query.
	 * @throws DataAccessException
	 */
	<T> T queryForObject(String cql, Class<T> requiredType) throws DataAccessException;

	/**
	 * Executes the provided {@link Select} query and returns the first column of the first Row as an object of type
	 * <code>T</code>.
	 * 
	 * @param select The {@link Select} query
	 * @param requiredType Type that Cassandra data types can be converted to.
	 * @return A {@link Cancellable} that can be used to cancel the query if necessary.
	 * @throws DataAccessException
	 */
	<T> Cancellable queryForObjectAsynchronously(Select select, Class<T> requiredType, QueryForObjectListener<T> listener)
			throws DataAccessException;

	/**
	 * Executes the provided select CQL query and returns the first column of the first Row as an object of type
	 * <code>T</code>.
	 * 
	 * @param cql The select query CQL. Must not be <code>null</code> or blank.
	 * @param requiredType The type to convert the first column of the first row to. Must not be <code>null</code>.
	 * @return A {@link Cancellable} that can be used to cancel the query if necessary. Must not be <code>null</code>.
	 * @throws DataAccessException
	 */
	<T> Cancellable queryForObjectAsynchronously(String cql, Class<T> requiredType, QueryForObjectListener<T> listener)
			throws DataAccessException;

	/**
	 * Executes the provided select CQL query and returns the first column of the first Row as an object of type
	 * <code>T</code>.
	 * 
	 * @param cql The select query CQL. Must not be <code>null</code> or blank.
	 * @param requiredType The type to convert the first column of the first row to. Must not be <code>null</code>.
	 * @param options The {@link QueryOptions} to use. May be <code>null</code>.
	 * @return A {@link Cancellable} that can be used to cancel the query if necessary. Must not be <code>null</code>.
	 * @throws DataAccessException
	 */
	<T> Cancellable queryForObjectAsynchronously(String cql, Class<T> requiredType, QueryForObjectListener<T> listener,
			QueryOptions options) throws DataAccessException;

	/**
	 * Executes the provided Select query and tries to return the first column of the first Row as a Class<T>.
	 * 
	 * @param select The Select Query
	 * @param requiredType Valid Class that Cassandra Data Types can be converted to.
	 * @return The Object<T> - item [0,0] in the result table of the query.
	 * @throws DataAccessException
	 */
	<T> T queryForObject(Select select, Class<T> requiredType) throws DataAccessException;

	/**
	 * Process a ResultSet, trying to convert the first columns of the first Row to Class<T>. This is used internal to the
	 * Template for core operations, but is made available through Operations in the event you have a ResultSet to
	 * process. The ResultsSet could come from a ResultSetFuture after an asynchronous query.
	 * 
	 * @param resultSet
	 * @param requiredType
	 * @return
	 * @throws DataAccessException
	 */
	<T> T processOne(ResultSet resultSet, Class<T> requiredType) throws DataAccessException;

	/**
	 * Executes the provided CQL Query and maps <b>ONE</b> Row to a basic Map of Strings and Objects. If more than one Row
	 * is returned from the Query, an exception will be thrown.
	 * 
	 * @param cql The Query
	 * @return Map representing the results of the Query
	 * @throws DataAccessException
	 */
	Map<String, Object> queryForMap(String cql) throws DataAccessException;

	/**
	 * Executes the provided Select Query and maps <b>ONE</b> Row to a basic Map of Strings and Objects. If more than one
	 * Row is returned from the Query, an exception will be thrown.
	 * 
	 * @param select The Select Query
	 * @return Map representing the results of the Query
	 * @throws DataAccessException
	 */
	Map<String, Object> queryForMap(Select select) throws DataAccessException;

	/**
	 * Executes the provided CQL query asynchronously and maps the first row to a {@link Map}&lt;String,Object&gt;.
	 * Additional rows are ignored.
	 * 
	 * @param cql The select query CQL. Must not be <code>null</code> or blank.
	 * @param listener The {@link QueryForMapListener} that will recieve the results upon query completion. Must not be
	 *          <code>null</code>.
	 * @return A {@link Cancellable} that can be used to cancel the query if necessary. Must not be <code>null</code>.
	 * @throws DataAccessException
	 */
	Cancellable queryForMapAsynchronously(String cql, QueryForMapListener listener) throws DataAccessException;

	/**
	 * Executes the provided CQL query asynchronously and maps the first row to a {@link Map}&lt;String,Object&gt;.
	 * Additional rows are ignored.
	 * 
	 * @param cql The select query CQL. Must not be <code>null</code> or blank.
	 * @param listener The {@link QueryForMapListener} that will recieve the results upon query completion. Must not be
	 *          <code>null</code>.
	 * @param options The {@link QueryOptions} to use. May be <code>null</code>.
	 * @return A {@link Cancellable} that can be used to cancel the query if necessary. Must not be <code>null</code>.
	 * @throws DataAccessException
	 */
	Cancellable queryForMapAsynchronously(String cql, QueryForMapListener listener, QueryOptions options)
			throws DataAccessException;

	/**
	 * Executes the provided {@link Select} query asynchronously and maps the first row to a {@link Map}
	 * &lt;String,Object&gt;. Additional rows are ignored.
	 * 
	 * @param cql The select query CQL. Must not be <code>null</code> or blank.
	 * @param listener The {@link QueryForMapListener} that will recieve the results upon query completion. Must not be
	 *          <code>null</code>.
	 * @return A {@link Cancellable} that can be used to cancel the query if necessary. Must not be <code>null</code>.
	 * @throws DataAccessException
	 */
	Cancellable queryForMapAsynchronously(Select select, QueryForMapListener listener) throws DataAccessException;

	/**
	 * Process a ResultSet with <b>ONE</b> Row and convert to a Map. This is used internal to the Template for core
	 * operations, but is made available through Operations in the event you have a ResultSet to process. The ResultsSet
	 * could come from a ResultSetFuture after an asynchronous query.
	 * 
	 * @param resultSet
	 * @return
	 * @throws DataAccessException
	 */
	Map<String, Object> processMap(ResultSet resultSet) throws DataAccessException;

	/**
	 * Executes the provided CQL and returns all values in the first column of the Results as a List of the Type in the
	 * second argument.
	 * 
	 * @param cql The Query
	 * @param elementType Type to cast the data values to
	 * @return List of elementType
	 * @throws DataAccessException
	 */
	<T> List<T> queryForList(String cql, Class<T> elementType) throws DataAccessException;

	/**
	 * Executes the provided Select Query and returns all values in the first column of the Results as a List of the Type
	 * in the second argument.
	 * 
	 * @param select The Select Query
	 * @param elementType Type to cast the data values to
	 * @return List of elementType
	 * @throws DataAccessException
	 */
	<T> List<T> queryForList(Select select, Class<T> elementType) throws DataAccessException;

	/**
	 * Executes the provided {@link Select} query asynchronously and returns all values in the first column of the results
	 * as a {@link List} of the type in the second argument.
	 * 
	 * @param select The {@link Select} query
	 * @param elementType The type to cast the data values to
	 * @param listener The listener to receive the results asynchronously. Must not be <code>null</code>.
	 * @return {@link Cancellable} to cancel the query if necessary
	 * @throws DataAccessException
	 */
	<T> Cancellable queryForListAsynchronously(Select select, Class<T> elementType, QueryForListListener<T> listener)
			throws DataAccessException;

	/**
	 * Executes the provided {@link Select} query asynchronously and returns all values in the first column of the results
	 * as a {@link List} of the type in the second argument.
	 * 
	 * @param select The select query CQL
	 * @param elementType The type to cast the data values to
	 * @param listener The listener to receive the results asynchronously. Must not be <code>null</code>.
	 * @return {@link Cancellable} to cancel the query if necessary
	 * @throws DataAccessException
	 */
	<T> Cancellable queryForListAsynchronously(String select, Class<T> elementType, QueryForListListener<T> listener)
			throws DataAccessException;

	/**
	 * Process a ResultSet and convert the first column of the results to a List. This is used internal to the Template
	 * for core operations, but is made available through Operations in the event you have a ResultSet to process. The
	 * ResultsSet could come from a ResultSetFuture after an asynchronous query.
	 * 
	 * @param resultSet
	 * @param elementType
	 * @return
	 * @throws DataAccessException
	 */
	<T> List<T> processList(ResultSet resultSet, Class<T> elementType) throws DataAccessException;

	/**
	 * Executes the provided CQL and converts the results to a basic List of Maps. Each element in the List represents a
	 * Row returned from the Query. Each Row's columns are put into the map as column/value.
	 * 
	 * @param cql The Query
	 * @return List of Maps with the query results
	 * @throws DataAccessException
	 */
	List<Map<String, Object>> queryForListOfMap(String cql) throws DataAccessException;

	/**
	 * Executes the provided {@link Select} query and converts the results to a {@link List} of {@link Map}s. Each element
	 * in the {@link List} represents a row returned from the query. Each row's column(s) are put into a {@link Map} as
	 * values keyed by column name.
	 * 
	 * @param select The {@link Select} query. Must not be <code>null</code>.
	 * @param listener The listener that will receive the results upon query completion. Must not be <code>null</code>.
	 * @return A {@link Cancellable} that can be used to cancel the query. Must not be <code>null</code>.
	 * @throws DataAccessException
	 */
	Cancellable queryForListOfMapAsynchronously(Select select, QueryForListListener<Map<String, Object>> listener)
			throws DataAccessException;

	/**
	 * Executes the provided select CQL query and converts the results to a {@link List} of {@link Map}s. Each element in
	 * the {@link List} represents a row returned from the query. Each row's column(s) are put into a {@link Map} as
	 * values keyed by column name.
	 * 
	 * @param select The select query CQL. Must not be <code>null</code> or blank.
	 * @param listener The listener that will receive the results upon query completion. Must not be <code>null</code>.
	 * @return A {@link Cancellable} that can be used to cancel the query. Must not be <code>null</code>.
	 * @throws DataAccessException
	 */
	Cancellable queryForListOfMapAsynchronously(String cql, QueryForListListener<Map<String, Object>> listener)
			throws DataAccessException;

	/**
	 * Executes the provided select CQL query and converts the results to a {@link List} of {@link Map}s. Each element in
	 * the {@link List} represents a row returned from the query. Each row's column(s) are put into a {@link Map} as
	 * values keyed by column name.
	 * 
	 * @param select The select query CQL. Must not be <code>null</code> or blank.
	 * @param listener The listener that will receive the results upon query completion. Must not be <code>null</code>.
	 * @param options The {@link QueryOptions} to use. May be <code>null</code>.
	 * @return A {@link Cancellable} that can be used to cancel the query. Must not be <code>null</code>.
	 * @throws DataAccessException
	 */
	Cancellable queryForListOfMapAsynchronously(String cql, QueryForListListener<Map<String, Object>> listener,
			QueryOptions options) throws DataAccessException;

	/**
	 * Executes the provided Select Query and converts the results to a basic List of Maps. Each element in the List
	 * represents a Row returned from the Query. Each Row's columns are put into the map as column/value.
	 * 
	 * @param select The Select Query
	 * @return List of Maps with the query results
	 * @throws DataAccessException
	 */
	List<Map<String, Object>> queryForListOfMap(Select select) throws DataAccessException;

	/**
	 * Process a ResultSet and convert it to a List of Maps with column/value. This is used internal to the Template for
	 * core operations, but is made available through Operations in the event you have a ResultSet to process. The
	 * ResultsSet could come from a ResultSetFuture after an asynchronous query.
	 * 
	 * @param resultSet
	 * @return
	 * @throws DataAccessException
	 */
	List<Map<String, Object>> processListOfMap(ResultSet resultSet) throws DataAccessException;

	/**
	 * Creates and caches a {@link PreparedStatement} from the given CQL, invokes the {@link PreparedStatementCallback}
	 * with that {@link PreparedStatement}, then returns the value returned by the {@link PreparedStatementCallback}.
	 * 
	 * @param cql The CQL statement from which to create and cache a {@link PreparedStatement}
	 * @param action The callback that is given the {@link PreparedStatement}
	 * @return The value returned by the given {@link PreparedStatementCallback}
	 * @throws DataAccessException
	 */
	<T> T execute(String cql, PreparedStatementCallback<T> action) throws DataAccessException;

	/**
	 * Uses the provided {@link PreparedStatementCreator} to create a {@link PreparedStatement} in the current
	 * {@link Session}, then passes that {@link PreparedStatement} to the given {@link PreparedStatementCallback}.
	 * 
	 * @param psc The {@link PreparedStatementCreator}
	 * @param action The callback that receives the {@link PreparedStatement}
	 * @return The value returned by the given {@link PreparedStatementCallback}
	 * @throws DataAccessException
	 */
	<T> T execute(PreparedStatementCreator psc, PreparedStatementCallback<T> action) throws DataAccessException;

	/**
	 * Converts the CQL provided into a {@link CachedPreparedStatementCreator}. Then, the PreparedStatementBinder will
	 * bind its values to the bind variables in the provided CQL String. The results of the PreparedStatement are
	 * processed with the ResultSetExtractor implementation provided by the Application Code. The can return any object,
	 * including a List of Objects to support the ResultSet processing.
	 * 
	 * @param cql The Query to Prepare
	 * @param psb The Binding implementation
	 * @param rse The implementation for extracting the results of the query.
	 * @return Type<T> generated by the ResultSetExtractor
	 * @throws DataAccessException
	 */
	<T> T query(String cql, PreparedStatementBinder psb, ResultSetExtractor<T> rse) throws DataAccessException;

	/**
	 * Converts the CQL provided into a {@link CachedPreparedStatementCreator}. Then, the PreparedStatementBinder will
	 * bind its values to the bind variables in the provided CQL String. The results of the PreparedStatement are
	 * processed with the ResultSetExtractor implementation provided by the Application Code. The can return any object,
	 * including a List of Objects to support the ResultSet processing.
	 * 
	 * @param cql The Query to Prepare
	 * @param psb The Binding implementation
	 * @param rse The implementation for extracting the results of the query.
	 * @param options The Query Options to apply to the PreparedStatement
	 * @return Type<T> generated by the ResultSetExtractor
	 * @throws DataAccessException
	 */
	<T> T query(String cql, PreparedStatementBinder psb, ResultSetExtractor<T> rse, QueryOptions options)
			throws DataAccessException;

	/**
	 * Converts the CQL provided into a {@link CachedPreparedStatementCreator}. Then, the PreparedStatementBinder will
	 * bind its values to the bind variables in the provided CQL String. The results of the PreparedStatement are
	 * processed with the RowCallbackHandler implementation provided and nothing is returned.
	 * 
	 * @param cql The Query to Prepare
	 * @param psb The Binding implementation
	 * @param rch The RowCallbackHandler for processing the ResultSet
	 * @throws DataAccessException
	 */
	void query(String cql, PreparedStatementBinder psb, RowCallbackHandler rch) throws DataAccessException;

	/**
	 * Converts the CQL provided into a {@link CachedPreparedStatementCreator}. Then, the PreparedStatementBinder will
	 * bind its values to the bind variables in the provided CQL String. The results of the PreparedStatement are
	 * processed with the RowCallbackHandler implementation provided and nothing is returned.
	 * 
	 * @param cql The Query to Prepare
	 * @param psb The Binding implementation
	 * @param rch The RowCallbackHandler for processing the ResultSet
	 * @param options The Query Options Object
	 * @throws DataAccessException
	 */
	void query(String cql, PreparedStatementBinder psb, RowCallbackHandler rch, QueryOptions options)
			throws DataAccessException;

	/**
	 * Converts the CQL provided into a {@link CachedPreparedStatementCreator}. Then, the PreparedStatementBinder will
	 * bind its values to the bind variables in the provided CQL String. The results of the PreparedStatement are
	 * processed with the RowMapper implementation provided and a List is returned with elements of Type <T> for each Row
	 * returned.
	 * 
	 * @param cql The Query to Prepare
	 * @param psb The Binding implementation
	 * @param rowMapper The implementation for Mapping a Row to Type <T>
	 * @return List of <T> for each Row returned from the Query.
	 * @throws DataAccessException
	 */
	<T> List<T> query(String cql, PreparedStatementBinder psb, RowMapper<T> rowMapper) throws DataAccessException;

	/**
	 * Converts the CQL provided into a {@link CachedPreparedStatementCreator}. Then, the PreparedStatementBinder will
	 * bind its values to the bind variables in the provided CQL String. The results of the PreparedStatement are
	 * processed with the RowMapper implementation provided and a List is returned with elements of Type <T> for each Row
	 * returned.
	 * 
	 * @param cql The Query to Prepare
	 * @param psb The Binding implementation
	 * @param rowMapper The implementation for Mapping a Row to Type <T>
	 * @param options The Query Options Object
	 * @return List of <T> for each Row returned from the Query.
	 * @throws DataAccessException
	 */
	<T> List<T> query(String cql, PreparedStatementBinder psb, RowMapper<T> rowMapper, QueryOptions options)
			throws DataAccessException;

	/**
	 * Uses the provided PreparedStatementCreator to prepare a new Session call. <b>This can only be used for CQL
	 * Statements that do not have data binding.</b> The results of the PreparedStatement are processed with
	 * ResultSetExtractor implementation provided by the Application Code.
	 * 
	 * @param psc The implementation to create the PreparedStatement
	 * @param rse Implementation for extracting from the ResultSet
	 * @return Type <T> which is the output of the ResultSetExtractor
	 * @throws DataAccessException
	 */
	<T> T query(PreparedStatementCreator psc, ResultSetExtractor<T> rse) throws DataAccessException;

	/**
	 * Uses the provided PreparedStatementCreator to prepare a new Session call. <b>This can only be used for CQL
	 * Statements that do not have data binding.</b> The results of the PreparedStatement are processed with
	 * ResultSetExtractor implementation provided by the Application Code.
	 * 
	 * @param psc The implementation to create the PreparedStatement
	 * @param rse Implementation for extracting from the ResultSet
	 * @param options The Query Options Object
	 * @return Type <T> which is the output of the ResultSetExtractor
	 * @throws DataAccessException
	 */
	<T> T query(PreparedStatementCreator psc, ResultSetExtractor<T> rse, QueryOptions options) throws DataAccessException;

	/**
	 * Uses the provided PreparedStatementCreator to prepare a new Session call. <b>This can only be used for CQL
	 * Statements that do not have data binding.</b> The results of the PreparedStatement are processed with
	 * RowCallbackHandler and nothing is returned.
	 * 
	 * @param psc The implementation to create the PreparedStatement
	 * @param rch The implementation to process Results
	 * @throws DataAccessException
	 */
	void query(PreparedStatementCreator psc, RowCallbackHandler rch) throws DataAccessException;

	/**
	 * Uses the provided PreparedStatementCreator to prepare a new Session call. <b>This can only be used for CQL
	 * Statements that do not have data binding.</b> The results of the PreparedStatement are processed with
	 * RowCallbackHandler and nothing is returned.
	 * 
	 * @param psc The implementation to create the PreparedStatement
	 * @param rch The implementation to process Results
	 * @param options The Query Options Object
	 * @throws DataAccessException
	 */
	void query(PreparedStatementCreator psc, RowCallbackHandler rch, QueryOptions options) throws DataAccessException;

	/**
	 * Uses the provided PreparedStatementCreator to prepare a new Session call. <b>This can only be used for CQL
	 * Statements that do not have data binding.</b> The results of the PreparedStatement are processed with RowMapper
	 * implementation provided and a List is returned with elements of Type <T> for each Row returned.
	 * 
	 * @param psc The implementation to create the PreparedStatement
	 * @param rowMapper The implementation for mapping each Row returned.
	 * @return List of Type <T> mapped from each Row in the Results
	 * @throws DataAccessException
	 */
	<T> List<T> query(PreparedStatementCreator psc, RowMapper<T> rowMapper) throws DataAccessException;

	/**
	 * Uses the provided PreparedStatementCreator to prepare a new Session call. <b>This can only be used for CQL
	 * Statements that do not have data binding.</b> The results of the PreparedStatement are processed with RowMapper
	 * implementation provided and a List is returned with elements of Type <T> for each Row returned.
	 * 
	 * @param psc The implementation to create the PreparedStatement
	 * @param rowMapper The implementation for mapping each Row returned.
	 * @param options The Query Options Object
	 * @return List of Type <T> mapped from each Row in the Results
	 * @throws DataAccessException
	 */
	<T> List<T> query(PreparedStatementCreator psc, RowMapper<T> rowMapper, QueryOptions options)
			throws DataAccessException;

	/**
	 * Uses the provided PreparedStatementCreator to prepare a new Session call. Binds the values from the
	 * PreparedStatementBinder to the available bind variables. The results of the PreparedStatement are processed with
	 * ResultSetExtractor implementation provided by the Application Code.
	 * 
	 * @param psc The implementation to create the PreparedStatement
	 * @param psb The implementation to bind variables to values
	 * @param rse Implementation for extracting from the ResultSet
	 * @param options The Query Options Object
	 * @return Type <T> which is the output of the ResultSetExtractor
	 * @throws DataAccessException
	 */
	<T> T query(PreparedStatementCreator psc, PreparedStatementBinder psb, ResultSetExtractor<T> rse, QueryOptions options)
			throws DataAccessException;

	/**
	 * Uses the provided PreparedStatementCreator to prepare a new Session call. Binds the values from the
	 * PreparedStatementBinder to the available bind variables. The results of the PreparedStatement are processed with
	 * ResultSetExtractor implementation provided by the Application Code.
	 * 
	 * @param psc The implementation to create the PreparedStatement
	 * @param psb The implementation to bind variables to values
	 * @param rse Implementation for extracting from the ResultSet
	 * @return Type <T> which is the output of the ResultSetExtractor
	 * @throws DataAccessException
	 */
	<T> T query(PreparedStatementCreator psc, PreparedStatementBinder psb, ResultSetExtractor<T> rse)
			throws DataAccessException;

	/**
	 * Uses the provided PreparedStatementCreator to prepare a new Session call. Binds the values from the
	 * PreparedStatementBinder to the available bind variables. The results of the PreparedStatement are processed with
	 * RowCallbackHandler and nothing is returned.
	 * 
	 * @param psc The implementation to create the PreparedStatement
	 * @param psb The implementation to bind variables to values
	 * @param rch The implementation to process Results
	 * @param options The Query Options Object
	 * @return Type <T> which is the output of the ResultSetExtractor
	 * @throws DataAccessException
	 */
	void query(PreparedStatementCreator psc, PreparedStatementBinder psb, RowCallbackHandler rch, QueryOptions options)
			throws DataAccessException;

	/**
	 * Uses the provided PreparedStatementCreator to prepare a new Session call. Binds the values from the
	 * PreparedStatementBinder to the available bind variables. The results of the PreparedStatement are processed with
	 * RowCallbackHandler and nothing is returned.
	 * 
	 * @param psc The implementation to create the PreparedStatement
	 * @param psb The implementation to bind variables to values
	 * @param rch The implementation to process Results
	 * @return Type <T> which is the output of the ResultSetExtractor
	 * @throws DataAccessException
	 */
	void query(PreparedStatementCreator psc, PreparedStatementBinder psb, RowCallbackHandler rch)
			throws DataAccessException;

	/**
	 * Uses the provided PreparedStatementCreator to prepare a new Session call. Binds the values from the
	 * PreparedStatementBinder to the available bind variables. The results of the PreparedStatement are processed with
	 * RowMapper implementation provided and a List is returned with elements of Type <T> for each Row returned.
	 * 
	 * @param psc The implementation to create the PreparedStatement
	 * @param psb The implementation to bind variables to values
	 * @param rowMapper The implementation for mapping each Row returned.
	 * @param options The Query Options Object
	 * @return Type <T> which is the output of the ResultSetExtractor
	 * @throws DataAccessException
	 */
	<T> List<T> query(PreparedStatementCreator psc, PreparedStatementBinder psb, RowMapper<T> rowMapper,
			QueryOptions options) throws DataAccessException;

	/**
	 * Uses the provided PreparedStatementCreator to prepare a new Session call. Binds the values from the
	 * PreparedStatementBinder to the available bind variables. The results of the PreparedStatement are processed with
	 * RowMapper implementation provided and a List is returned with elements of Type <T> for each Row returned.
	 * 
	 * @param psc The implementation to create the PreparedStatement
	 * @param psb The implementation to bind variables to values
	 * @param rowMapper The implementation for mapping each Row returned.
	 * @return Type <T> which is the output of the ResultSetExtractor
	 * @throws DataAccessException
	 */
	<T> List<T> query(PreparedStatementCreator psc, PreparedStatementBinder psb, RowMapper<T> rowMapper)
			throws DataAccessException;

	/**
	 * Describe the current Ring. This uses the provided {@link RingMemberHostMapper} to provide the basics of the
	 * Cassandra Ring topology.
	 * 
	 * @return The list of ring tokens that are active in the cluster
	 */
	List<RingMember> describeRing() throws DataAccessException;

	/**
	 * Describe the current Ring. Application code must provide its own {@link HostMapper} implementation to process the
	 * lists of hosts returned by the Cassandra Cluster Metadata.
	 * 
	 * @param hostMapper The implementation to use for host mapping.
	 * @return Collection generated by the provided HostMapper.
	 * @throws DataAccessException
	 */
	<T> Collection<T> describeRing(HostMapper<T> hostMapper) throws DataAccessException;

	/**
	 * Get the current Session used for operations in the implementing class.
	 * 
	 * @return The DataStax Driver Session Object
	 */
	Session getSession();

	/**
	 * This is an operation designed for high performance writes. The cql is used to create a PreparedStatement once, then
	 * all row values are bound to the single PreparedStatement and executed against the Session.
	 * <p>
	 * This is used internally by the other ingest() methods, but can be used if you want to write your own RowIterator.
	 * The Object[] length returned by the next() implementation must match the number of bind variables in the CQL.
	 * </p>
	 * 
	 * @param cql The CQL
	 * @param rowIterator Implementation to provide the Object[] to be bound to the CQL.
	 * @param options The Query Options Object
	 */
	void ingest(String cql, RowIterator rowIterator, WriteOptions options);

	/**
	 * This is an operation designed for high performance writes. The CQL is used to create a PreparedStatement once, then
	 * all row values are bound to the single PreparedStatement and executed against the Session.
	 * <p>
	 * This is used internally by the other ingest() methods, but can be used if you want to write your own RowIterator.
	 * The Object[] length returned by the next() implementation must match the number of bind variables in the CQL.
	 * </p>
	 * 
	 * @param cql The CQL
	 * @param rowIterator Implementation to provide the Object[] to be bound to the CQL.
	 */
	void ingest(String cql, RowIterator rowIterator);

	/**
	 * This is an operation designed for high performance writes. The CQL is used to create a PreparedStatement once, then
	 * all row values are bound to the single PreparedStatement and executed against the Session.
	 * <p>
	 * The List<?> length must match the number of bind variables in the CQL.
	 * </p>
	 * 
	 * @param cql The CQL
	 * @param rows List of List<?> with data to bind to the CQL.
	 * @param options The Query Options Object
	 */
	void ingest(String cql, List<List<?>> rows, WriteOptions options);

	/**
	 * This is an operation designed for high performance writes. The CQL is used to create a {@link PreparedStatement}
	 * once, then all row values are bound to that {@link PreparedStatement} and executed against the {@link Session}.
	 * <p>
	 * The lengths of the nested {@link List}s must not be less than the number of bind variables in the CQL.
	 * </p>
	 * 
	 * @param cql The CQL
	 * @param rows The data to bind to the CQL statement
	 */
	void ingest(String cql, List<List<?>> rows);

	/**
	 * This is an operation designed for high performance writes. The CQL is used to create a {@link PreparedStatement}
	 * once, then all row values are bound to that {@link PreparedStatement} and executed against the {@link Session}.
	 * <p>
	 * The lengths of the nested object arrays must not be less than the number of bind variables in the CQL.
	 * </p>
	 * 
	 * @param cql The CQL
	 * @param rows The data to bind to the CQL statement
	 */
	void ingest(String cql, Object[][] rows);

	/**
	 * This is an operation designed for high performance writes. The CQL is used to create a {@link PreparedStatement}
	 * once, then all row values are bound to that {@link PreparedStatement} and executed against the {@link Session}.
	 * <p>
	 * The lengths of the nested object arrays must not be less than the number of bind variables in the CQL.
	 * </p>
	 * 
	 * @param cql The CQL
	 * @param rows The data to bind to the CQL statement
	 * @param options The Query Options Object
	 */
	void ingest(String cql, Object[][] rows, WriteOptions options);

	/**
	 * Delete all rows in the table
	 * 
	 * @param tableName
	 */
	void truncate(CqlIdentifier tableName);

	/**
	 * Delete all rows in the table
	 * 
	 * @param tableName
	 */
	void truncate(String tableName);

	/**
	 * Counts all rows for given table
	 * 
	 * @param tableName
	 * @return
	 */
	long count(CqlIdentifier tableName);

	/**
	 * Counts all rows for given table
	 * 
	 * @param tableName
	 * @return
	 */
	long count(String tableName);

	/**
	 * Convenience method to convert the given specification to CQL and execute it.
	 * 
	 * @param specification The specification to execute; must not be null.
	 */
	ResultSet execute(DropTableSpecification specification);

	/**
	 * Convenience method to convert the given specification to CQL and execute it.
	 * 
	 * @param specification The specification to execute; must not be null.
	 */
	ResultSet execute(CreateTableSpecification specification);

	/**
	 * Convenience method to convert the given specification to CQL and execute it.
	 * 
	 * @param specification The specification to execute; must not be null.
	 */
	ResultSet execute(AlterTableSpecification specification);

	/**
	 * Convenience method to convert the given specification to CQL and execute it.
	 * 
	 * @param specification The specification to execute; must not be null.
	 */
	ResultSet execute(DropKeyspaceSpecification specification);

	/**
	 * Convenience method to convert the given specification to CQL and execute it.
	 * 
	 * @param specification The specification to execute; must not be null.
	 */
	ResultSet execute(CreateKeyspaceSpecification specification);

	/**
	 * Convenience method to convert the given specification to CQL and execute it.
	 * 
	 * @param specification The specification to execute; must not be null.
	 */
	ResultSet execute(AlterKeyspaceSpecification specification);

	/**
	 * Convenience method to convert the given specification to CQL and execute it.
	 * 
	 * @param specification The specification to execute; must not be null.
	 */
	ResultSet execute(DropIndexSpecification specification);

	/**
	 * Convenience method to convert the given specification to CQL and execute it.
	 * 
	 * @param specification The specification to execute; must not be null.
	 */
	ResultSet execute(CreateIndexSpecification specification);
}
