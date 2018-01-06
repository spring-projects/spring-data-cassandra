/*
 * Copyright 2016-2018 the original author or authors.
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
package org.springframework.data.cassandra;

import java.util.List;

import reactor.core.publisher.Flux;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.Row;

/**
 * The reactive result of a query.
 * <p>
 * The retrieval of the rows of a {@link ReactiveResultSet} is generally paged (a first page of result is fetched and
 * the next one is only fetched once all the results of the first one has been consumed). The size of the pages can be
 * configured either globally through {@link com.datastax.driver.core.QueryOptions#setFetchSize} or per-statement with
 * {@link com.datastax.driver.core.Statement#setFetchSize}.
 * <p>
 * Please note however that this {@link ReactiveResultSet} paging is not available with the version 1 of the native
 * protocol (i.e. with Cassandra 1.2 or if version 1 has been explicitly requested through
 * {@link com.datastax.driver.core.Cluster.Builder#withProtocolVersion}). If the protocol version 1 is in use, a
 * {@link ReactiveResultSet} is always fetched in it's entirely and it's up to the client to make sure that no query can
 * yield {@link ReactiveResultSet} that won't hold in memory.
 * <p>
 * Note that this class is not thread-safe.
 *
 * @author Mark Paluch
 * @since 2.0
 * @see Flux
 * @see ReactiveSession
 * @see com.datastax.driver.core.ResultSet
 */
public interface ReactiveResultSet {

	/**
	 * Returns a {@link Flux} over the rows contained in this result set.
	 * <p>
	 * The {@link Flux} will stream over all records that in this {@link ReactiveResultSet} according to the reactive
	 * demand.
	 * <p>
	 *
	 * @return a {@link Flux} of rows that will stream over all {@link Row rows} in this {@link ReactiveResultSet}.
	 */
	Flux<Row> rows();

	/**
	 * Returns the columns returned in this ResultSet.
	 *
	 * @return the columns returned in this ResultSet.
	 */
	ColumnDefinitions getColumnDefinitions();

	/**
	 * If the query that produced this ResultSet was a conditional update, return whether it was successfully applied.
	 * <p>
	 * For consistency, this method always returns {@code true} for non-conditional queries (although there is no reason
	 * to call the method in that case). This is also the case for conditional DDL statements
	 * ({@code CREATE KEYSPACE... IF NOT EXISTS}, {@code CREATE TABLE... IF NOT EXISTS}), for which Cassandra doesn't
	 * return an {@code [applied]} column.
	 * <p>
	 * Note that, for versions of Cassandra strictly lower than 2.0.9 and 2.1.0-rc2, a server-side bug (CASSANDRA-7337)
	 * causes this method to always return {@code true} for batches containing conditional queries.
	 *
	 * @return if the query was a conditional update, whether it was applied. {@code true} for other types of queries.
	 * @see <a href="https://issues.apache.org/jira/browse/CASSANDRA-7337">CASSANDRA-7337</a>
	 */
	boolean wasApplied();

	/**
	 * Returns information on the execution of the last query made for this result set.
	 * <p>
	 * Note that in most cases, a result set is fetched with only one query, but large result sets can be paged and thus
	 * be retrieved by multiple queries. In that case this method return the {@link ExecutionInfo} for the last query
	 * performed. To retrieve the information for all queries, use {@link #getAllExecutionInfo}.
	 * <p>
	 * The returned object includes basic information such as the queried hosts, but also the Cassandra query trace if
	 * tracing was enabled for the query.
	 *
	 * @return the execution info for the last query made for this result set.
	 */
	ExecutionInfo getExecutionInfo();

	/**
	 * Return the execution information for all queries made to retrieve this result set.
	 * <p>
	 * Unless the result set is large enough to get paged underneath, the returned list will be singleton. If paging has
	 * been used however, the returned list contains the {@link ExecutionInfo} objects for all the queries done to obtain
	 * this result set (at the time of the call) in the order those queries were made.
	 *
	 * @return a list of the execution info for all the queries made for this result set.
	 */
	List<ExecutionInfo> getAllExecutionInfo();

}
