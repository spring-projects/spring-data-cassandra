/*
 * Copyright 2017-2021 the original author or authors.
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
package org.springframework.data.cassandra;

import reactor.core.publisher.Mono;

import java.io.Closeable;
import java.util.Map;

import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;

/**
 * A session holds connections to a Cassandra cluster, allowing it to be queried. {@link ReactiveSession} executes
 * queries and prepares statements in a reactive style returning results wrapped in {@link Mono} and
 * {@link reactor.core.publisher.Flux}.
 * <p/>
 * Each session maintains multiple connections to the cluster nodes, provides policies to choose which node to use for
 * each query (round-robin on all nodes of the cluster by default), and handles retries for failed queries (when it
 * makes sense).
 * <p/>
 * Session instances are thread-safe and usually a single instance is enough per application. As a given session can
 * only be "logged" into one keyspace at a time (where the "logged" keyspace is the one used by queries that don't
 * explicitly use a fully qualified table name), it can make sense to create one session per keyspace used. This is
 * however not necessary when querying multiple keyspaces since it is always possible to use a single session with fully
 * qualified table names in queries.
 *
 * @author Mark Paluch
 * @since 2.0
 * @see org.reactivestreams.Publisher
 * @see Mono
 * @see ReactiveResultSet
 */
public interface ReactiveSession extends Closeable {

	/**
	 * Whether this Session instance has been closed.
	 * <p/>
	 * Note that this method returns true as soon as the closing of this Session has started but it does not guarantee
	 * that the closing is done. If you want to guarantee that the closing is done, you can call {@code close()} and wait
	 * until it returns (or call the get method on {@code closeAsync()} with a very short timeout and check this doesn't
	 * timeout).
	 *
	 * @return {@code true} if this Session instance has been closed, {@code false} otherwise.
	 */
	boolean isClosed();

	/**
	 * Returns a context that provides access to all the policies used by this driver instance.
	 *
	 * @return a context that provides access to all the policies used by this driver instance.
	 */
	DriverContext getContext();

	/**
	 * Executes the provided query.
	 * <p/>
	 * This is a convenience method for {@code execute(new SimpleStatement(query))}.
	 *
	 * @param query the CQL query to execute.
	 * @return the result of the query. That result will never be null but can be empty (and will be for any non SELECT
	 *         query).
	 */
	Mono<ReactiveResultSet> execute(String query);

	/**
	 * Executes the provided query using the provided values.
	 * <p/>
	 * This is a convenience method for {@code execute(new SimpleStatement(query, values))}.
	 *
	 * @param query the CQL query to execute.
	 * @param values values required for the execution of {@code query}. See
	 *          {@link SimpleStatement#newInstance(String, Object...)} for more details.
	 * @return the result of the query. That result will never be null but can be empty (and will be for any non SELECT
	 *         query).
	 */
	Mono<ReactiveResultSet> execute(String query, Object... values);

	/**
	 * Executes the provided query using the provided named values.
	 * <p/>
	 * This is a convenience method for {@code execute(new SimpleStatement(query, values))}.
	 *
	 * @param query the CQL query to execute.
	 * @param values values required for the execution of {@code query}. See
	 *          {@link SimpleStatement#newInstance(String, Map)} for more details.
	 * @return the result of the query. That result will never be null but can be empty (and will be for any non SELECT
	 *         query).
	 */
	Mono<ReactiveResultSet> execute(String query, Map<String, Object> values);

	/**
	 * Executes the provided query.
	 * <p/>
	 * This method blocks until at least some result has been received from the database. However, for SELECT queries, it
	 * does not guarantee that the result has been received in full. But it does guarantee that some response has been
	 * received from the database, and in particular guarantees that if the request is invalid, an exception will be
	 * thrown by this method.
	 *
	 * @param statement the CQL query to execute (that can be any {@link Statement}).
	 * @return the result of the query. That result will never be null but can be empty (and will be for any non SELECT
	 *         query).
	 */
	Mono<ReactiveResultSet> execute(Statement<?> statement);

	/**
	 * Prepares the provided query string.
	 *
	 * @param query the CQL query string to prepare
	 * @return the prepared statement corresponding to {@code query}.
	 */
	Mono<PreparedStatement> prepare(String query);

	/**
	 * Prepares the provided query.
	 * <p/>
	 * This method behaves like {@link #prepare(String)}, but note that the resulting {@code PreparedStatement} will
	 * inherit the query properties set on {@code statement}. Concretely, this means that in the following code:
	 *
	 * <pre>
	 * Statement toPrepare = SimpleStatement.newInstance("SELECT * FROM test WHERE k=?")
	 * 		.setConsistencyLevel(ConsistencyLevel.QUORUM);
	 * PreparedStatement prepared = session.prepare(toPrepare);
	 * session.execute(prepared.bind("someValue"));
	 * </pre>
	 *
	 * the final execution will be performed with Quorum consistency.
	 * <p/>
	 * Please note that if the same CQL statement is prepared more than once, all calls to this method will return the
	 * same {@code PreparedStatement} object but the method will still apply the properties of the prepared
	 * {@code Statement} to this object.
	 *
	 * @param statement the statement to prepare
	 * @return the prepared statement corresponding to {@code statement}.
	 * @throws IllegalArgumentException if {@code statement.getValues() != null} (values for executing a prepared
	 *           statement should be provided after preparation though the {@link PreparedStatement#bind} method or
	 *           through a corresponding {@link BoundStatement}).
	 */
	Mono<PreparedStatement> prepare(SimpleStatement statement);

	/**
	 * Initiates a shutdown of this session instance and blocks until that shutdown completes.
	 */
	@Override
	void close();

}
