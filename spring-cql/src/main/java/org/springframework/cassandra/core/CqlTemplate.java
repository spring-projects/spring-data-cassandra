/*
 * Copyright 2013-2016 the original author or authors.
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

import static org.springframework.cassandra.core.cql.CqlIdentifier.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.springframework.cassandra.core.cql.CqlIdentifier;
import org.springframework.cassandra.core.cql.generator.AlterKeyspaceCqlGenerator;
import org.springframework.cassandra.core.cql.generator.AlterTableCqlGenerator;
import org.springframework.cassandra.core.cql.generator.CreateIndexCqlGenerator;
import org.springframework.cassandra.core.cql.generator.CreateKeyspaceCqlGenerator;
import org.springframework.cassandra.core.cql.generator.CreateTableCqlGenerator;
import org.springframework.cassandra.core.cql.generator.DropIndexCqlGenerator;
import org.springframework.cassandra.core.cql.generator.DropKeyspaceCqlGenerator;
import org.springframework.cassandra.core.cql.generator.DropTableCqlGenerator;
import org.springframework.cassandra.core.keyspace.AlterKeyspaceSpecification;
import org.springframework.cassandra.core.keyspace.AlterTableSpecification;
import org.springframework.cassandra.core.keyspace.CreateIndexSpecification;
import org.springframework.cassandra.core.keyspace.CreateKeyspaceSpecification;
import org.springframework.cassandra.core.keyspace.CreateTableSpecification;
import org.springframework.cassandra.core.keyspace.DropIndexSpecification;
import org.springframework.cassandra.core.keyspace.DropKeyspaceSpecification;
import org.springframework.cassandra.core.keyspace.DropTableSpecification;
import org.springframework.cassandra.support.CassandraAccessor;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.util.Assert;

import com.datastax.driver.core.*;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Truncate;
import com.datastax.driver.core.querybuilder.Update;

/**
 * <b>This is the central class in the Cassandra core package.</b> {@link CqlTemplate} simplifies the use of Cassandra
 * and helps to avoid common errors. The template executes the core Cassandra workflow, leaving application code to
 * provide CQL and result handling. The template executes CQL queries, provides different ways to extract and map
 * results, and provides Exception translation to the generic, more informative exception hierarchy defined in the
 * <code>org.springframework.dao</code> package.
 * <p>
 * For working with POJOs, use the CassandraTemplate.
 * </p>
 *
 * @author David Webb
 * @author Matthew Adams
 * @author Ryan Scheidter
 * @author Antoine Toulme
 * @author John Blum
 * @author Mark Paluch
 * @see org.springframework.cassandra.core.CqlOperations
 * @see org.springframework.cassandra.support.CassandraAccessor
 */
public class CqlTemplate extends CassandraAccessor implements CqlOperations {

	protected static final Executor RUN_RUNNABLE_EXECUTOR = new Executor() {

		@Override
		@SuppressWarnings("all")
		public void execute(Runnable command) {
			command.run();
		}
	};

	protected static final ResultSetExtractor<ResultSet> RESULT_SET_RETURNING_EXTRACTOR = new ResultSetExtractor<ResultSet>() {

		@Override
		public ResultSet extractData(ResultSet resultSet) {
			return resultSet;
		}
	};

	protected String logCql(String cql) {
		return logCql("executing CQL [{}]", cql);
	}

	protected String logCql(String message, String cql) {
		logDebug(message, cql);
		return cql;
	}

	protected <T extends Statement> T logStatement(T statement) {
		logDebug("executing statement [{}]", statement);
		return statement;
	}

	/**
	 * Add common {@link QueryOptions} to Cassandra {@link PreparedStatement}s.
	 *
	 * @param preparedStatement the Cassandra {@link PreparedStatement} to execute.
	 * @param queryOptions query options (e.g. consistency level) to add to the Cassandra {@link PreparedStatement}.
	 */
	public static PreparedStatement addPreparedStatementOptions(PreparedStatement preparedStatement,
			QueryOptions queryOptions) {

		if (queryOptions != null) {
			if (queryOptions.getDriverConsistencyLevel() != null) {
				preparedStatement.setConsistencyLevel(queryOptions.getDriverConsistencyLevel());
			} else if (queryOptions.getConsistencyLevel() != null) {
				preparedStatement.setConsistencyLevel(ConsistencyLevelResolver.resolve(queryOptions.getConsistencyLevel()));
			}

			if (queryOptions.getDriverRetryPolicy() != null) {
				preparedStatement.setRetryPolicy(queryOptions.getDriverRetryPolicy());
			} else if (queryOptions.getRetryPolicy() != null) {
				preparedStatement.setRetryPolicy(RetryPolicyResolver.resolve(queryOptions.getRetryPolicy()));
			}
		}

		return preparedStatement;
	}

	/**
	 * Add common {@link QueryOptions} to all types of queries.
	 *
	 * @param statement CQL {@link Statement} to execute.
	 * @param queryOptions query options (e.g. consistency level) to add to the CQL statement.
	 * @return the given {@link Statement}.
	 */
	public static <T extends Statement> T addQueryOptions(T statement, QueryOptions queryOptions) {

		if (queryOptions != null) {
			if (queryOptions.getDriverConsistencyLevel() != null) {
				statement.setConsistencyLevel(queryOptions.getDriverConsistencyLevel());
			} else if (queryOptions.getConsistencyLevel() != null) {
				statement.setConsistencyLevel(ConsistencyLevelResolver.resolve(queryOptions.getConsistencyLevel()));
			}

			if (queryOptions.getDriverRetryPolicy() != null) {
				statement.setRetryPolicy(queryOptions.getDriverRetryPolicy());
			} else if (queryOptions.getRetryPolicy() != null) {
				statement.setRetryPolicy(RetryPolicyResolver.resolve(queryOptions.getRetryPolicy()));
			}

			if (queryOptions.getFetchSize() != null) {
				statement.setFetchSize(queryOptions.getFetchSize());
			}

			if (queryOptions.getReadTimeout() != null) {
				statement.setReadTimeoutMillis(queryOptions.getReadTimeout().intValue());
			}

			if (queryOptions.getTracing() != null) {
				if (queryOptions.getTracing()) {
					statement.enableTracing();
				} else {
					statement.disableTracing();
				}
			}
		}

		return statement;
	}

	/**
	 * Add common {@link WriteOptions} options to {@link Insert} CQL statements.
	 *
	 * @param insert {@link Insert} CQL statement to execute.
	 * @param writeOptions write options (e.g. consistency level) to add to the CQL statement.
	 * @return the given {@link Insert}.
	 */
	public static Insert addWriteOptions(Insert insert, WriteOptions writeOptions) {

		if (writeOptions != null) {

			addQueryOptions(insert, writeOptions);

			if (writeOptions.getTtl() != null) {
				insert.using(QueryBuilder.ttl(writeOptions.getTtl()));
			}
		}

		return insert;
	}

	/**
	 * Add common {@link WriteOptions} options to {@link Update} CQL statements.
	 *
	 * @param update {@link Update} CQL statement to execute.
	 * @param writeOptions write options (e.g. consistency level) to add to the CQL statement.
	 * @return the given {@link Update}.
	 */
	public static Update addWriteOptions(Update update, WriteOptions writeOptions) {

		if (writeOptions != null) {

			addQueryOptions(update, writeOptions);

			if (writeOptions.getTtl() != null) {
				update.using(QueryBuilder.ttl(writeOptions.getTtl()));
			}
		}

		return update;
	}

	/**
	 * Constructs an uninitialized instance of {@link CqlTemplate}. A Cassandra {@link Session} is required before use.
	 *
	 * @see #CqlTemplate(Session)
	 */
	public CqlTemplate() {}

	/**
	 * Constructs an instance of {@link CqlTemplate} initialized with the given {@link Session}.
	 *
	 * @param session Cassandra {@link Session} used by this template to perform CQL operations. Must not be
	 *          {@literal null}.
	 * @see com.datastax.driver.core.Session
	 * @see #setSession(Session)
	 */
	// TODO: should not call setSession(..) in constructor for initialization safety;
	// only really matters if CqlTemplate makes Thread-safety guarantees, which currently it does not.
	public CqlTemplate(Session session) {
		setSession(session);
	}

	/**
	 * Executes the given command in a Cassandra {@link Session}.
	 *
	 * @param <T> Class type of the callback return value.
	 * @param callback {@link SessionCallback} to execute in the context of a Cassandra {@link Session}.
	 * @return the result of the callback.
	 */
	protected <T> T doExecute(SessionCallback<T> callback) {

		Assert.notNull(callback, "SessionCallback must not be null");

		try {
			return callback.doInSession(getSession());
		} catch (Throwable t) {
			throw translateExceptionIfPossible(t);
		}
	}

	protected ResultSet doExecuteQueryReturnResultSet(final String query) {
		return doExecute(new SessionCallback<ResultSet>() {
			@Override
			public ResultSet doInSession(Session session) throws DataAccessException {
				return session.execute(logCql(query));
			}
		});
	}

	protected ResultSet doExecuteQueryReturnResultSet(final Select select) {
		return doExecute(new SessionCallback<ResultSet>() {
			@Override public ResultSet doInSession(Session session) throws DataAccessException {
				return session.execute(logStatement(select));
			}
		});
	}

	@Override
	public <T> T execute(SessionCallback<T> sessionCallback) {
		return doExecute(sessionCallback);
	}

	@Override
	public void execute(String cql) {
		execute(cql, (QueryOptions) null);
	}

	@Override
	public void execute(String cql, QueryOptions options) {
		doExecute(cql, options);
	}

	@Override
	public void execute(Statement statement) {
		doExecute(statement);
	}

	@Override
	public ResultSetFuture queryAsynchronously(final String cql) {

		return execute(new SessionCallback<ResultSetFuture>() {

			@Override
			public ResultSetFuture doInSession(Session session) {
				return session.executeAsync(logCql("async execute CQL [{}]", cql));
			}
		});
	}

	@Override
	public <T> T queryAsynchronously(String cql, ResultSetExtractor<T> resultSetExtractor, Long timeout,
			TimeUnit timeUnit) {

		return queryAsynchronously(cql, resultSetExtractor, timeout, timeUnit, null);
	}

	@Override
	public <T> T queryAsynchronously(final String cql, final ResultSetExtractor<T> resultSetExtractor, final Long timeout,
			final TimeUnit timeUnit, final QueryOptions options) {

		return resultSetExtractor.extractData(execute(new SessionCallback<ResultSet>() {

			@Override
			public ResultSet doInSession(Session session) {

				Statement statement = addQueryOptions(new SimpleStatement(logCql(cql)), options);

				ResultSetFuture resultSetFuture = session.executeAsync(statement);

				try {
					return resultSetFuture.get(timeout, timeUnit);
				} catch (TimeoutException e) {
					throw new QueryTimeoutException(String.format(
							"timeout occurred in [%1$d %2$s] while asynchronously executing CQL [%3$s]", timeout, timeUnit, cql), e);
				} catch (InterruptedException e) {
					throw translateExceptionIfPossible(e);
				} catch (ExecutionException e) {

					if (e.getCause() instanceof Exception) {
						throw translateExceptionIfPossible(e.getCause());
					}
					throw new CassandraUncategorizedDataAccessException("Unknown Throwable", e.getCause());
				}
			}
		}));
	}

	@Override
	public ResultSetFuture queryAsynchronously(final String cql, final QueryOptions queryOptions) {

		return execute(new SessionCallback<ResultSetFuture>() {

			@Override
			public ResultSetFuture doInSession(Session session) {
				return session.executeAsync(addQueryOptions(new SimpleStatement(logCql(cql)), queryOptions));
			}
		});
	}

	@Override
	public Cancellable queryAsynchronously(String cql, Runnable listener) {
		return queryAsynchronously(cql, listener, RUN_RUNNABLE_EXECUTOR);
	}

	@Override
	public Cancellable queryAsynchronously(String cql, AsynchronousQueryListener listener) {
		return queryAsynchronously(cql, listener, RUN_RUNNABLE_EXECUTOR);
	}

	@Override
	public Cancellable queryAsynchronously(String cql, Runnable listener, QueryOptions queryOptions) {
		return queryAsynchronously(cql, listener, queryOptions, RUN_RUNNABLE_EXECUTOR);
	}

	@Override
	public Cancellable queryAsynchronously(String cql, AsynchronousQueryListener listener, QueryOptions queryOptions) {
		return queryAsynchronously(cql, listener, queryOptions, RUN_RUNNABLE_EXECUTOR);
	}

	@Override
	public Cancellable queryAsynchronously(String cql, Runnable listener, Executor executor) {
		return queryAsynchronously(cql, listener, null, executor);
	}

	@Override
	public Cancellable queryAsynchronously(String cql, AsynchronousQueryListener listener, Executor executor) {
		return queryAsynchronously(cql, listener, null, executor);
	}

	@Override
	public Cancellable queryAsynchronously(final String cql, final Runnable listener, final QueryOptions queryOptions,
			final Executor executor) {

		return execute(new SessionCallback<Cancellable>() {

			@Override
			public Cancellable doInSession(Session session) {

				Statement statement = addQueryOptions(new SimpleStatement(logCql("async execute CQL [{}]", cql)), queryOptions);

				ResultSetFuture resultSetFuture = session.executeAsync(statement);
				resultSetFuture.addListener(listener, executor);
				return new ResultSetFutureCancellable(resultSetFuture);
			}
		});
	}

	@Override
	public Cancellable queryAsynchronously(final String cql, final AsynchronousQueryListener listener,
			final QueryOptions queryOptions, final Executor executor) {

		return execute(new SessionCallback<Cancellable>() {

			@Override
			public Cancellable doInSession(Session session) {

				Statement statement = addQueryOptions(new SimpleStatement(logCql("async execute CQL [{}]", cql)),
					queryOptions);

				final ResultSetFuture resultSetFuture = session.executeAsync(statement);

				Runnable runnable = new Runnable() {
					@Override
					public void run() {
						listener.onQueryComplete(resultSetFuture);
					}
				};

				resultSetFuture.addListener(runnable, executor);

				return new ResultSetFutureCancellable(resultSetFuture);
			}
		});
	}

	@SuppressWarnings("unused")
	public <T> T queryAsynchronously(String cql, ResultSetFutureExtractor<T> resultSetFutureExtractor) {
		return queryAsynchronously(cql, resultSetFutureExtractor, null);
	}

	public <T> T queryAsynchronously(final String cql, ResultSetFutureExtractor<T> resultSetFutureExtractor,
			final QueryOptions queryOptions) {

		return resultSetFutureExtractor.extractData(execute(new SessionCallback<ResultSetFuture>() {

			@Override
			public ResultSetFuture doInSession(Session session) {
				return session
						.executeAsync(addQueryOptions(new SimpleStatement(logCql("async execute CQL [{}]", cql)), queryOptions));
			}
		}));
	}

	@Override
	public <T> T query(String cql, ResultSetExtractor<T> resultSetExtractor) {
		return query(cql, resultSetExtractor, null);
	}

	@Override
	public <T> T query(String cql, ResultSetExtractor<T> resultSetExtractor, QueryOptions queryOptions) {

		Assert.notNull(cql, "CQL must not be null");

		return resultSetExtractor.extractData(doExecute(cql, queryOptions));
	}

	@Override
	public void query(String cql, RowCallbackHandler rowCallbackHandler) {
		query(cql, rowCallbackHandler, null);
	}

	@Override
	public void query(String cql, RowCallbackHandler rowCallbackHandler, QueryOptions queryOptions) {
		process(doExecute(cql, queryOptions), rowCallbackHandler);
	}

	@Override
	public <T> List<T> query(String cql, RowMapper<T> rowMapper, QueryOptions queryOptions) {
		return process(doExecute(cql, queryOptions), rowMapper);
	}

	@Override
	public ResultSet query(String cql) {
		return query(cql, (QueryOptions) null);
	}

	@Override
	public ResultSet query(String cql, QueryOptions queryOptions) {
		return query(cql, RESULT_SET_RETURNING_EXTRACTOR, queryOptions);
	}

	@Override
	public <T> List<T> query(String cql, RowMapper<T> rowMapper) {
		return query(cql, rowMapper, null);
	}

	@Override
	public List<Map<String, Object>> queryForListOfMap(String cql) {
		return processListOfMap(doExecute(cql, null));
	}

	@Override
	public <T> List<T> queryForList(String cql, Class<T> elementType) {
		return processList(doExecute(cql, null), elementType);
	}

	@Override
	public Map<String, Object> queryForMap(String cql) {
		return processMap(doExecute(cql, null));
	}

	@Override
	public <T> T queryForObject(String cql, Class<T> requiredType) {
		return processOne(doExecute(cql, null), requiredType);
	}

	@Override
	public <T> T queryForObject(String cql, RowMapper<T> rowMapper) {
		return processOne(doExecute(cql, null), rowMapper);
	}

	@SuppressWarnings("unused")
	protected ResultSet doExecute(String cql) {
		return doExecute(cql, null);
	}

	protected ResultSet doExecute(String cql, QueryOptions queryOptions) {
		return doExecute(addQueryOptions(new SimpleStatement(logCql(cql)), queryOptions));
	}

	/**
	 * Execute a command at the Session Level with optional options
	 *
	 * @param statement The query to execute.
	 */
	protected ResultSet doExecute(final Statement statement) {

		return doExecute(new SessionCallback<ResultSet>() {

			@Override
			public ResultSet doInSession(Session session) {

				logDebug("execute [{}]", statement);
				return session.execute(statement);
			}
		});
	}

	protected ResultSetFuture doExecuteAsync(final Statement statement) {

		return doExecute(new SessionCallback<ResultSetFuture>() {

			@Override
			public ResultSetFuture doInSession(Session session) {

				logDebug("async execute [{}]", statement);
				return session.executeAsync(statement);
			}
		});
	}

	protected Cancellable doExecuteAsync(final Statement statement, final AsynchronousQueryListener listener) {
		return doExecuteAsync(statement, listener, null);
	}

	protected Cancellable doExecuteAsync(final Statement statement, final AsynchronousQueryListener listener,
			final QueryOptions queryOptions) {

		return doExecute(new SessionCallback<Cancellable>() {

			@Override
			public Cancellable doInSession(Session session) {
				logDebug("async execute [{}]", statement);

				final ResultSetFuture resultSetFuture = session.executeAsync(addQueryOptions(statement, queryOptions));

				if (listener != null) {
					resultSetFuture.addListener(new Runnable() {
						@Override
						public void run() {
							listener.onQueryComplete(resultSetFuture);
						}
					}, RUN_RUNNABLE_EXECUTOR);
				}

				return new ResultSetFutureCancellable(resultSetFuture);
			}
		});
	}

	protected Object firstColumnToObject(Row row) {

		Iterator<Definition> columnDefinitions = row.getColumnDefinitions().iterator();
		return (columnDefinitions.hasNext() ? columnToObject(row, columnDefinitions.next()) : null);
	}

	/* (non-Javadoc) */
	<T> T columnToObject(Row row, Definition columnDefinition) {
		return (T) row.getObject(columnDefinition.getName());
	}

	protected Map<String, Object> toMap(Row row) {

		Map<String, Object> map = null;

		if (row != null) {
			ColumnDefinitions columns = row.getColumnDefinitions();
			map = new HashMap<String, Object>(columns.size());

			for (Definition columnDefinition : columns.asList()) {
				map.put(columnDefinition.getName(), columnToObject(row, columnDefinition));
			}
		}

		return map;
	}

	@Override
	public List<RingMember> describeRing() {
		return new ArrayList<RingMember>(describeRing(new RingMemberHostMapper()));
	}

	/**
	 * Requests the set of hosts in the Cassandra cluster from the current {@link Session}.
	 */
	protected Set<Host> getHosts() {

		return doExecute(new SessionCallback<Set<Host>>() {

			@Override
			public Set<Host> doInSession(Session session) {
				return session.getCluster().getMetadata().getAllHosts();
			}
		});
	}

	@Override
	public <T> Collection<T> describeRing(HostMapper<T> hostMapper) {
		return hostMapper.mapHosts(getHosts());
	}

	@Override
	public ResultSetFuture executeAsynchronously(String cql) {
		return executeAsynchronously(cql, (QueryOptions) null);
	}

	@Override
	public ResultSetFuture executeAsynchronously(String cql, QueryOptions queryOptions) {
		return doExecuteAsync(addQueryOptions(new SimpleStatement(logCql(cql)), queryOptions));
	}

	@Override
	public Cancellable executeAsynchronously(String cql, Runnable listener) {
		return executeAsynchronously(cql, listener, RUN_RUNNABLE_EXECUTOR);
	}

	@Override
	public Cancellable executeAsynchronously(final String cql, final Runnable listener, final Executor executor) {

		return execute(new SessionCallback<Cancellable>() {

			@Override
			public Cancellable doInSession(Session session) {

				Statement statement = new SimpleStatement(logCql("async execute CQL [{}]", cql));

				ResultSetFuture resultSetFuture = session.executeAsync(statement);
				resultSetFuture.addListener(listener, executor);

				return new ResultSetFutureCancellable(resultSetFuture);
			}
		});
	}

	@Override
	public Cancellable executeAsynchronously(String cql, AsynchronousQueryListener listener) {

		return executeAsynchronously(cql, listener, RUN_RUNNABLE_EXECUTOR);
	}

	@Override
	public Cancellable executeAsynchronously(final String cql, final AsynchronousQueryListener listener,
			final Executor executor) {

		return execute(new SessionCallback<Cancellable>() {

			@Override
			public Cancellable doInSession(Session session) {

				Statement statement = new SimpleStatement(logCql("async execute CQL [{}]", cql));

				final ResultSetFuture resultSetFuture = session.executeAsync(statement);

				Runnable runnable = new Runnable() {
					@Override
					public void run() {
						listener.onQueryComplete(resultSetFuture);
					}
				};

				resultSetFuture.addListener(runnable, executor);

				return new ResultSetFutureCancellable(resultSetFuture);
			}
		});
	}

	@Override
	public ResultSetFuture executeAsynchronously(Statement statement) {
		return doExecuteAsync(statement);
	}

	@Override
	public Cancellable executeAsynchronously(Statement statement, Runnable listener) {
		return executeAsynchronously(statement, listener, RUN_RUNNABLE_EXECUTOR);
	}

	@Override
	public Cancellable executeAsynchronously(Statement statement, AsynchronousQueryListener listener) {

		return executeAsynchronously(statement, listener, RUN_RUNNABLE_EXECUTOR);
	}

	@Override
	public Cancellable executeAsynchronously(final Statement statement, final Runnable listener,
			final Executor executor) {

		return execute(new SessionCallback<Cancellable>() {

			@Override
			public Cancellable doInSession(Session session) {

				logDebug("executing [{}]", statement);

				final ResultSetFuture resultSetFuture = session.executeAsync(statement);
				resultSetFuture.addListener(listener, executor);

				return new ResultSetFutureCancellable(resultSetFuture);
			}
		});
	}

	@Override
	public Cancellable executeAsynchronously(final Statement statement, final AsynchronousQueryListener listener,
			final Executor executor) {

		return execute(new SessionCallback<Cancellable>() {

			@Override
			public Cancellable doInSession(Session session) {

				logDebug("executing [{}]", statement);

				final ResultSetFuture resultSetFuture = session.executeAsync(statement);

				Runnable runnable = new Runnable() {
					@Override
					public void run() {
						listener.onQueryComplete(resultSetFuture);
					}
				};

				resultSetFuture.addListener(runnable, executor);

				return new ResultSetFutureCancellable(resultSetFuture);
			}
		});
	}

	@Override
	public void process(ResultSet resultSet, RowCallbackHandler rowCallbackHandler) {

		try {
			for (Row row : resultSet.all()) {
				rowCallbackHandler.processRow(row);
			}
		} catch (DriverException e) {
			throw translateExceptionIfPossible(e);
		}
	}

	@Override
	public <T> List<T> process(ResultSet resultSet, RowMapper<T> rowMapper) {

		try {

			List<Row> rows = resultSet.all();
			List<T> mappedRows = new ArrayList<T>(rows.size());

			int rowIndex = 0;

			for (Row row : rows) {
				mappedRows.add(rowMapper.mapRow(row, rowIndex++));
			}

			return mappedRows;
		} catch (DriverException dx) {
			throw translateExceptionIfPossible(dx);
		}
	}

	@Override
	public <T> T processOne(ResultSet resultSet, RowMapper<T> rowMapper) {

		Assert.notNull(resultSet, "ResultSet must not be null");
		Assert.notNull(rowMapper, "RowMapper must not be null");

		try {

			Row row = resultSet.one();

			if (row == null) {
				throw new IncorrectResultSizeDataAccessException(1, 0);
			}

			if (!resultSet.isExhausted()) {
				throw new IncorrectResultSizeDataAccessException("ResultSet size exceeds 1", 1);
			}

			return rowMapper.mapRow(row, 0);
		} catch (DriverException e) {
			throw translateExceptionIfPossible(e);
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> T processOne(ResultSet resultSet, Class<T> requiredType) {

		Assert.notNull(resultSet, "ResultSet must not be null");

		try {

			Row row = resultSet.one();

			if (row == null) {
				throw new IncorrectResultSizeDataAccessException(1, 0);
			}

			if (!resultSet.isExhausted()) {
				throw new IncorrectResultSizeDataAccessException("ResultSet size exceeds 1", 1);
			}

			return requiredType.cast(firstColumnToObject(row));
		} catch (DriverException e) {
			throw translateExceptionIfPossible(e);
		}
	}

	@Override
	public Map<String, Object> processMap(ResultSet resultSet) {
		return (resultSet != null ? toMap(resultSet.one()) : null);
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> List<T> processList(ResultSet resultSet, Class<T> elementType) {

		List<Row> rows = resultSet.all();
		List<T> list = new ArrayList<T>(rows.size());

		for (Row row : rows) {
			list.add(elementType.cast(firstColumnToObject(row)));
		}

		return list;
	}

	@Override
	public List<Map<String, Object>> processListOfMap(ResultSet resultSet) {

		List<Row> rows = resultSet.all();
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>(rows.size());

		for (Row row : rows) {
			list.add(toMap(row));
		}

		return list;
	}

	/**
	 * Attempts to translate the {@link Exception} into a Spring Data {@link Exception}.
	 *
	 * @param e the {@link Exception} to translate.
	 * @return the translated {@link RuntimeException}.
	 * @see <a href="http://docs.spring.io/spring/docs/current/spring-framework-reference/htmlsingle/#dao-exceptions">Consistent exception hierarchy</a>
	 */
	@SuppressWarnings("all")
	protected RuntimeException translateExceptionIfPossible(Throwable t) {
		return translateExceptionIfPossible(t, getExceptionTranslator());
	}

	/**
	 * Tries to convert the given {@link RuntimeException} into a {@link DataAccessException} but returns the original
	 * exception if the conversation failed. Thus allows safe re-throwing of the return value.
	 *
	 * @param e the exception to translate
	 * @param exceptionTranslator the {@link PersistenceExceptionTranslator} to be used for translation
	 * @return
	 */
	@SuppressWarnings("all")
	protected static RuntimeException translateExceptionIfPossible(Throwable t,
			PersistenceExceptionTranslator exceptionTranslator) {

		Assert.notNull(t, "Throwble must not be null");
		Assert.notNull(exceptionTranslator, "PersistenceExceptionTranslator must not be null");

		return (t instanceof RuntimeException) ? potentiallyConvertRuntimeException((RuntimeException) t, exceptionTranslator)
			:  new CassandraUncategorizedDataAccessException("Caught Uncategorized Exception", t);
	}

	/**
	 * Tries to convert the given {@link RuntimeException} into a {@link DataAccessException} but returns the original
	 * exception if the conversation failed. Thus allows safe re-throwing of the return value.
	 *
	 * @param e the exception to translate
	 * @param exceptionTranslator the {@link PersistenceExceptionTranslator} to be used for translation
	 * @return
	 */
	@SuppressWarnings("all")
	private static RuntimeException potentiallyConvertRuntimeException(RuntimeException e,
			PersistenceExceptionTranslator exceptionTranslator) {

		RuntimeException resolved = exceptionTranslator.translateExceptionIfPossible(e);

		return (resolved != null ? resolved : e);
	}

	@Override
	public <T> T execute(PreparedStatementCreator preparedStatementCreator,
			PreparedStatementCallback<T> preparedStatementCallback) {

		try {

			PreparedStatement preparedStatement = preparedStatementCreator.createPreparedStatement(getSession());
			logDebug("executing [{}]", preparedStatement);

			return preparedStatementCallback.doInPreparedStatement(preparedStatement);
		} catch (DriverException dx) {
			throw translateExceptionIfPossible(dx);
		}
	}

	@Override
	public <T> T execute(String cql, PreparedStatementCallback<T> callback) {
		return execute(new CachedPreparedStatementCreator(logCql(cql)), callback);
	}

	@Override
	public <T> T query(PreparedStatementCreator preparedStatementCreator, ResultSetExtractor<T> resultSetExtractor) {
		return query(preparedStatementCreator, resultSetExtractor, null);
	}

	@Override
	public <T> T query(PreparedStatementCreator preparedStatementCreator, ResultSetExtractor<T> resultSetExtractor,
			QueryOptions queryOptions) {
		return query(preparedStatementCreator, null, resultSetExtractor, queryOptions);
	}

	@Override
	public void query(PreparedStatementCreator preparedStatementCreator, RowCallbackHandler rowCallbackHandler) {
		query(preparedStatementCreator, rowCallbackHandler, null);
	}

	@Override
	public void query(PreparedStatementCreator preparedStatementCreator, RowCallbackHandler rowCallbackHandler,
			QueryOptions queryOptions) {
		query(preparedStatementCreator, null, rowCallbackHandler, queryOptions);
	}

	@Override
	public <T> List<T> query(PreparedStatementCreator preparedStatementCreator, RowMapper<T> rowMapper) {
		return query(preparedStatementCreator, rowMapper, null);
	}

	@Override
	public <T> List<T> query(PreparedStatementCreator preparedStatementCreator, RowMapper<T> rowMapper,
			QueryOptions queryOptions) {
		return query(preparedStatementCreator, null, rowMapper, queryOptions);
	}

	@Override
	public <T> T query(String cql, PreparedStatementBinder preparedStatementBinder,
			ResultSetExtractor<T> resultSetExtractor) {

		return query(cql, preparedStatementBinder, resultSetExtractor, null);
	}

	@Override
	public <T> T query(String cql, PreparedStatementBinder preparedStatementBinder,
			ResultSetExtractor<T> resultSetExtractor, QueryOptions queryOptions) {

		return query(new CachedPreparedStatementCreator(logCql(cql)), preparedStatementBinder, resultSetExtractor,
				queryOptions);
	}

	@Override
	public void query(String cql, PreparedStatementBinder preparedStatementBinder,
			RowCallbackHandler rowCallbackHandler) {

		query(cql, preparedStatementBinder, rowCallbackHandler, null);
	}

	@Override
	public void query(String cql, PreparedStatementBinder preparedStatementBinder, RowCallbackHandler rowCallbackHandler,
			QueryOptions queryOptions) {

		query(new CachedPreparedStatementCreator(logCql(cql)), preparedStatementBinder, rowCallbackHandler, queryOptions);
	}

	@Override
	public <T> List<T> query(String cql, PreparedStatementBinder preparedStatementBinder, RowMapper<T> rowMapper) {
		return query(cql, preparedStatementBinder, rowMapper, null);
	}

	@Override
	public <T> List<T> query(String cql, PreparedStatementBinder preparedStatementBinder, RowMapper<T> rowMapper,
			QueryOptions queryOptions) {

		return query(new CachedPreparedStatementCreator(logCql(cql)), preparedStatementBinder, rowMapper, queryOptions);
	}

	@Override
	public void ingest(String cql, RowIterator rowIterator, WriteOptions options) {

		CachedPreparedStatementCreator cachedPreparedStatementCreator =
				new CachedPreparedStatementCreator(logCql(cql));

		PreparedStatement preparedStatement = addPreparedStatementOptions(
				cachedPreparedStatementCreator.createPreparedStatement(getSession()), options);

		Session session = getSession();

		while (rowIterator.hasNext()) {
			session.executeAsync(preparedStatement.bind(rowIterator.next()));
		}
	}

	@Override
	public void ingest(String cql, RowIterator rowIterator) {
		ingest(cql, rowIterator, null);
	}

	@Override
	public void ingest(String cql, List<List<?>> rows) {
		ingest(cql, rows, null);
	}

	@Override
	public void ingest(String cql, final List<List<?>> rows, WriteOptions writeOptions) {

		Assert.notNull(rows, "Rows must not be null");
		Assert.notEmpty(rows, "Rows must not be empty");

		ingest(cql, new RowIterator() {

			Iterator<List<?>> rowIterator = rows.iterator();

			@Override
			public Object[] next() {
				return rowIterator.next().toArray();
			}

			@Override
			public boolean hasNext() {
				return rowIterator.hasNext();
			}

		}, writeOptions);
	}

	@Override
	public void ingest(String cql, Object[][] rows) {
		ingest(cql, rows, null);
	}

	@Override
	public void ingest(String cql, final Object[][] rows, WriteOptions writeOptions) {

		ingest(cql, new RowIterator() {

			int index = 0;

			@Override
			public boolean hasNext() {
				return (index < rows.length);
			}

			@Override
			public Object[] next() {
				if (!hasNext()) {
					throw new NoSuchElementException("No more elements");
				}

				return rows[index++];
			}
		}, writeOptions);
	}

	@Override
	public void truncate(String tableName) {
		truncate(cqlId(tableName));
	}

	@Override
	public void truncate(CqlIdentifier tableName) {
		doExecute(QueryBuilder.truncate(logCql(tableName.toCql())));
	}

	@Override
	public <T> T query(PreparedStatementCreator preparedStatementCreator, PreparedStatementBinder preparedStatementBinder,
			ResultSetExtractor<T> resultSetExtractor) {

		return query(preparedStatementCreator, preparedStatementBinder, resultSetExtractor, null);
	}

	@Override
	public <T> T query(PreparedStatementCreator preparedStatementCreator,
			final PreparedStatementBinder preparedStatementBinder, final ResultSetExtractor<T> resultSetExtractor,
			final QueryOptions queryOptions) {

		Assert.notNull(resultSetExtractor, "ResultSetExtractor must not be null");

		return execute(preparedStatementCreator, new PreparedStatementCallback<T>() {

			@Override
			public T doInPreparedStatement(PreparedStatement preparedStatement) {

				BoundStatement boundStatement = (preparedStatementBinder != null
						? preparedStatementBinder.bindValues(preparedStatement) : preparedStatement.bind());

				return resultSetExtractor.extractData(doExecute(addQueryOptions(boundStatement, queryOptions)));
			}
		});
	}

	@Override
	public void query(PreparedStatementCreator preparedStatementCreator,
			final PreparedStatementBinder preparedStatementBinder, final RowCallbackHandler rowCallbackHandler,
			final QueryOptions queryOptions) {

		Assert.notNull(rowCallbackHandler, "RowCallbackHandler must not be null");

		execute(preparedStatementCreator, new PreparedStatementCallback<Object>() {

			@Override
			public Object doInPreparedStatement(PreparedStatement preparedStatement) {

				BoundStatement boundStatement = (preparedStatementBinder != null
						? preparedStatementBinder.bindValues(preparedStatement) : preparedStatement.bind());

				process(doExecute(addQueryOptions(boundStatement, queryOptions)), rowCallbackHandler);

				return null;
			}
		});
	}

	@Override
	public void query(PreparedStatementCreator preparedStatementCreator, PreparedStatementBinder preparedStatementBinder,
			RowCallbackHandler rowCallbackHandler) {

		query(preparedStatementCreator, preparedStatementBinder, rowCallbackHandler, null);
	}

	@Override
	public <T> List<T> query(PreparedStatementCreator preparedStatementCreator,
			final PreparedStatementBinder preparedStatementBinder, final RowMapper<T> rowMapper,
			final QueryOptions queryOptions) {

		Assert.notNull(rowMapper, "RowMapper must not be null");

		return execute(preparedStatementCreator, new PreparedStatementCallback<List<T>>() {

			@Override
			public List<T> doInPreparedStatement(PreparedStatement preparedStatement) {

				BoundStatement boundStatement = (preparedStatementBinder != null
						? preparedStatementBinder.bindValues(preparedStatement) : preparedStatement.bind());

				return process(doExecute(addQueryOptions(boundStatement, queryOptions)), rowMapper);
			}
		});
	}

	@Override
	public <T> List<T> query(PreparedStatementCreator preparedStatementCreator,
			PreparedStatementBinder preparedStatementBinder, RowMapper<T> rowMapper) {

		return query(preparedStatementCreator, preparedStatementBinder, rowMapper, null);
	}

	@Override
	public ResultSet execute(final AlterKeyspaceSpecification specification) {

		return execute(new SessionCallback<ResultSet>() {

			@Override
			public ResultSet doInSession(Session session) {
				return session.execute(logCql(AlterKeyspaceCqlGenerator.toCql(specification)));
			}
		});
	}

	@Override
	public ResultSet execute(final CreateKeyspaceSpecification specification) {

		return execute(new SessionCallback<ResultSet>() {

			@Override
			public ResultSet doInSession(Session session) {
				return session.execute(logCql(CreateKeyspaceCqlGenerator.toCql(specification)));
			}
		});
	}

	@Override
	public ResultSet execute(final DropKeyspaceSpecification specification) {

		return execute(new SessionCallback<ResultSet>() {

			@Override
			public ResultSet doInSession(Session session) {
				return session.execute(logCql(DropKeyspaceCqlGenerator.toCql(specification)));
			}
		});
	}

	@Override
	public ResultSet execute(final AlterTableSpecification specification) {

		return execute(new SessionCallback<ResultSet>() {

			@Override
			public ResultSet doInSession(Session session) {
				return session.execute(logCql(AlterTableCqlGenerator.toCql(specification)));
			}
		});
	}

	@Override
	public ResultSet execute(final CreateTableSpecification specification) {

		return execute(new SessionCallback<ResultSet>() {

			@Override
			public ResultSet doInSession(Session session) {
				return session.execute(logCql(CreateTableCqlGenerator.toCql(specification)));
			}
		});
	}

	@Override
	public ResultSet execute(final DropTableSpecification specification) {

		return execute(new SessionCallback<ResultSet>() {

			@Override
			public ResultSet doInSession(Session session) {
				return session.execute(logCql(DropTableCqlGenerator.toCql(specification)));
			}
		});
	}

	@Override
	public ResultSet execute(final CreateIndexSpecification specification) {

		return execute(new SessionCallback<ResultSet>() {

			@Override
			public ResultSet doInSession(Session session) {
				return session.execute(logCql(CreateIndexCqlGenerator.toCql(specification)));
			}
		});
	}

	@Override
	public ResultSet execute(final DropIndexSpecification specification) {

		return execute(new SessionCallback<ResultSet>() {

			@Override
			public ResultSet doInSession(Session session) {
				return session.execute(logCql(DropIndexCqlGenerator.toCql(specification)));
			}
		});
	}

	@Override
	public void execute(Batch batch) {
		doExecute(batch);
	}

	@Override
	public void execute(Delete delete) {
		doExecute(delete);
	}

	@Override
	public void execute(Insert insert) {
		doExecute(insert);
	}

	@Override
	public void execute(Truncate truncate) {
		doExecute(truncate);
	}

	@Override
	public void execute(Update update) {
		doExecute(update);
	}

	@Override
	public long count(String tableName) {
		return count(cqlId(tableName));
	}

	@Override
	public long count(CqlIdentifier tableName) {
		return selectCount(QueryBuilder.select().countAll().from(tableName.toCql()));
	}

	protected long selectCount(final Select select) {

		return query(select, new ResultSetExtractor<Long>() {

			@Override
			public Long extractData(ResultSet resultSet) {

				Row row = resultSet.one();

				if (row == null) {
					throw new InvalidDataAccessApiUsageException(
							String.format("count query [%1$s] did not return any results", select));
				}

				return row.getLong(0);
			}
		});
	}

	@Override
	public ResultSetFuture executeAsynchronously(Batch batch) {
		return doExecuteAsync(batch);
	}

	@Override
	public ResultSetFuture executeAsynchronously(Delete delete) {
		return doExecuteAsync(delete);
	}

	@Override
	public ResultSetFuture executeAsynchronously(Insert insert) {
		return doExecuteAsync(insert);
	}

	@Override
	public ResultSetFuture executeAsynchronously(Truncate truncate) {
		return doExecuteAsync(truncate);
	}

	@Override
	public ResultSetFuture executeAsynchronously(Update update) {
		return doExecuteAsync(update);
	}

	@Override
	public Cancellable executeAsynchronously(Batch batch, AsynchronousQueryListener listener) {
		return doExecuteAsync(batch, listener);
	}

	@Override
	public Cancellable executeAsynchronously(Delete delete, AsynchronousQueryListener listener) {
		return doExecuteAsync(delete, listener);
	}

	@Override
	public Cancellable executeAsynchronously(Insert insert, AsynchronousQueryListener listener) {
		return doExecuteAsync(insert, listener);
	}

	@Override
	public Cancellable executeAsynchronously(Truncate truncate, AsynchronousQueryListener listener) {
		return doExecuteAsync(truncate, listener);
	}

	@Override
	public Cancellable executeAsynchronously(Update update, AsynchronousQueryListener listener) {
		return doExecuteAsync(update, listener);
	}

	@Override
	public ResultSetFuture queryAsynchronously(final Select select) {

		return execute(new SessionCallback<ResultSetFuture>() {
			@Override
			public ResultSetFuture doInSession(Session session) {

				logDebug("async query [{}]", select);
				return session.executeAsync(select);
			}
		});
	}

	@Override
	public Cancellable queryAsynchronously(Select select, AsynchronousQueryListener listener) {
		return queryAsynchronously(select, listener, RUN_RUNNABLE_EXECUTOR);
	}

	@Override
	public Cancellable queryAsynchronously(final Select select, final AsynchronousQueryListener listener,
			final Executor executor) {

		return execute(new SessionCallback<Cancellable>() {

			@Override
			public Cancellable doInSession(Session session) {

				logDebug("async query [{}]", select);

				final ResultSetFuture resultSetFuture = session.executeAsync(select);

				Runnable wrapper = new Runnable() {

					@Override
					public void run() {
						listener.onQueryComplete(resultSetFuture);
					}
				};

				resultSetFuture.addListener(wrapper, executor);

				return new ResultSetFutureCancellable(resultSetFuture);
			}
		});
	}

	@Override
	public Cancellable queryAsynchronously(Select select, Runnable listener) {
		return queryAsynchronously(select, listener, RUN_RUNNABLE_EXECUTOR);
	}

	@Override
	public Cancellable queryAsynchronously(final Select select, final Runnable listener, final Executor executor) {

		return execute(new SessionCallback<Cancellable>() {

			@Override
			public Cancellable doInSession(Session session) {

				logDebug("async query [{}]", select);

				ResultSetFuture resultSetFuture = session.executeAsync(select);
				resultSetFuture.addListener(listener, executor);

				return new ResultSetFutureCancellable(resultSetFuture);
			}
		});
	}

	@Override
	public ResultSet query(Select select) {
		return query(select, RESULT_SET_RETURNING_EXTRACTOR);
	}

	@Override
	public <T> T query(Select select, ResultSetExtractor<T> resultSetExtractor) {

		Assert.notNull(select);

		return resultSetExtractor.extractData(doExecute(select));
	}

	@Override
	public void query(Select select, RowCallbackHandler rowCallbackHandler) {
		process(doExecute(select), rowCallbackHandler);
	}

	@Override
	public <T> List<T> query(Select select, RowMapper<T> rowMapper) {
		return process(doExecute(select), rowMapper);
	}

	@Override
	public <T> T queryForObject(Select select, RowMapper<T> rowMapper) {
		return processOne(doExecute(select), rowMapper);
	}

	@Override
	public <T> T queryForObject(Select select, Class<T> requiredType) {
		return processOne(doExecute(select), requiredType);
	}

	@Override
	public Map<String, Object> queryForMap(Select select) {
		return processMap(doExecute(select));
	}

	@Override
	public <T> List<T> queryForList(Select select, Class<T> elementType) {
		return processList(doExecute(select), elementType);
	}

	@Override
	public List<Map<String, Object>> queryForListOfMap(Select select) {
		return processListOfMap(doExecute(select));
	}

	@Override
	public <T> Cancellable queryForListAsynchronously(Select select, final Class<T> requiredType,
			final QueryForListListener<T> listener) {

		Assert.notNull(select, "Select must not be null");
		Assert.notNull(requiredType, "Required type must not be null");
		Assert.notNull(listener, "Listener must not be null");

		return doExecuteAsync(select, new AsynchronousQueryListener() {

			@Override
			public void onQueryComplete(ResultSetFuture resultSetFuture) {

				try {
					listener.onQueryComplete(processList(resultSetFuture.getUninterruptibly(), requiredType));
				} catch (Exception e) {
					listener.onException(translateExceptionIfPossible(e));
				}
			}
		});
	}

	@Override
	public <T> Cancellable queryForListAsynchronously(String select, final Class<T> requiredType,
			final QueryForListListener<T> listener) {

		Assert.hasText(select, "Select must not be null");
		Assert.notNull(requiredType, "Required type must not be null");
		Assert.notNull(listener, "Listener must not be null");

		return doExecuteAsync(new SimpleStatement(logCql(select)), new AsynchronousQueryListener() {

			@Override
			public void onQueryComplete(ResultSetFuture resultSetFuture) {

				try {
					listener.onQueryComplete(processList(resultSetFuture.getUninterruptibly(), requiredType));
				} catch (Exception e) {
					listener.onException(translateExceptionIfPossible(e));
				}
			}
		});
	}

	@Override
	public Cancellable queryForListOfMapAsynchronously(Select select,
			final QueryForListListener<Map<String, Object>> listener) {

		return doExecuteAsync(select, new AsynchronousQueryListener() {

			@Override
			public void onQueryComplete(ResultSetFuture resultSetFuture) {

				try {
					listener.onQueryComplete(processListOfMap(resultSetFuture.getUninterruptibly()));
				} catch (Exception e) {
					listener.onException(translateExceptionIfPossible(e));
				}
			}
		});
	}

	@Override
	public Cancellable queryForListOfMapAsynchronously(String cql,
			final QueryForListListener<Map<String, Object>> listener) {

		return queryForListOfMapAsynchronously(cql, listener, null);
	}

	@Override
	public Cancellable queryForListOfMapAsynchronously(String cql,
			final QueryForListListener<Map<String, Object>> listener, QueryOptions queryOptions) {

		return doExecuteAsync(new SimpleStatement(logCql(cql)), new AsynchronousQueryListener() {

			@Override
			public void onQueryComplete(ResultSetFuture rsf) {

				try {
					listener.onQueryComplete(processListOfMap(rsf.getUninterruptibly()));
				} catch (Exception e) {
					listener.onException(translateExceptionIfPossible(e));
				}
			}
		}, queryOptions);
	}

	@Override
	public Cancellable queryForMapAsynchronously(String cql, QueryForMapListener listener) {
		return queryForMapAsynchronously(cql, listener, null);
	}

	@Override
	public Cancellable queryForMapAsynchronously(String cql, final QueryForMapListener listener,
			final QueryOptions queryOptions) {

		return doExecuteAsync(new SimpleStatement(logCql(cql)), new AsynchronousQueryListener() {

			@Override
			public void onQueryComplete(ResultSetFuture resultSetFuture) {

				try {
					listener.onQueryComplete(processMap(resultSetFuture.getUninterruptibly()));
				} catch (Exception e) {
					listener.onException(translateExceptionIfPossible(e));
				}
			}
		}, queryOptions);
	}

	@Override
	public Cancellable queryForMapAsynchronously(Select select, final QueryForMapListener listener) {

		return doExecuteAsync(select, new AsynchronousQueryListener() {

			@Override
			public void onQueryComplete(ResultSetFuture resultSetFuture) {

				try {
					listener.onQueryComplete(processMap(resultSetFuture.getUninterruptibly()));
				} catch (Exception e) {
					listener.onException(translateExceptionIfPossible(e));
				}
			}
		});
	}

	@Override
	public <T> Cancellable queryForObjectAsynchronously(Select select, final Class<T> requiredType,
			final QueryForObjectListener<T> listener) {

		return doExecuteAsync(select, new AsynchronousQueryListener() {

			@Override
			public void onQueryComplete(ResultSetFuture resultSetFuture) {

				try {
					listener.onQueryComplete(processOne(resultSetFuture.getUninterruptibly(), requiredType));
				} catch (Exception e) {
					listener.onException(translateExceptionIfPossible(e));
				}
			}
		});
	}

	@Override
	public <T> Cancellable queryForObjectAsynchronously(String cql, Class<T> requiredType,
			QueryForObjectListener<T> listener) {

		return queryForObjectAsynchronously(cql, requiredType, listener, null);
	}

	@Override
	public <T> Cancellable queryForObjectAsynchronously(String cql, final Class<T> requiredType,
			final QueryForObjectListener<T> listener, QueryOptions options) {

		return doExecuteAsync(new SimpleStatement(logCql(cql)), new AsynchronousQueryListener() {

			@Override
			public void onQueryComplete(ResultSetFuture resultSetFuture) {

				try {
					listener.onQueryComplete(processOne(resultSetFuture.getUninterruptibly(), requiredType));
				} catch (Exception e) {
					listener.onException(translateExceptionIfPossible(e));
				}
			}
		}, options);
	}

	@Override
	public <T> Cancellable queryForObjectAsynchronously(String cql, RowMapper<T> rowMapper,
			QueryForObjectListener<T> listener) {

		return queryForObjectAsynchronously(cql, rowMapper, listener, null);
	}

	@Override
	public <T> Cancellable queryForObjectAsynchronously(String cql, final RowMapper<T> rowMapper,
			final QueryForObjectListener<T> listener, QueryOptions options) {

		return doExecuteAsync(new SimpleStatement(logCql(cql)), new AsynchronousQueryListener() {

			@Override
			public void onQueryComplete(ResultSetFuture resultSetFuture) {

				try {
					listener.onQueryComplete(processOne(resultSetFuture.getUninterruptibly(), rowMapper));
				} catch (Exception e) {
					listener.onException(translateExceptionIfPossible(e));
				}
			}
		}, options);
	}

	@Override
	public <T> Cancellable queryForObjectAsynchronously(Select select, final RowMapper<T> rowMapper,
			final QueryForObjectListener<T> listener) {

		return doExecuteAsync(select, new AsynchronousQueryListener() {

			@Override
			public void onQueryComplete(ResultSetFuture resultSetFuture) {

				try {
					listener.onQueryComplete(processOne(resultSetFuture.getUninterruptibly(), rowMapper));
				} catch (Exception e) {
					listener.onException(translateExceptionIfPossible(e));
				}
			}
		});
	}

	@Override
	public ResultSet getResultSetUninterruptibly(ResultSetFuture resultSetFuture) {
		return getResultSetUninterruptibly(resultSetFuture, 0, null);
	}

	@Override
	public ResultSet getResultSetUninterruptibly(ResultSetFuture resultSetFuture, long milliseconds) {
		return getResultSetUninterruptibly(resultSetFuture, milliseconds, TimeUnit.MILLISECONDS);
	}

	@Override
	public ResultSet getResultSetUninterruptibly(ResultSetFuture resultSetFuture, long timeout, TimeUnit timeUnit) {
		try {
			timeUnit = (timeUnit != null ? timeUnit : TimeUnit.MILLISECONDS);

			return (timeout > 0 ? resultSetFuture.getUninterruptibly(timeout, timeUnit)
					: resultSetFuture.getUninterruptibly());
		} catch (Exception e) {
			throw translateExceptionIfPossible(e);
		}
	}
}
