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

import static org.springframework.cassandra.core.cql.CqlIdentifier.cqlId;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.util.Assert;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Truncate;
import com.datastax.driver.core.querybuilder.Update;

/**
 * <b>This is the Central class in the Cassandra core package.</b> It simplifies the use of Cassandra and helps to avoid
 * common errors. It executes the core Cassandra workflow, leaving application code to provide CQL and result
 * extraction. This class execute CQL Queries, provides different ways to extract/map results, and provides Exception
 * translation to the generic, more informative exception hierarchy defined in the <code>org.springframework.dao</code>
 * package.
 * <p>
 * For working with POJOs, use the {@link CassandraDataTemplate}.
 * </p>
 * 
 * @author David Webb
 * @author Matthew Adams
 */
public class CqlTemplate extends CassandraAccessor implements CqlOperations {

	protected static final Logger log = LoggerFactory.getLogger(CqlTemplate.class);

	/**
	 * Add common {@link Statement} options for all types of queries.
	 * 
	 * @param q
	 * @param options
	 * @return the {@link Statement} given.
	 */
	public static Statement addQueryOptions(Statement q, QueryOptions options) {

		if (options == null) {
			return q;
		}

		if (options.getConsistencyLevel() != null) {
			q.setConsistencyLevel(ConsistencyLevelResolver.resolve(options.getConsistencyLevel()));
		}
		if (options.getRetryPolicy() != null) {
			q.setRetryPolicy(RetryPolicyResolver.resolve(options.getRetryPolicy()));
		}

		return q;
	}

	/**
	 * Add common {@link Query} options for Insert queries.
	 * 
	 * @param q
	 * @param options
	 * @return the {@link Query} given.
	 */
	public static Insert addWriteOptions(Insert q, WriteOptions options) {

		if (options == null) {
			return q;
		}

		if (options.getConsistencyLevel() != null) {
			q.setConsistencyLevel(ConsistencyLevelResolver.resolve(options.getConsistencyLevel()));
		}
		if (options.getRetryPolicy() != null) {
			q.setRetryPolicy(RetryPolicyResolver.resolve(options.getRetryPolicy()));
		}
		if (options.getTtl() != null) {
			q.using(QueryBuilder.ttl(options.getTtl()));
		}

		return q;
	}

	/**
	 * Add common {@link Query} options for Update queries.
	 * 
	 * @param q
	 * @param options
	 * @return the {@link Query} given.
	 */
	public static Update addWriteOptions(Update q, WriteOptions options) {

		if (options == null) {
			return q;
		}

		if (options.getConsistencyLevel() != null) {
			q.setConsistencyLevel(ConsistencyLevelResolver.resolve(options.getConsistencyLevel()));
		}
		if (options.getRetryPolicy() != null) {
			q.setRetryPolicy(RetryPolicyResolver.resolve(options.getRetryPolicy()));
		}
		if (options.getTtl() != null) {
			q.using(QueryBuilder.ttl(options.getTtl()));
		}

		return q;
	}

	/**
	 * Add common Query options for all types of queries.
	 * 
	 * @param q
	 * @param optionsByName
	 */
	public static void addPreparedStatementOptions(PreparedStatement s, QueryOptions options) {

		if (options == null) {
			return;
		}

		/*
		 * Add Query Options
		 */
		if (options.getConsistencyLevel() != null) {
			s.setConsistencyLevel(ConsistencyLevelResolver.resolve(options.getConsistencyLevel()));
		}
		if (options.getRetryPolicy() != null) {
			s.setRetryPolicy(RetryPolicyResolver.resolve(options.getRetryPolicy()));
		}

	}

	/**
	 * Blank constructor. You must wire in the Session before use.
	 */
	public CqlTemplate() {
	}

	/**
	 * Constructor used for a basic template configuration
	 * 
	 * @param session must not be {@literal null}.
	 */
	public CqlTemplate(Session session) {
		setSession(session);
	}

	@Override
	public <T> T execute(SessionCallback<T> sessionCallback) throws DataAccessException {
		return doExecute(sessionCallback);
	}

	@Override
	public void execute(String cql) throws DataAccessException {
		execute(cql, (QueryOptions) null);
	}

	@Override
	public void execute(String cql, QueryOptions options) throws DataAccessException {
		doExecute(cql, options);
	}

	@Override
	public void execute(Statement query) throws DataAccessException {
		doExecute(query);
	}

	@Override
	public ResultSetFuture queryAsynchronously(final String cql) {
		return execute(new SessionCallback<ResultSetFuture>() {
			@Override
			public ResultSetFuture doInSession(Session s) throws DataAccessException {
				return s.executeAsync(cql);
			}
		});
	}

	@Override
	public <T> T queryAsynchronously(String cql, ResultSetExtractor<T> rse, Long timeout, TimeUnit timeUnit) {
		return queryAsynchronously(cql, rse, timeout, timeUnit, null);
	}

	@Override
	public <T> T queryAsynchronously(final String cql, final ResultSetExtractor<T> rse, final Long timeout,
			final TimeUnit timeUnit, final QueryOptions options) {
		return rse.extractData(execute(new SessionCallback<ResultSet>() {
			@Override
			public ResultSet doInSession(Session s) throws DataAccessException {
				Statement statement = new SimpleStatement(cql);
				addQueryOptions(statement, options);
				ResultSetFuture rsf = s.executeAsync(statement);
				ResultSet rs = null;
				try {
					rs = rsf.get(timeout, timeUnit);
				} catch (TimeoutException e) {
					throw new QueryTimeoutException("Asyncronous Query Timed Out.", e);
				} catch (InterruptedException e) {
					throw translateExceptionIfPossible(e);
				} catch (ExecutionException e) {
					if (e.getCause() instanceof Exception) {
						throw translateExceptionIfPossible((Exception) e.getCause());
					}
					throw new CassandraUncategorizedDataAccessException("Unknown Throwable", e.getCause());
				}
				return rs;
			}
		}));
	}

	@Override
	public ResultSetFuture queryAsynchronously(final String cql, final QueryOptions options) {
		return execute(new SessionCallback<ResultSetFuture>() {
			@Override
			public ResultSetFuture doInSession(Session s) throws DataAccessException {
				Statement statement = new SimpleStatement(cql);
				addQueryOptions(statement, options);
				return s.executeAsync(statement);
			}
		});
	}

	@Override
	public Cancellable queryAsynchronously(String cql, Runnable listener) {
		return queryAsynchronously(cql, listener, new Executor() {
			@Override
			public void execute(Runnable command) {
				command.run();
			}
		});
	}

	@Override
	public Cancellable queryAsynchronously(String cql, AsynchronousQueryListener listener) {
		return queryAsynchronously(cql, listener, new Executor() {
			@Override
			public void execute(Runnable command) {
				command.run();
			}
		});
	}

	@Override
	public Cancellable queryAsynchronously(String cql, Runnable listener, QueryOptions options) {
		return queryAsynchronously(cql, listener, options, new Executor() {
			@Override
			public void execute(Runnable command) {
				command.run();
			}
		});
	}

	@Override
	public Cancellable queryAsynchronously(String cql, AsynchronousQueryListener listener, QueryOptions options) {
		return queryAsynchronously(cql, listener, options, new Executor() {
			@Override
			public void execute(Runnable command) {
				command.run();
			}
		});
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
	public Cancellable queryAsynchronously(final String cql, final Runnable listener, final QueryOptions options,
			final Executor executor) {
		return execute(new SessionCallback<Cancellable>() {
			@Override
			public Cancellable doInSession(Session s) throws DataAccessException {
				Statement statement = new SimpleStatement(cql);
				addQueryOptions(statement, options);
				ResultSetFuture rsf = s.executeAsync(statement);
				rsf.addListener(listener, executor);
				return new ResultSetFutureCancellable(rsf);
			}
		});
	}

	@Override
	public Cancellable queryAsynchronously(final String cql, final AsynchronousQueryListener listener,
			final QueryOptions options, final Executor executor) {
		return execute(new SessionCallback<Cancellable>() {
			@Override
			public Cancellable doInSession(Session s) throws DataAccessException {
				Statement statement = new SimpleStatement(cql);
				addQueryOptions(statement, options);
				final ResultSetFuture rsf = s.executeAsync(statement);
				Runnable wrapper = new Runnable() {
					@Override
					public void run() {
						listener.onQueryComplete(rsf);
					}
				};
				rsf.addListener(wrapper, executor);
				return new ResultSetFutureCancellable(rsf);
			}
		});
	}

	public <T> T queryAsynchronously(final String cql, ResultSetFutureExtractor<T> rse, final QueryOptions options)
			throws DataAccessException {
		return rse.extractData(execute(new SessionCallback<ResultSetFuture>() {
			@Override
			public ResultSetFuture doInSession(Session s) throws DataAccessException {
				Statement statement = new SimpleStatement(cql);
				addQueryOptions(statement, options);
				return s.executeAsync(statement);
			}
		}));
	}

	public <T> T queryAsynchronously(final String cql, ResultSetFutureExtractor<T> rse) throws DataAccessException {
		return queryAsynchronously(cql, rse, null);
	}

	@Override
	public <T> T query(String cql, ResultSetExtractor<T> rse) throws DataAccessException {
		return query(cql, rse, null);
	}

	@Override
	public <T> T query(String cql, ResultSetExtractor<T> rse, QueryOptions options) throws DataAccessException {
		Assert.notNull(cql);
		ResultSet rs = doExecute(cql, options);
		return rse.extractData(rs);
	}

	@Override
	public void query(String cql, RowCallbackHandler rch, QueryOptions options) throws DataAccessException {
		process(doExecute(cql, options), rch);
	}

	@Override
	public void query(String cql, RowCallbackHandler rch) throws DataAccessException {
		query(cql, rch, null);
	}

	@Override
	public <T> List<T> query(String cql, RowMapper<T> rowMapper, QueryOptions options) throws DataAccessException {
		return process(doExecute(cql, options), rowMapper);
	}

	@Override
	public ResultSet query(String cql) {
		return query(cql, (QueryOptions) null);
	}

	@Override
	public ResultSet query(String cql, QueryOptions options) {

		return query(cql, new ResultSetExtractor<ResultSet>() {

			@Override
			public ResultSet extractData(ResultSet rs) throws DriverException, DataAccessException {
				return rs;
			}
		}, options);
	}

	@Override
	public <T> List<T> query(String cql, RowMapper<T> rowMapper) throws DataAccessException {
		return query(cql, rowMapper, null);
	}

	@Override
	public List<Map<String, Object>> queryForListOfMap(String cql) throws DataAccessException {
		return processListOfMap(doExecute(cql, null));
	}

	@Override
	public <T> List<T> queryForList(String cql, Class<T> elementType) throws DataAccessException {
		return processList(doExecute(cql, null), elementType);
	}

	@Override
	public Map<String, Object> queryForMap(String cql) throws DataAccessException {
		return processMap(doExecute(cql, null));
	}

	@Override
	public <T> T queryForObject(String cql, Class<T> requiredType) throws DataAccessException {
		return processOne(doExecute(cql, null), requiredType);
	}

	@Override
	public <T> T queryForObject(String cql, RowMapper<T> rowMapper) throws DataAccessException {
		return processOne(doExecute(cql, null), rowMapper);
	}

	/**
	 * Execute a command at the Session Level
	 * 
	 * @param callback
	 * @return
	 */
	protected <T> T doExecute(SessionCallback<T> callback) {

		Assert.notNull(callback);

		try {

			return callback.doInSession(getSession());

		} catch (DataAccessException e) {
			throw translateExceptionIfPossible(e);
		}
	}

	protected ResultSet doExecute(String cql) {
		return doExecute(cql, null);
	}

	protected ResultSet doExecute(String cql, QueryOptions options) {
		return doExecute(addQueryOptions(new SimpleStatement(cql), options));
	}

	/**
	 * Execute a command at the Session Level with optional options
	 * 
	 * @param q The query to execute.
	 * @param options The {@link QueryOptions}. May be null.
	 */
	protected ResultSet doExecute(final Statement q) {

		return doExecute(new SessionCallback<ResultSet>() {

			@Override
			public ResultSet doInSession(Session s) throws DataAccessException {

				if (log.isDebugEnabled()) {
					log.debug("executing [{}]", q.toString());
				}

				return s.execute(q);
			}
		});
	}

	protected ResultSetFuture doExecuteAsync(final Statement q) {

		return doExecute(new SessionCallback<ResultSetFuture>() {

			@Override
			public ResultSetFuture doInSession(Session s) throws DataAccessException {

				if (log.isDebugEnabled()) {
					log.debug("asynchronously executing [{}]", q.toString());
				}
				return s.executeAsync(q);
			}
		});
	}

	protected Cancellable doExecuteAsync(final Statement q, final AsynchronousQueryListener listener) {
		return doExecuteAsync(q, listener, null);
	}

	protected Cancellable doExecuteAsync(final Statement q, final AsynchronousQueryListener listener,
			final QueryOptions options) {

		return doExecute(new SessionCallback<Cancellable>() {

			@Override
			public Cancellable doInSession(Session s) throws DataAccessException {

				if (log.isDebugEnabled()) {
					log.debug("asynchronously executing [{}]", q.toString());
				}

				if (options != null) {
					addQueryOptions(q, options);
				}

				final ResultSetFuture rsf = s.executeAsync(q);

				if (listener != null) {
					rsf.addListener(new Runnable() {

						@Override
						public void run() {
							listener.onQueryComplete(rsf);
						}
					}, new Executor() {

						@Override
						public void execute(Runnable command) {
							command.run();
						}
					});
				}
				return new ResultSetFutureCancellable(rsf);
			}
		});
	}

	/**
	 * @param row
	 * @return
	 */
	protected Object firstColumnToObject(Row row) {
		ColumnDefinitions cols = row.getColumnDefinitions();
		if (cols.size() == 0) {
			return null;
		}
		return cols.getType(0).deserialize(row.getBytesUnsafe(0), ProtocolVersion.NEWEST_SUPPORTED);
	}

	/**
	 * @param row
	 * @return
	 */
	protected Map<String, Object> toMap(Row row) {
		if (row == null) {
			return null;
		}

		ColumnDefinitions cols = row.getColumnDefinitions();
		Map<String, Object> map = new HashMap<String, Object>(cols.size());

		for (Definition def : cols.asList()) {
			String name = def.getName();
			map.put(name, def.getType().deserialize(row.getBytesUnsafe(name), ProtocolVersion.NEWEST_SUPPORTED));
		}

		return map;
	}

	@Override
	public List<RingMember> describeRing() throws DataAccessException {
		return new ArrayList<RingMember>(describeRing(new RingMemberHostMapper()));
	}

	/**
	 * Pulls the list of Hosts for the current Session
	 * 
	 * @return
	 */
	protected Set<Host> getHosts() {

		return doExecute(new SessionCallback<Set<Host>>() {

			@Override
			public Set<Host> doInSession(Session s) throws DataAccessException {
				return s.getCluster().getMetadata().getAllHosts();
			}
		});
	}

	@Override
	public <T> Collection<T> describeRing(HostMapper<T> hostMapper) throws DataAccessException {
		Set<Host> hosts = getHosts();
		return hostMapper.mapHosts(hosts);
	}

	@Override
	public ResultSetFuture executeAsynchronously(String cql) throws DataAccessException {
		return executeAsynchronously(cql, (QueryOptions) null);
	}

	@Override
	public ResultSetFuture executeAsynchronously(final String cql, QueryOptions options) throws DataAccessException {
		return doExecuteAsync(addQueryOptions(new SimpleStatement(cql), options));
	}

	@Override
	public Cancellable executeAsynchronously(String cql, Runnable listener) throws DataAccessException {
		return executeAsynchronously(cql, listener, new Executor() {
			@Override
			public void execute(Runnable command) {
				command.run();
			}
		});
	}

	@Override
	public Cancellable executeAsynchronously(final String cql, final Runnable listener, final Executor executor)
			throws DataAccessException {

		return execute(new SessionCallback<Cancellable>() {
			@Override
			public Cancellable doInSession(Session s) throws DataAccessException {
				Statement statement = new SimpleStatement(cql);
				final ResultSetFuture rsf = s.executeAsync(statement);
				rsf.addListener(listener, executor);
				return new ResultSetFutureCancellable(rsf);
			}
		});
	}

	@Override
	public Cancellable executeAsynchronously(String cql, AsynchronousQueryListener listener) throws DataAccessException {

		return executeAsynchronously(cql, listener, new Executor() {
			@Override
			public void execute(Runnable command) {
				command.run();
			}
		});
	}

	@Override
	public Cancellable executeAsynchronously(final String cql, final AsynchronousQueryListener listener,
			final Executor executor) throws DataAccessException {

		return execute(new SessionCallback<Cancellable>() {
			@Override
			public Cancellable doInSession(Session s) throws DataAccessException {
				Statement statement = new SimpleStatement(cql);
				final ResultSetFuture rsf = s.executeAsync(statement);
				Runnable wrapper = new Runnable() {
					@Override
					public void run() {
						listener.onQueryComplete(rsf);
					}
				};
				rsf.addListener(wrapper, executor);
				return new ResultSetFutureCancellable(rsf);
			}
		});
	}

	@Override
	public ResultSetFuture executeAsynchronously(Statement query) throws DataAccessException {
		return doExecuteAsync(query);
	}

	@Override
	public Cancellable executeAsynchronously(Statement query, Runnable listener) throws DataAccessException {
		return executeAsynchronously(query, listener, new Executor() {
			@Override
			public void execute(Runnable command) {
				command.run();
			}
		});
	}

	@Override
	public Cancellable executeAsynchronously(Statement query, AsynchronousQueryListener listener)
			throws DataAccessException {
		return executeAsynchronously(query, listener, new Executor() {
			@Override
			public void execute(Runnable command) {
				command.run();
			}
		});
	}

	@Override
	public Cancellable executeAsynchronously(final Statement query, final Runnable listener, final Executor executor)
			throws DataAccessException {
		return execute(new SessionCallback<Cancellable>() {
			@Override
			public Cancellable doInSession(Session s) throws DataAccessException {
				final ResultSetFuture rsf = s.executeAsync(query);
				rsf.addListener(listener, executor);
				return new ResultSetFutureCancellable(rsf);
			}
		});
	}

	@Override
	public Cancellable executeAsynchronously(final Statement query, final AsynchronousQueryListener listener,
			final Executor executor) throws DataAccessException {

		return execute(new SessionCallback<Cancellable>() {
			@Override
			public Cancellable doInSession(Session s) throws DataAccessException {
				final ResultSetFuture rsf = s.executeAsync(query);
				if (listener != null) {
					Runnable wrapper = new Runnable() {
						@Override
						public void run() {
							listener.onQueryComplete(rsf);
						}
					};
					rsf.addListener(wrapper, executor);
				}
				return new ResultSetFutureCancellable(rsf);
			}
		});
	}

	@Override
	public void process(ResultSet resultSet, RowCallbackHandler rch) throws DataAccessException {
		try {
			for (Row row : resultSet.all()) {
				rch.processRow(row);
			}
		} catch (DriverException dx) {
			throw translateExceptionIfPossible(dx);
		}
	}

	@Override
	public <T> List<T> process(ResultSet resultSet, RowMapper<T> rowMapper) throws DataAccessException {
		List<T> mappedRows = new ArrayList<T>();
		try {
			int i = 0;
			for (Row row : resultSet.all()) {
				mappedRows.add(rowMapper.mapRow(row, i++));
			}
		} catch (DriverException dx) {
			throw translateExceptionIfPossible(dx);
		}
		return mappedRows;
	}

	@Override
	public <T> T processOne(ResultSet resultSet, RowMapper<T> rowMapper) throws DataAccessException {
		T row = null;
		Assert.notNull(resultSet, "ResultSet cannot be null");
		try {
			List<Row> rows = resultSet.all();
			Assert.notNull(rows, "null row list returned from query");
			Assert.isTrue(rows.size() == 1, "row list has " + rows.size() + " rows instead of one");
			row = rowMapper.mapRow(rows.get(0), 0);
		} catch (DriverException dx) {
			throw translateExceptionIfPossible(dx);
		}
		return row;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T processOne(ResultSet resultSet, Class<T> requiredType) throws DataAccessException {
		if (resultSet == null) {
			return null;
		}
		Row row = resultSet.one();
		if (row == null) {
			return null;
		}
		return (T) firstColumnToObject(row);
	}

	@Override
	public Map<String, Object> processMap(ResultSet resultSet) throws DataAccessException {
		if (resultSet == null) {
			return null;
		}
		return toMap(resultSet.one());
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> List<T> processList(ResultSet resultSet, Class<T> elementType) throws DataAccessException {
		List<Row> rows = resultSet.all();
		List<T> list = new ArrayList<T>(rows.size());
		for (Row row : rows) {
			list.add((T) firstColumnToObject(row));
		}
		return list;
	}

	@Override
	public List<Map<String, Object>> processListOfMap(ResultSet resultSet) throws DataAccessException {
		List<Row> rows = resultSet.all();
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>(rows.size());
		for (Row row : rows) {
			list.add(toMap(row));
		}
		return list;
	}

	/**
	 * Attempt to translate a Runtime Exception to a Spring Data Exception
	 * 
	 * @param ex
	 * @return
	 */
	protected RuntimeException translateExceptionIfPossible(RuntimeException ex) {
		RuntimeException resolved = getExceptionTranslator().translateExceptionIfPossible(ex);
		return resolved == null ? ex : resolved;
	}

	protected RuntimeException translateExceptionIfPossible(Exception ex) {
		if (ex instanceof RuntimeException) {
			return translateExceptionIfPossible((RuntimeException) ex);
		}
		return new CassandraUncategorizedDataAccessException("Caught Uncategorized Exception", ex);
	}

	@Override
	public <T> T execute(PreparedStatementCreator psc, PreparedStatementCallback<T> action) {

		try {
			PreparedStatement ps = psc.createPreparedStatement(getSession());
			return action.doInPreparedStatement(ps);
		} catch (DriverException dx) {
			throw translateExceptionIfPossible(dx);
		}
	}

	@Override
	public <T> T execute(String cql, PreparedStatementCallback<T> action) {
		return execute(new CachedPreparedStatementCreator(cql), action);
	}

	@Override
	public <T> T query(PreparedStatementCreator psc, ResultSetExtractor<T> rse, QueryOptions options)
			throws DataAccessException {
		return query(psc, null, rse, options);
	}

	@Override
	public <T> T query(PreparedStatementCreator psc, ResultSetExtractor<T> rse) throws DataAccessException {
		return query(psc, rse, null);
	}

	@Override
	public void query(PreparedStatementCreator psc, RowCallbackHandler rch, QueryOptions options)
			throws DataAccessException {
		query(psc, null, rch, options);
	}

	@Override
	public void query(PreparedStatementCreator psc, RowCallbackHandler rch) throws DataAccessException {
		query(psc, rch, null);
	}

	@Override
	public <T> List<T> query(PreparedStatementCreator psc, RowMapper<T> rowMapper, QueryOptions options)
			throws DataAccessException {
		return query(psc, null, rowMapper, options);
	}

	@Override
	public <T> List<T> query(PreparedStatementCreator psc, RowMapper<T> rowMapper) throws DataAccessException {
		return query(psc, rowMapper, null);
	}

	@Override
	public <T> T query(String cql, PreparedStatementBinder psb, ResultSetExtractor<T> rse, QueryOptions options)
			throws DataAccessException {
		return query(new CachedPreparedStatementCreator(cql), psb, rse, options);
	}

	@Override
	public <T> T query(String cql, PreparedStatementBinder psb, ResultSetExtractor<T> rse) throws DataAccessException {
		return query(cql, psb, rse, null);
	}

	@Override
	public void query(String cql, PreparedStatementBinder psb, RowCallbackHandler rch, QueryOptions options)
			throws DataAccessException {
		query(new CachedPreparedStatementCreator(cql), psb, rch, options);
	}

	@Override
	public void query(String cql, PreparedStatementBinder psb, RowCallbackHandler rch) throws DataAccessException {
		query(cql, psb, rch, null);
	}

	@Override
	public <T> List<T> query(String cql, PreparedStatementBinder psb, RowMapper<T> rowMapper, QueryOptions options)
			throws DataAccessException {
		return query(new CachedPreparedStatementCreator(cql), psb, rowMapper, options);
	}

	@Override
	public <T> List<T> query(String cql, PreparedStatementBinder psb, RowMapper<T> rowMapper) throws DataAccessException {
		return query(cql, psb, rowMapper, null);
	}

	@Override
	public void ingest(String cql, RowIterator rowIterator, WriteOptions options) {

		CachedPreparedStatementCreator cpsc = new CachedPreparedStatementCreator(cql);

		PreparedStatement preparedStatement = cpsc.createPreparedStatement(getSession());
		addPreparedStatementOptions(preparedStatement, options);

		Session s = getSession();
		while (rowIterator.hasNext()) {
			s.executeAsync(preparedStatement.bind(rowIterator.next()));
		}
	}

	@Override
	public void ingest(String cql, RowIterator rowIterator) {
		ingest(cql, rowIterator, null);
	}

	@Override
	public void ingest(String cql, final List<List<?>> rows, WriteOptions options) {

		Assert.notNull(rows);
		Assert.notEmpty(rows);

		ingest(cql, new RowIterator() {

			Iterator<List<?>> i = rows.iterator();

			@Override
			public Object[] next() {
				return i.next().toArray();
			}

			@Override
			public boolean hasNext() {
				return i.hasNext();
			}

		}, options);

	}

	@Override
	public void ingest(String cql, List<List<?>> rows) {
		ingest(cql, rows, null);
	}

	@Override
	public void ingest(String cql, final Object[][] rows, WriteOptions options) {

		ingest(cql, new RowIterator() {

			int index = 0;

			@Override
			public Object[] next() {
				return rows[index++];
			}

			@Override
			public boolean hasNext() {
				return index < rows.length;
			}

		}, options);
	}

	@Override
	public void ingest(String cql, final Object[][] rows) {
		ingest(cql, rows, null);
	}

	@Override
	public void truncate(String tableName) throws DataAccessException {
		truncate(cqlId(tableName));
	}

	@Override
	public void truncate(CqlIdentifier tableName) throws DataAccessException {
		Truncate truncate = QueryBuilder.truncate(tableName.toCql());
		doExecute(truncate);
	}

	@Override
	public <T> T query(PreparedStatementCreator psc, final PreparedStatementBinder psb, final ResultSetExtractor<T> rse,
			final QueryOptions options) throws DataAccessException {

		Assert.notNull(rse, "ResultSetExtractor must not be null");
		logger.debug("Executing prepared CQL query");

		return execute(psc, new PreparedStatementCallback<T>() {
			@Override
			public T doInPreparedStatement(PreparedStatement ps) throws DriverException {
				ResultSet rs = null;
				BoundStatement bs = null;
				if (psb != null) {
					bs = psb.bindValues(ps);
				} else {
					bs = ps.bind();
				}
				rs = doExecute(addQueryOptions(bs, options));
				return rse.extractData(rs);
			}
		});
	}

	@Override
	public <T> T query(PreparedStatementCreator psc, PreparedStatementBinder psb, ResultSetExtractor<T> rse)
			throws DataAccessException {
		return query(psc, psb, rse, null);
	}

	@Override
	public void query(PreparedStatementCreator psc, final PreparedStatementBinder psb, final RowCallbackHandler rch,
			final QueryOptions options) throws DataAccessException {

		Assert.notNull(rch, "RowCallbackHandler must not be null");
		logger.debug("Executing prepared CQL query");

		execute(psc, new PreparedStatementCallback<Object>() {
			@Override
			public Object doInPreparedStatement(PreparedStatement ps) throws DriverException {
				ResultSet rs = null;
				BoundStatement bs = null;
				if (psb != null) {
					bs = psb.bindValues(ps);
				} else {
					bs = ps.bind();
				}
				rs = doExecute(addQueryOptions(bs, options));
				process(rs, rch);
				return null;
			}
		});
	}

	@Override
	public void query(PreparedStatementCreator psc, PreparedStatementBinder psb, RowCallbackHandler rch)
			throws DataAccessException {
		query(psc, psb, rch, null);
	}

	@Override
	public <T> List<T> query(PreparedStatementCreator psc, final PreparedStatementBinder psb,
			final RowMapper<T> rowMapper, final QueryOptions options) throws DataAccessException {
		Assert.notNull(rowMapper, "RowMapper must not be null");
		logger.debug("Executing prepared CQL query");

		return execute(psc, new PreparedStatementCallback<List<T>>() {
			@Override
			public List<T> doInPreparedStatement(PreparedStatement ps) throws DriverException {
				ResultSet rs = null;
				BoundStatement bs = null;
				if (psb != null) {
					bs = psb.bindValues(ps);
				} else {
					bs = ps.bind();
				}
				rs = doExecute(addQueryOptions(bs, options));

				return process(rs, rowMapper);
			}
		});
	}

	@Override
	public <T> List<T> query(PreparedStatementCreator psc, PreparedStatementBinder psb, RowMapper<T> rowMapper)
			throws DataAccessException {
		return query(psc, psb, rowMapper, null);
	}

	@Override
	public ResultSet execute(final DropTableSpecification specification) {

		return execute(new SessionCallback<ResultSet>() {

			@Override
			public ResultSet doInSession(Session s) throws DataAccessException {
				return s.execute(DropTableCqlGenerator.toCql(specification));
			}
		});
	}

	@Override
	public ResultSet execute(final CreateTableSpecification specification) {

		return execute(new SessionCallback<ResultSet>() {

			@Override
			public ResultSet doInSession(Session s) throws DataAccessException {
				return s.execute(CreateTableCqlGenerator.toCql(specification));
			}
		});
	}

	@Override
	public ResultSet execute(final AlterTableSpecification specification) {

		return execute(new SessionCallback<ResultSet>() {

			@Override
			public ResultSet doInSession(Session s) throws DataAccessException {
				return s.execute(AlterTableCqlGenerator.toCql(specification));
			}
		});
	}

	@Override
	public ResultSet execute(final DropKeyspaceSpecification specification) {

		return execute(new SessionCallback<ResultSet>() {

			@Override
			public ResultSet doInSession(Session s) throws DataAccessException {
				return s.execute(DropKeyspaceCqlGenerator.toCql(specification));
			}
		});
	}

	@Override
	public ResultSet execute(final CreateKeyspaceSpecification specification) {

		return execute(new SessionCallback<ResultSet>() {

			@Override
			public ResultSet doInSession(Session s) throws DataAccessException {
				return s.execute(CreateKeyspaceCqlGenerator.toCql(specification));
			}
		});
	}

	@Override
	public ResultSet execute(final AlterKeyspaceSpecification specification) {

		return execute(new SessionCallback<ResultSet>() {

			@Override
			public ResultSet doInSession(Session s) throws DataAccessException {
				return s.execute(AlterKeyspaceCqlGenerator.toCql(specification));
			}
		});
	}

	@Override
	public ResultSet execute(final DropIndexSpecification specification) {

		return execute(new SessionCallback<ResultSet>() {

			@Override
			public ResultSet doInSession(Session s) throws DataAccessException {
				return s.execute(DropIndexCqlGenerator.toCql(specification));
			}
		});
	}

	@Override
	public ResultSet execute(final CreateIndexSpecification specification) {

		return execute(new SessionCallback<ResultSet>() {

			@Override
			public ResultSet doInSession(Session s) throws DataAccessException {
				return s.execute(CreateIndexCqlGenerator.toCql(specification));
			}
		});
	}

	@Override
	public void execute(Delete delete) throws DataAccessException {
		doExecute(delete);
	}

	@Override
	public void execute(Insert insert) throws DataAccessException {
		doExecute(insert);
	}

	@Override
	public void execute(Update update) throws DataAccessException {
		doExecute(update);
	}

	@Override
	public void execute(Batch batch) throws DataAccessException {
		doExecute(batch);
	}

	@Override
	public void execute(Truncate truncate) throws DataAccessException {
		doExecute(truncate);
	}

	@Override
	public long count(CqlIdentifier tableName) {
		return selectCount(QueryBuilder.select().countAll().from(tableName.toCql()));
	}

	@Override
	public long count(String tableName) {
		return count(cqlId(tableName));
	}

	protected long selectCount(Select select) {

		return query(select, new ResultSetExtractor<Long>() {

			@Override
			public Long extractData(ResultSet rs) throws DriverException, DataAccessException {

				Row row = rs.one();
				if (row == null) {
					throw new InvalidDataAccessApiUsageException(String.format("count query did not return any results"));
				}

				return row.getLong(0);
			}
		});
	}

	@Override
	public ResultSetFuture executeAsynchronously(Truncate truncate) throws DataAccessException {
		return doExecuteAsync(truncate);
	}

	@Override
	public ResultSetFuture executeAsynchronously(Delete delete) throws DataAccessException {
		return doExecuteAsync(delete);
	}

	@Override
	public ResultSetFuture executeAsynchronously(Insert insert) throws DataAccessException {
		return doExecuteAsync(insert);
	}

	@Override
	public ResultSetFuture executeAsynchronously(Update update) throws DataAccessException {
		return doExecuteAsync(update);
	}

	@Override
	public ResultSetFuture executeAsynchronously(Batch batch) throws DataAccessException {
		return doExecuteAsync(batch);
	}

	@Override
	public Cancellable executeAsynchronously(Truncate truncate, AsynchronousQueryListener listener)
			throws DataAccessException {
		return doExecuteAsync(truncate, listener);
	}

	@Override
	public Cancellable executeAsynchronously(Delete delete, AsynchronousQueryListener listener)
			throws DataAccessException {
		return doExecuteAsync(delete, listener);
	}

	@Override
	public Cancellable executeAsynchronously(Insert insert, AsynchronousQueryListener listener)
			throws DataAccessException {
		return doExecuteAsync(insert, listener);
	}

	@Override
	public Cancellable executeAsynchronously(Update update, AsynchronousQueryListener listener)
			throws DataAccessException {
		return doExecuteAsync(update, listener);
	}

	@Override
	public Cancellable executeAsynchronously(Batch batch, AsynchronousQueryListener listener) throws DataAccessException {
		return doExecuteAsync(batch, listener);
	}

	@Override
	public ResultSetFuture queryAsynchronously(final Select select) {
		return execute(new SessionCallback<ResultSetFuture>() {
			@Override
			public ResultSetFuture doInSession(Session s) throws DataAccessException {
				return s.executeAsync(select);
			}
		});
	}

	@Override
	public Cancellable queryAsynchronously(Select select, Runnable listener) {
		return queryAsynchronously(select, listener, new Executor() {
			@Override
			public void execute(Runnable command) {
				command.run();
			}
		});
	}

	@Override
	public Cancellable queryAsynchronously(Select select, AsynchronousQueryListener listener) {
		return queryAsynchronously(select, listener, new Executor() {
			@Override
			public void execute(Runnable command) {
				command.run();
			}
		});
	}

	@Override
	public Cancellable queryAsynchronously(final Select select, final AsynchronousQueryListener listener,
			final Executor executor) {

		return execute(new SessionCallback<Cancellable>() {
			@Override
			public Cancellable doInSession(Session s) throws DataAccessException {
				final ResultSetFuture rsf = s.executeAsync(select);
				Runnable wrapper = new Runnable() {
					@Override
					public void run() {
						listener.onQueryComplete(rsf);
					}
				};
				rsf.addListener(wrapper, executor);
				return new ResultSetFutureCancellable(rsf);
			}
		});
	}

	@Override
	public Cancellable queryAsynchronously(final Select select, final Runnable listener, final Executor executor) {
		return execute(new SessionCallback<Cancellable>() {
			@Override
			public Cancellable doInSession(Session s) throws DataAccessException {
				ResultSetFuture rsf = s.executeAsync(select);
				rsf.addListener(listener, executor);
				return new ResultSetFutureCancellable(rsf);
			}
		});
	}

	@Override
	public ResultSet query(Select select) {
		return query(select, new ResultSetExtractor<ResultSet>() {
			@Override
			public ResultSet extractData(ResultSet rs) throws DriverException, DataAccessException {
				return rs;
			}
		});
	}

	@Override
	public <T> T query(Select select, ResultSetExtractor<T> rse) throws DataAccessException {
		Assert.notNull(select);
		ResultSet rs = doExecute(select);
		return rse.extractData(rs);
	}

	@Override
	public void query(Select select, RowCallbackHandler rch) throws DataAccessException {
		process(doExecute(select), rch);
	}

	@Override
	public <T> List<T> query(Select select, RowMapper<T> rowMapper) throws DataAccessException {
		return process(doExecute(select), rowMapper);
	}

	@Override
	public <T> T queryForObject(Select select, RowMapper<T> rowMapper) throws DataAccessException {
		return processOne(doExecute(select), rowMapper);
	}

	@Override
	public <T> T queryForObject(Select select, Class<T> requiredType) throws DataAccessException {
		return processOne(doExecute(select), requiredType);
	}

	@Override
	public Map<String, Object> queryForMap(Select select) throws DataAccessException {
		return processMap(doExecute(select));
	}

	@Override
	public <T> List<T> queryForList(Select select, Class<T> elementType) throws DataAccessException {
		return processList(doExecute(select), elementType);
	}

	@Override
	public List<Map<String, Object>> queryForListOfMap(Select select) throws DataAccessException {
		return processListOfMap(doExecute(select));
	}

	@Override
	public <T> Cancellable queryForListAsynchronously(Select select, final Class<T> elementType,
			final QueryForListListener<T> listener) throws DataAccessException {

		Assert.notNull(select);
		Assert.notNull(elementType);
		Assert.notNull(listener);

		return doExecuteAsync(select, new AsynchronousQueryListener() {

			@Override
			public void onQueryComplete(ResultSetFuture rsf) {
				try {
					listener.onQueryComplete(processList(rsf.getUninterruptibly(), elementType));
				} catch (Exception e) {
					listener.onException(translateExceptionIfPossible(e));
				}
			}
		});
	}

	@Override
	public <T> Cancellable queryForListAsynchronously(String select, final Class<T> elementType,
			final QueryForListListener<T> listener) throws DataAccessException {

		Assert.hasText(select);
		Assert.notNull(elementType);
		Assert.notNull(listener);

		return doExecuteAsync(new SimpleStatement(select), new AsynchronousQueryListener() {

			@Override
			public void onQueryComplete(ResultSetFuture rsf) {
				try {
					listener.onQueryComplete(processList(rsf.getUninterruptibly(), elementType));
				} catch (Exception e) {
					listener.onException(translateExceptionIfPossible(e));
				}
			}
		});
	}

	@Override
	public Cancellable queryForListOfMapAsynchronously(Select select,
			final QueryForListListener<Map<String, Object>> listener) throws DataAccessException {

		return doExecuteAsync(select, new AsynchronousQueryListener() {

			@Override
			public void onQueryComplete(ResultSetFuture rsf) {
				try {
					listener.onQueryComplete(processListOfMap(rsf.getUninterruptibly()));
				} catch (Exception e) {
					listener.onException(translateExceptionIfPossible(e));
				}
			}
		});
	}

	@Override
	public Cancellable queryForListOfMapAsynchronously(String cql,
			final QueryForListListener<Map<String, Object>> listener) throws DataAccessException {
		return queryForListOfMapAsynchronously(cql, listener, null);
	}

	@Override
	public Cancellable queryForListOfMapAsynchronously(String cql,
			final QueryForListListener<Map<String, Object>> listener, QueryOptions options) throws DataAccessException {

		return doExecuteAsync(new SimpleStatement(cql), new AsynchronousQueryListener() {

			@Override
			public void onQueryComplete(ResultSetFuture rsf) {
				try {
					listener.onQueryComplete(processListOfMap(rsf.getUninterruptibly()));
				} catch (Exception e) {
					listener.onException(translateExceptionIfPossible(e));
				}
			}
		}, options);
	}

	@Override
	public Cancellable queryForMapAsynchronously(String cql, QueryForMapListener listener) throws DataAccessException {
		return queryForMapAsynchronously(cql, listener, null);
	}

	@Override
	public Cancellable queryForMapAsynchronously(String cql, final QueryForMapListener listener,
			final QueryOptions options) throws DataAccessException {

		return doExecuteAsync(new SimpleStatement(cql), new AsynchronousQueryListener() {

			@Override
			public void onQueryComplete(ResultSetFuture rsf) {
				try {
					listener.onQueryComplete(processMap(rsf.getUninterruptibly()));
				} catch (Exception e) {
					listener.onException(translateExceptionIfPossible(e));
				}
			}
		}, options);
	}

	@Override
	public Cancellable queryForMapAsynchronously(Select select, final QueryForMapListener listener)
			throws DataAccessException {

		return doExecuteAsync(select, new AsynchronousQueryListener() {

			@Override
			public void onQueryComplete(ResultSetFuture rsf) {
				try {
					listener.onQueryComplete(processMap(rsf.getUninterruptibly()));
				} catch (Exception e) {
					listener.onException(translateExceptionIfPossible(e));
				}
			}
		});
	}

	@Override
	public <T> Cancellable queryForObjectAsynchronously(Select select, final Class<T> requiredType,
			final QueryForObjectListener<T> listener) throws DataAccessException {

		return doExecuteAsync(select, new AsynchronousQueryListener() {

			@Override
			public void onQueryComplete(ResultSetFuture rsf) {
				try {
					listener.onQueryComplete(processOne(rsf.getUninterruptibly(), requiredType));
				} catch (Exception e) {
					listener.onException(translateExceptionIfPossible(e));
				}
			}
		});
	}

	@Override
	public <T> Cancellable queryForObjectAsynchronously(String cql, Class<T> requiredType,
			QueryForObjectListener<T> listener) throws DataAccessException {
		return queryForObjectAsynchronously(cql, requiredType, listener, null);
	}

	@Override
	public <T> Cancellable queryForObjectAsynchronously(String cql, final Class<T> requiredType,
			final QueryForObjectListener<T> listener, QueryOptions options) throws DataAccessException {

		return doExecuteAsync(new SimpleStatement(cql), new AsynchronousQueryListener() {

			@Override
			public void onQueryComplete(ResultSetFuture rsf) {
				try {
					listener.onQueryComplete(processOne(rsf.getUninterruptibly(), requiredType));
				} catch (Exception e) {
					listener.onException(translateExceptionIfPossible(e));
				}
			}
		}, options);
	}

	@Override
	public <T> Cancellable queryForObjectAsynchronously(String cql, RowMapper<T> rowMapper,
			QueryForObjectListener<T> listener) throws DataAccessException {
		return queryForObjectAsynchronously(cql, rowMapper, listener, null);
	}

	@Override
	public <T> Cancellable queryForObjectAsynchronously(String cql, final RowMapper<T> rowMapper,
			final QueryForObjectListener<T> listener, QueryOptions options) throws DataAccessException {

		return doExecuteAsync(new SimpleStatement(cql), new AsynchronousQueryListener() {

			@Override
			public void onQueryComplete(ResultSetFuture rsf) {
				try {
					listener.onQueryComplete(processOne(rsf.getUninterruptibly(), rowMapper));
				} catch (Exception e) {
					listener.onException(translateExceptionIfPossible(e));
				}
			}
		}, options);
	}

	@Override
	public <T> Cancellable queryForObjectAsynchronously(Select select, final RowMapper<T> rowMapper,
			final QueryForObjectListener<T> listener) throws DataAccessException {

		return doExecuteAsync(select, new AsynchronousQueryListener() {

			@Override
			public void onQueryComplete(ResultSetFuture rsf) {
				try {
					listener.onQueryComplete(processOne(rsf.getUninterruptibly(), rowMapper));
				} catch (Exception e) {
					listener.onException(translateExceptionIfPossible(e));
				}
			}
		});
	}

	@Override
	public ResultSet getResultSetUninterruptibly(ResultSetFuture rsf) {
		return getResultSetUninterruptibly(rsf, 0, null);
	}

	@Override
	public ResultSet getResultSetUninterruptibly(ResultSetFuture rsf, long millis) {
		return getResultSetUninterruptibly(rsf, millis, TimeUnit.MILLISECONDS);
	}

	@Override
	public ResultSet getResultSetUninterruptibly(ResultSetFuture rsf, long timeout, TimeUnit unit) {
		try {
			return timeout <= 0 ? rsf.getUninterruptibly() : rsf.getUninterruptibly(timeout,
					unit == null ? TimeUnit.MILLISECONDS : unit);
		} catch (Exception x) {
			throw translateExceptionIfPossible(x);
		}
	}
}
