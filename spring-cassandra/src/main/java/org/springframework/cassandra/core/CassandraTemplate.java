/*
 * Copyright 2011-2013 the original author or authors.
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
package org.springframework.cassandra.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.cassandra.support.CassandraAccessor;
import org.springframework.dao.DataAccessException;
import org.springframework.util.Assert;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Truncate;

/**
 * <b>This is the Central class in the Cassandra core package.</b> It simplifies the use of Cassandra and helps to avoid
 * common errors. It executes the core Cassandra workflow, leaving application code to provide CQL and result
 * extraction. This class execute CQL Queries, provides different ways to extract/map results, and provides Exception
 * translation to the generic, more informative exception hierarchy defined in the <code>org.springframework.dao</code>
 * package.
 * 
 * <p>
 * For working with POJOs, use the {@link CassandraDataTemplate}.
 * </p>
 * 
 * @author David Webb
 * @author Matthew Adams
 */
public class CassandraTemplate extends CassandraAccessor implements CassandraOperations {

	/**
	 * Blank constructor. You must wire in the Session before use.
	 * 
	 */
	public CassandraTemplate() {
	}

	/**
	 * Constructor used for a basic template configuration
	 * 
	 * @param session must not be {@literal null}.
	 */
	public CassandraTemplate(Session session) {
		setSession(session);
		afterPropertiesSet();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#execute(org.springframework.data.cassandra.core.SessionCallback)
	 */
	@Override
	public <T> T execute(SessionCallback<T> sessionCallback) throws DataAccessException {
		return doExecute(sessionCallback);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#execute(java.lang.String)
	 */
	@Override
	public void execute(final String cql) throws DataAccessException {
		doExecute(cql, Collections.<String, Object> emptyMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#queryAsynchronously(java.lang.String, org.springframework.cassandra.core.ResultSetFutureExtractor, java.util.Map)
	 */
	@Override
	public <T> T queryAsynchronously(final String cql, ResultSetFutureExtractor<T> rse,
			final Map<String, Object> optionsByName) throws DataAccessException {
		return rse.extractData(execute(new SessionCallback<ResultSetFuture>() {
			@Override
			public ResultSetFuture doInSession(Session s) throws DataAccessException {
				Statement statement = new SimpleStatement(cql);
				addQueryOptions(statement, optionsByName);
				return s.executeAsync(statement);
			}
		}));
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#queryAsynchronously(java.lang.String, org.springframework.cassandra.core.ResultSetFutureExtractor, org.springframework.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> T queryAsynchronously(String cql, ResultSetFutureExtractor<T> rse, QueryOptions options)
			throws DataAccessException {
		Assert.notNull(options);
		return queryAsynchronously(cql, rse, options.toMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#query(java.lang.String, org.springframework.cassandra.core.FutureResultSetExtractor)
	 */
	@Override
	public <T> T queryAsynchronously(final String cql, ResultSetFutureExtractor<T> rse) throws DataAccessException {
		return queryAsynchronously(cql, rse, Collections.<String, Object> emptyMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#query(java.lang.String, org.springframework.cassandra.core.ResultSetExtractor)
	 */
	public <T> T query(String cql, ResultSetExtractor<T> rse, Map<String, Object> optionsByName)
			throws DataAccessException {
		Assert.notNull(cql);
		Assert.notNull(optionsByName);
		ResultSet rs = doExecute(cql, optionsByName);
		return rse.extractData(rs);
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#query(java.lang.String, org.springframework.cassandra.core.ResultSetExtractor, java.util.Map)
	 */
	@Override
	public <T> T query(String cql, ResultSetExtractor<T> rse) throws DataAccessException {
		return query(cql, rse, Collections.<String, Object> emptyMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#query(java.lang.String, org.springframework.cassandra.core.ResultSetExtractor, org.springframework.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> T query(String cql, ResultSetExtractor<T> rse, QueryOptions options) throws DataAccessException {
		Assert.notNull(options);
		return query(cql, rse, options.toMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#query(java.lang.String, org.springframework.cassandra.core.RowCallbackHandler, java.util.Map)
	 */
	@Override
	public void query(String cql, RowCallbackHandler rch, Map<String, Object> optionsByName) throws DataAccessException {
		process(doExecute(cql, optionsByName), rch);
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#query(java.lang.String, org.springframework.cassandra.core.RowCallbackHandler, org.springframework.cassandra.core.QueryOptions)
	 */
	@Override
	public void query(String cql, RowCallbackHandler rch, QueryOptions options) throws DataAccessException {
		Assert.notNull(options);
		query(cql, rch, options.toMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#query(java.lang.String, org.springframework.cassandra.core.RowCallbackHandler)
	 */
	public void query(String cql, RowCallbackHandler rch) throws DataAccessException {
		query(cql, rch, Collections.<String, Object> emptyMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#query(java.lang.String, org.springframework.cassandra.core.RowMapper, java.util.Map)
	 */
	@Override
	public <T> List<T> query(String cql, RowMapper<T> rowMapper, Map<String, Object> optionsByName)
			throws DataAccessException {
		Assert.notNull(optionsByName);
		return process(doExecute(cql, optionsByName), rowMapper);
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#query(java.lang.String, org.springframework.cassandra.core.RowMapper, org.springframework.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> List<T> query(String cql, RowMapper<T> rowMapper, QueryOptions options) throws DataAccessException {
		Assert.notNull(options);
		return query(cql, rowMapper, options.toMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#query(java.lang.String, org.springframework.cassandra.core.RowMapper)
	 */
	public <T> List<T> query(String cql, RowMapper<T> rowMapper) throws DataAccessException {
		return query(cql, rowMapper, Collections.<String, Object> emptyMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#queryForList(java.lang.String)
	 */
	public List<Map<String, Object>> queryForListOfMap(String cql) throws DataAccessException {
		return processListOfMap(doExecute(cql, Collections.<String, Object> emptyMap()));
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#queryForList(java.lang.String, java.lang.Class)
	 */
	public <T> List<T> queryForList(String cql, Class<T> elementType) throws DataAccessException {
		return processList(doExecute(cql, Collections.<String, Object> emptyMap()), elementType);
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#queryForMap(java.lang.String)
	 */
	public Map<String, Object> queryForMap(String cql) throws DataAccessException {
		return processMap(doExecute(cql, Collections.<String, Object> emptyMap()));
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#queryForObject(java.lang.String, java.lang.Class)
	 */
	public <T> T queryForObject(String cql, Class<T> requiredType) throws DataAccessException {
		return processOne(doExecute(cql, Collections.<String, Object> emptyMap()), requiredType);
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#queryForObject(java.lang.String, org.springframework.cassandra.core.RowMapper)
	 */
	public <T> T queryForObject(String cql, RowMapper<T> rowMapper) throws DataAccessException {
		return processOne(doExecute(cql, Collections.<String, Object> emptyMap()), rowMapper);
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
			throw throwTranslated(e);
		}
	}

	/**
	 * Execute a command at the Session Level
	 * 
	 * @param callback
	 * @return
	 */
	protected ResultSet doExecute(final String cql, final Map<String, Object> optionsByName) {

		logger.info(cql);

		return doExecute(new SessionCallback<ResultSet>() {

			@Override
			public ResultSet doInSession(Session s) throws DataAccessException {
				SimpleStatement statement = new SimpleStatement(cql);
				addQueryOptions(statement, optionsByName);
				return s.execute(statement);
			}
		});
	}

	/**
	 * Execute a command at the Session Level
	 * 
	 * @param callback
	 * @return
	 */
	protected ResultSet doExecute(final BoundStatement bs, final Map<String, Object> optionsByName) {

		return doExecute(new SessionCallback<ResultSet>() {

			@Override
			public ResultSet doInSession(Session s) throws DataAccessException {
				addQueryOptions(bs, optionsByName);
				return s.execute(bs);
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
		return cols.getType(0).deserialize(row.getBytesUnsafe(0));
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
			DataType dataType = def.getType();
			map.put(name, def.getType().deserialize(row.getBytesUnsafe(name)));
		}

		return map;
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#describeRing()
	 */
	@Override
	public List<RingMember> describeRing() throws DataAccessException {
		return new ArrayList<RingMember>(describeRing(new RingMemberHostMapper()));
	}

	/**
	 * Pulls the list of Hosts for the current Session
	 * 
	 * @return
	 */
	private Set<Host> getHosts() {

		/*
		 * Get the cluster metadata for this session
		 */
		Metadata clusterMetadata = doExecute(new SessionCallback<Metadata>() {

			@Override
			public Metadata doInSession(Session s) throws DataAccessException {
				return s.getCluster().getMetadata();
			}

		});

		/*
		 * Get all hosts in the cluster
		 */
		Set<Host> hosts = clusterMetadata.getAllHosts();

		return hosts;

	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#describeRing(org.springframework.cassandra.core.HostMapper)
	 */
	@Override
	public <T> Collection<T> describeRing(HostMapper<T> hostMapper) throws DataAccessException {
		Set<Host> hosts = getHosts();
		return hostMapper.mapHosts(hosts);
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#executeAsynchronously(java.lang.String)
	 */
	@Override
	public void executeAsynchronously(final String cql) throws DataAccessException {
		execute(new SessionCallback<Object>() {
			@Override
			public Object doInSession(Session s) throws DataAccessException {
				return s.executeAsync(cql);
			}
		});
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#process(com.datastax.driver.core.ResultSet, org.springframework.cassandra.core.RowCallbackHandler)
	 */
	@Override
	public void process(ResultSet resultSet, RowCallbackHandler rch) throws DataAccessException {
		try {
			for (Row row : resultSet.all()) {
				rch.processRow(row);
			}
		} catch (DriverException dx) {
			throwTranslated(dx);
		}
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#process(com.datastax.driver.core.ResultSet, org.springframework.cassandra.core.RowMapper)
	 */
	@Override
	public <T> List<T> process(ResultSet resultSet, RowMapper<T> rowMapper) throws DataAccessException {
		List<T> mappedRows = new ArrayList<T>();
		try {
			int i = 0;
			for (Row row : resultSet.all()) {
				mappedRows.add(rowMapper.mapRow(row, i++));
			}
		} catch (DriverException dx) {
			throwTranslated(dx);
		}
		return mappedRows;
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#processOne(com.datastax.driver.core.ResultSet, org.springframework.cassandra.core.RowMapper)
	 */
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
			throwTranslated(dx);
		}
		return row;
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#processOne(com.datastax.driver.core.ResultSet, java.lang.Class)
	 */
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

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#processMap(com.datastax.driver.core.ResultSet)
	 */
	@Override
	public Map<String, Object> processMap(ResultSet resultSet) throws DataAccessException {
		if (resultSet == null) {
			return null;
		}
		return toMap(resultSet.one());
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#processList(com.datastax.driver.core.ResultSet, java.lang.Class)
	 */
	@Override
	public <T> List<T> processList(ResultSet resultSet, Class<T> elementType) throws DataAccessException {
		List<Row> rows = resultSet.all();
		List<T> list = new ArrayList<T>(rows.size());
		for (Row row : rows) {
			list.add((T) firstColumnToObject(row));
		}
		return list;
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#processListOfMap(com.datastax.driver.core.ResultSet)
	 */
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
	protected RuntimeException throwTranslated(RuntimeException ex) {
		RuntimeException resolved = getExceptionTranslator().translateExceptionIfPossible(ex);
		return resolved == null ? ex : resolved;
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#execute(org.springframework.cassandra.core.PreparedStatementCreator, org.springframework.cassandra.core.PreparedStatementCallback)
	 */
	@Override
	public <T> T execute(PreparedStatementCreator psc, PreparedStatementCallback<T> action) {

		try {
			PreparedStatement ps = psc.createPreparedStatement(getSession());
			return action.doInPreparedStatement(ps);
		} catch (DriverException dx) {
			throwTranslated(dx);
		}

		return null;
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#execute(java.lang.String, org.springframework.cassandra.core.PreparedStatementCallback)
	 */
	@Override
	public <T> T execute(String cql, PreparedStatementCallback<T> action) {
		return execute(new SimplePreparedStatementCreator(cql), action);
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#query(org.springframework.cassandra.core.PreparedStatementCreator, org.springframework.cassandra.core.ResultSetExtractor, java.util.Map)
	 */
	@Override
	public <T> T query(PreparedStatementCreator psc, ResultSetExtractor<T> rse, Map<String, Object> optionsByName)
			throws DataAccessException {
		Assert.notNull(optionsByName);
		return query(psc, null, rse, optionsByName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#query(org.springframework.cassandra.core.PreparedStatementCreator, org.springframework.cassandra.core.ResultSetExtractor, org.springframework.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> T query(PreparedStatementCreator psc, ResultSetExtractor<T> rse, QueryOptions options)
			throws DataAccessException {
		Assert.notNull(options);
		return query(psc, rse, options.toMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#query(org.springframework.cassandra.core.PreparedStatementCreator, org.springframework.cassandra.core.ResultSetExtractor)
	 */
	@Override
	public <T> T query(PreparedStatementCreator psc, ResultSetExtractor<T> rse) throws DataAccessException {
		return query(psc, rse, Collections.<String, Object> emptyMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#query(org.springframework.cassandra.core.PreparedStatementCreator, org.springframework.cassandra.core.RowCallbackHandler, java.util.Map)
	 */
	@Override
	public void query(PreparedStatementCreator psc, RowCallbackHandler rch, Map<String, Object> optionsByName)
			throws DataAccessException {
		Assert.notNull(optionsByName);
		query(psc, null, rch, optionsByName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#query(org.springframework.cassandra.core.PreparedStatementCreator, org.springframework.cassandra.core.RowCallbackHandler, org.springframework.cassandra.core.QueryOptions)
	 */
	@Override
	public void query(PreparedStatementCreator psc, RowCallbackHandler rch, QueryOptions options)
			throws DataAccessException {
		Assert.notNull(options);
		query(psc, rch, options.toMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#query(org.springframework.cassandra.core.PreparedStatementCreator, org.springframework.cassandra.core.RowCallbackHandler)
	 */
	@Override
	public void query(PreparedStatementCreator psc, RowCallbackHandler rch) throws DataAccessException {
		query(psc, rch, Collections.<String, Object> emptyMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#query(org.springframework.cassandra.core.PreparedStatementCreator, org.springframework.cassandra.core.RowMapper, java.util.Map)
	 */
	@Override
	public <T> List<T> query(PreparedStatementCreator psc, RowMapper<T> rowMapper, Map<String, Object> optionsByName)
			throws DataAccessException {
		Assert.notNull(optionsByName);
		return query(psc, null, rowMapper, optionsByName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#query(org.springframework.cassandra.core.PreparedStatementCreator, org.springframework.cassandra.core.RowMapper, org.springframework.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> List<T> query(PreparedStatementCreator psc, RowMapper<T> rowMapper, QueryOptions options)
			throws DataAccessException {
		Assert.notNull(options);
		return query(psc, rowMapper, options.toMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#query(org.springframework.cassandra.core.PreparedStatementCreator, org.springframework.cassandra.core.RowMapper)
	 */
	@Override
	public <T> List<T> query(PreparedStatementCreator psc, RowMapper<T> rowMapper) throws DataAccessException {
		return query(psc, rowMapper, Collections.<String, Object> emptyMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#query(org.springframework.cassandra.core.PreparedStatementCreator, org.springframework.cassandra.core.PreparedStatementSetter, org.springframework.cassandra.core.ResultSetExtractor)
	 */
	public <T> T query(PreparedStatementCreator psc, final PreparedStatementBinder psb, final ResultSetExtractor<T> rse,
			final Map<String, Object> optionsByName) throws DataAccessException {

		Assert.notNull(rse, "ResultSetExtractor must not be null");
		logger.debug("Executing prepared CQL query");

		return execute(psc, new PreparedStatementCallback<T>() {
			public T doInPreparedStatement(PreparedStatement ps) throws DriverException {
				ResultSet rs = null;
				BoundStatement bs = null;
				if (psb != null) {
					bs = psb.bindValues(ps);
				} else {
					bs = ps.bind();
				}
				rs = doExecute(bs, optionsByName);
				return rse.extractData(rs);
			}
		});
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#query(java.lang.String, org.springframework.cassandra.core.PreparedStatementBinder, org.springframework.cassandra.core.ResultSetExtractor, java.util.Map)
	 */
	@Override
	public <T> T query(String cql, PreparedStatementBinder psb, ResultSetExtractor<T> rse,
			Map<String, Object> optionsByName) throws DataAccessException {
		Assert.notNull(optionsByName);
		return query(new SimplePreparedStatementCreator(cql), psb, rse, optionsByName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#query(java.lang.String, org.springframework.cassandra.core.PreparedStatementBinder, org.springframework.cassandra.core.ResultSetExtractor, org.springframework.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> T query(String cql, PreparedStatementBinder psb, ResultSetExtractor<T> rse, QueryOptions options)
			throws DataAccessException {
		Assert.notNull(options);
		return query(cql, psb, rse, options.toMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#query(java.lang.String, org.springframework.cassandra.core.PreparedStatementSetter, org.springframework.cassandra.core.ResultSetExtractor)
	 */
	@Override
	public <T> T query(String cql, PreparedStatementBinder psb, ResultSetExtractor<T> rse) throws DataAccessException {
		return query(cql, psb, rse, Collections.<String, Object> emptyMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#query(java.lang.String, org.springframework.cassandra.core.PreparedStatementBinder, org.springframework.cassandra.core.RowCallbackHandler, java.util.Map)
	 */
	@Override
	public void query(String cql, PreparedStatementBinder psb, RowCallbackHandler rch, Map<String, Object> optionsByName)
			throws DataAccessException {
		Assert.notNull(optionsByName);
		query(new SimplePreparedStatementCreator(cql), psb, rch, optionsByName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#query(java.lang.String, org.springframework.cassandra.core.PreparedStatementBinder, org.springframework.cassandra.core.RowCallbackHandler, org.springframework.cassandra.core.QueryOptions)
	 */
	@Override
	public void query(String cql, PreparedStatementBinder psb, RowCallbackHandler rch, QueryOptions options)
			throws DataAccessException {
		Assert.notNull(options);
		query(cql, psb, rch, options.toMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#query(java.lang.String, org.springframework.cassandra.core.PreparedStatementSetter, org.springframework.cassandra.core.RowCallbackHandler)
	 */
	@Override
	public void query(String cql, PreparedStatementBinder psb, RowCallbackHandler rch) throws DataAccessException {
		query(cql, psb, rch, Collections.<String, Object> emptyMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#query(java.lang.String, org.springframework.cassandra.core.PreparedStatementBinder, org.springframework.cassandra.core.RowMapper, java.util.Map)
	 */
	@Override
	public <T> List<T> query(String cql, PreparedStatementBinder psb, RowMapper<T> rowMapper,
			Map<String, Object> optionsByName) throws DataAccessException {
		Assert.notNull(optionsByName);
		return query(new SimplePreparedStatementCreator(cql), psb, rowMapper, optionsByName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#query(java.lang.String, org.springframework.cassandra.core.PreparedStatementBinder, org.springframework.cassandra.core.RowMapper, org.springframework.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> List<T> query(String cql, PreparedStatementBinder psb, RowMapper<T> rowMapper, QueryOptions options)
			throws DataAccessException {
		Assert.notNull(options);
		return query(cql, psb, rowMapper, options.toMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#query(java.lang.String, org.springframework.cassandra.core.PreparedStatementSetter, org.springframework.cassandra.core.RowMapper)
	 */
	@Override
	public <T> List<T> query(String cql, PreparedStatementBinder psb, RowMapper<T> rowMapper) throws DataAccessException {
		return query(cql, psb, rowMapper, Collections.<String, Object> emptyMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#query(org.springframework.cassandra.core.PreparedStatementCreator, org.springframework.cassandra.core.PreparedStatementBinder, org.springframework.cassandra.core.RowCallbackHandler)
	 */
	@Override
	public void query(PreparedStatementCreator psc, final PreparedStatementBinder psb, final RowCallbackHandler rch,
			final Map<String, Object> optionsByName) throws DataAccessException {
		Assert.notNull(rch, "RowCallbackHandler must not be null");
		logger.debug("Executing prepared CQL query");

		execute(psc, new PreparedStatementCallback<Object>() {
			public Object doInPreparedStatement(PreparedStatement ps) throws DriverException {
				ResultSet rs = null;
				BoundStatement bs = null;
				if (psb != null) {
					bs = psb.bindValues(ps);
				} else {
					bs = ps.bind();
				}
				rs = doExecute(bs, optionsByName);
				process(rs, rch);
				return null;
			}
		});
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#query(org.springframework.cassandra.core.PreparedStatementCreator, org.springframework.cassandra.core.PreparedStatementBinder, org.springframework.cassandra.core.RowMapper)
	 */
	@Override
	public <T> List<T> query(PreparedStatementCreator psc, final PreparedStatementBinder psb,
			final RowMapper<T> rowMapper, final Map<String, Object> optionsByName) throws DataAccessException {
		Assert.notNull(rowMapper, "RowMapper must not be null");
		logger.debug("Executing prepared CQL query");

		return execute(psc, new PreparedStatementCallback<List<T>>() {
			public List<T> doInPreparedStatement(PreparedStatement ps) throws DriverException {
				ResultSet rs = null;
				BoundStatement bs = null;
				if (psb != null) {
					bs = psb.bindValues(ps);
				} else {
					bs = ps.bind();
				}
				rs = doExecute(bs, optionsByName);

				return process(rs, rowMapper);
			}
		});
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#execute(java.lang.String, org.springframework.cassandra.core.RowProvider, int)
	 */
	@Override
	public void ingest(String cql, RowIterator rowIterator, Map<String, Object> optionsByName) {

		PreparedStatement preparedStatement = getSession().prepare(cql);
		addPreparedStatementOptions(preparedStatement, optionsByName);

		while (rowIterator.hasNext()) {
			getSession().execute(preparedStatement.bind(rowIterator.next()));
		}

	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#ingest(java.lang.String, org.springframework.cassandra.core.RowIterator, org.springframework.cassandra.core.QueryOptions)
	 */
	@Override
	public void ingest(String cql, RowIterator rowIterator, QueryOptions options) {
		Assert.notNull(options);
		ingest(cql, rowIterator, options.toMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#ingest(java.lang.String, org.springframework.cassandra.core.RowIterator)
	 */
	@Override
	public void ingest(String cql, RowIterator rowIterator) {
		ingest(cql, rowIterator, Collections.<String, Object> emptyMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#execute(java.lang.String, java.util.List)
	 */
	@Override
	public void ingest(String cql, List<List<?>> rows, Map<String, Object> optionsByName) {

		Assert.notNull(optionsByName);
		Assert.notNull(rows);
		Assert.notEmpty(rows);

		Object[][] values = new Object[rows.size()][];
		int i = 0;
		for (List<?> row : rows) {
			values[i++] = row.toArray();
		}

		ingest(cql, values, optionsByName);

	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#ingest(java.lang.String, java.util.List, org.springframework.cassandra.core.QueryOptions)
	 */
	@Override
	public void ingest(String cql, List<List<?>> rows, QueryOptions options) {
		Assert.notNull(options);
		ingest(cql, rows, options.toMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#ingest(java.lang.String, java.util.List)
	 */
	@Override
	public void ingest(String cql, List<List<?>> rows) {
		ingest(cql, rows, Collections.<String, Object> emptyMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#execute(java.lang.String, java.lang.Object[][])
	 */
	@Override
	public void ingest(String cql, final Object[][] rows, final Map<String, Object> optionsByName) {

		Assert.notNull(optionsByName);

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

		}, optionsByName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#ingest(java.lang.String, java.lang.Object[][], org.springframework.cassandra.core.QueryOptions)
	 */
	@Override
	public void ingest(String cql, final Object[][] rows, QueryOptions options) {
		Assert.notNull(options);
		ingest(cql, rows, options.toMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#ingest(java.lang.String, java.lang.Object[][])
	 */
	@Override
	public void ingest(String cql, final Object[][] rows) {
		ingest(cql, rows, Collections.<String, Object> emptyMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#truncate(java.lang.String)
	 */
	@Override
	public void truncate(String tableName) throws DataAccessException {
		Truncate truncate = QueryBuilder.truncate(tableName);
		doExecute(truncate.getQueryString(), Collections.<String, Object> emptyMap());
	}

	/**
	 * Add common Query options for all types of queries.
	 * 
	 * @param q
	 * @param optionsByName
	 */
	public static void addQueryOptions(Query q, Map<String, Object> optionsByName) {

		if (optionsByName == null) {
			return;
		}

		/*
		 * Add Query Options
		 */
		if (optionsByName.get(QueryOptions.QueryOptionMapKeys.CONSISTENCY_LEVEL) != null) {
			q.setConsistencyLevel(ConsistencyLevelResolver.resolve((ConsistencyLevel) optionsByName
					.get(QueryOptions.QueryOptionMapKeys.CONSISTENCY_LEVEL)));
		}
		if (optionsByName.get(QueryOptions.QueryOptionMapKeys.RETRY_POLICY) != null) {
			q.setRetryPolicy(RetryPolicyResolver.resolve((RetryPolicy) optionsByName
					.get(QueryOptions.QueryOptionMapKeys.RETRY_POLICY)));
		}

	}

	/**
	 * Add common Query options for all types of queries.
	 * 
	 * @param q
	 * @param optionsByName
	 */
	public static void addPreparedStatementOptions(PreparedStatement s, Map<String, Object> optionsByName) {

		if (optionsByName == null) {
			return;
		}

		/*
		 * Add Query Options
		 */
		if (optionsByName.get(QueryOptions.QueryOptionMapKeys.CONSISTENCY_LEVEL) != null) {
			s.setConsistencyLevel(ConsistencyLevelResolver.resolve((ConsistencyLevel) optionsByName
					.get(QueryOptions.QueryOptionMapKeys.CONSISTENCY_LEVEL)));
		}
		if (optionsByName.get(QueryOptions.QueryOptionMapKeys.RETRY_POLICY) != null) {
			s.setRetryPolicy(RetryPolicyResolver.resolve((RetryPolicy) optionsByName
					.get(QueryOptions.QueryOptionMapKeys.RETRY_POLICY)));
		}

	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#query(org.springframework.cassandra.core.PreparedStatementCreator, org.springframework.cassandra.core.PreparedStatementBinder, org.springframework.cassandra.core.ResultSetExtractor, org.springframework.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> T query(PreparedStatementCreator psc, PreparedStatementBinder psb, ResultSetExtractor<T> rse,
			QueryOptions options) throws DataAccessException {
		Assert.notNull(options);
		return query(psc, psb, rse, options.toMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#query(org.springframework.cassandra.core.PreparedStatementCreator, org.springframework.cassandra.core.PreparedStatementBinder, org.springframework.cassandra.core.ResultSetExtractor)
	 */
	@Override
	public <T> T query(PreparedStatementCreator psc, PreparedStatementBinder psb, ResultSetExtractor<T> rse)
			throws DataAccessException {
		return query(psc, psb, rse, Collections.<String, Object> emptyMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#query(org.springframework.cassandra.core.PreparedStatementCreator, org.springframework.cassandra.core.PreparedStatementBinder, org.springframework.cassandra.core.RowCallbackHandler, org.springframework.cassandra.core.QueryOptions)
	 */
	@Override
	public void query(PreparedStatementCreator psc, PreparedStatementBinder psb, RowCallbackHandler rch,
			QueryOptions options) throws DataAccessException {
		Assert.notNull(options);
		query(psc, psb, rch, options.toMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#query(org.springframework.cassandra.core.PreparedStatementCreator, org.springframework.cassandra.core.PreparedStatementBinder, org.springframework.cassandra.core.RowCallbackHandler)
	 */
	@Override
	public void query(PreparedStatementCreator psc, PreparedStatementBinder psb, RowCallbackHandler rch)
			throws DataAccessException {
		query(psc, psb, rch, Collections.<String, Object> emptyMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#query(org.springframework.cassandra.core.PreparedStatementCreator, org.springframework.cassandra.core.PreparedStatementBinder, org.springframework.cassandra.core.RowMapper, org.springframework.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> List<T> query(PreparedStatementCreator psc, PreparedStatementBinder psb, RowMapper<T> rowMapper,
			QueryOptions options) throws DataAccessException {
		Assert.notNull(options);
		return query(psc, psb, rowMapper, options.toMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#query(org.springframework.cassandra.core.PreparedStatementCreator, org.springframework.cassandra.core.PreparedStatementBinder, org.springframework.cassandra.core.RowMapper)
	 */
	@Override
	public <T> List<T> query(PreparedStatementCreator psc, PreparedStatementBinder psb, RowMapper<T> rowMapper)
			throws DataAccessException {
		return query(psc, psb, rowMapper, Collections.<String, Object> emptyMap());
	}

}