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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.cassandra.support.CassandraAccessor;
import org.springframework.dao.DataAccessException;
import org.springframework.data.cassandra.core.CassandraDataTemplate;
import org.springframework.util.Assert;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.DriverException;

/**
 * The CassandraTemplate is a Spring convenience wrapper for low level and explicit operations on the Cassandra
 * Database. For working iwth POJOs, use the {@link CassandraDataTemplate}
 * 
 * @author Alex Shvid
 * @author David Webb
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
	public <T> T execute(SessionCallback<T> sessionCallback) {
		return doExecute(sessionCallback);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#execute(java.lang.String)
	 */
	@Override
	public void execute(final String cql) {

		doExecute(new SessionCallback<Object>() {

			@Override
			public Object doInSession(Session s) throws DataAccessException {
				return s.execute(cql);
			}
		});

	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#executeQuery(java.lang.String)
	 */
	@Override
	public ResultSet executeQuery(final String query) {

		return doExecute(new SessionCallback<ResultSet>() {

			@Override
			public ResultSet doInSession(Session s) throws DataAccessException {

				return s.execute(query);

			}

		});

	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#executeQueryAsync(java.lang.String)
	 */
	@Override
	public ResultSetFuture executeQueryAsynchronously(final String query) {

		return doExecute(new SessionCallback<ResultSetFuture>() {

			@Override
			public ResultSetFuture doInSession(Session s) throws DataAccessException {

				return s.executeAsync(query);

			}

		});

	}

	/**
	 * Attempt to translate a Runtime Exception to a Spring Data Exception
	 * 
	 * @param ex
	 * @return
	 */
	public RuntimeException potentiallyConvertRuntimeException(RuntimeException ex) {
		RuntimeException resolved = getExceptionTranslator().translateExceptionIfPossible(ex);
		return resolved == null ? ex : resolved;
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
			throw potentiallyConvertRuntimeException(e);
		}
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#query(java.lang.String, org.springframework.cassandra.core.ResultSetExtractor)
	 */
	public <T> T query(String cql, ResultSetExtractor<T> rse) throws DataAccessException {
		try {
			ResultSet rs = getSession().execute(cql);
			return rse.extractData(rs);
		} catch (DriverException dx) {
			throw getExceptionTranslator().translateExceptionIfPossible(dx);
		}
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#query(java.lang.String, org.springframework.cassandra.core.RowCallbackHandler)
	 */
	public void query(String cql, RowCallbackHandler rch) throws DataAccessException {
		try {
			ResultSet rs = getSession().execute(cql);
			for (Row row : rs.all()) {
				rch.processRow(row);
			}
		} catch (DriverException dx) {
			throw getExceptionTranslator().translateExceptionIfPossible(dx);
		}

	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#query(java.lang.String, org.springframework.cassandra.core.RowMapper)
	 */
	public <T> List<T> query(String cql, RowMapper<T> rowMapper) throws DataAccessException {
		try {
			ResultSet rs = getSession().execute(cql);
			int i = 0;
			List<T> mappedRows = new ArrayList<T>();
			for (Row row : rs.all()) {
				mappedRows.add(rowMapper.mapRow(row, i++));
			}
			return mappedRows;
		} catch (DriverException dx) {
			throw getExceptionTranslator().translateExceptionIfPossible(dx);
		}
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#queryForObject(java.lang.String, org.springframework.cassandra.core.RowMapper)
	 */
	public <T> T queryForObject(String cql, RowMapper<T> rowMapper) throws DataAccessException {
		try {
			ResultSet rs = getSession().execute(cql);
			List<Row> rows = rs.all();
			Assert.notNull(rows, "null row list returned from query");
			Assert.isTrue(rows.size() == 1, "row list has " + rows.size() + " rows instead of one");
			return rowMapper.mapRow(rows.get(0), 0);
		} catch (DriverException dx) {
			throw getExceptionTranslator().translateExceptionIfPossible(dx);
		}
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#queryForObject(java.lang.String, java.lang.Class)
	 */
	@SuppressWarnings("unchecked")
	public <T> T queryForObject(String cql, Class<T> requiredType) throws DataAccessException {
		ResultSet rs = getSession().execute(cql);
		if (rs == null) {
			return null;
		}
		Row row = rs.one();
		if (row == null) {
			return null;
		}
		return (T) firstColumnToObject(row);
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

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#queryForMap(java.lang.String)
	 */
	public Map<String, Object> queryForMap(String cql) throws DataAccessException {
		ResultSet rs = getSession().execute(cql);
		if (rs == null) {
			return null;
		}
		return toMap(rs.one());
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
			map.put(name, def.getType().deserialize(row.getBytesUnsafe(name)));
		}

		return map;
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#queryForList(java.lang.String, java.lang.Class)
	 */
	@SuppressWarnings("unchecked")
	public <T> List<T> queryForList(String cql, Class<T> elementType) throws DataAccessException {
		ResultSet rs = getSession().execute(cql);
		List<Row> rows = rs.all();
		List<T> list = new ArrayList<T>(rows.size());
		for (Row row : rows) {
			list.add((T) firstColumnToObject(row));
		}
		return list;
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CassandraOperations#queryForList(java.lang.String)
	 */
	public List<Map<String, Object>> queryForList(String cql) throws DataAccessException {
		ResultSet rs = getSession().execute(cql);
		List<Row> rows = rs.all();
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>(rows.size());
		for (Row row : rows) {
			list.add(toMap(row));
		}
		return list;
	}

}
