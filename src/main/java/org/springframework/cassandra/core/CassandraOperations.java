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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.springframework.dao.DataAccessException;

import com.datastax.driver.core.ResultSet;

/**
 * Operations for interacting with Cassandra at the lowest level. This interface provides Exception Translation.
 * 
 * @author David Webb
 * @author Matthew Adams
 */
public interface CassandraOperations {

	/**
	 * Executes the supplied {@link SessionCallback} in the current Template Session. The implementation of
	 * SessionCallback can decide whether or not to <code>execute()</code> or <code>executeAsync()</code> the operation.
	 * 
	 * @param sessionCallback
	 * @return
	 */
	<T> T execute(SessionCallback<T> sessionCallback) throws DataAccessException;

	/**
	 * Executes the supplied CQL Query and returns nothing.
	 * 
	 * @param cql
	 */
	void execute(final String cql) throws DataAccessException;

	/**
	 * Executes the supplied CQL Query Asynchrously and returns nothing.
	 * 
	 * @param cql
	 */
	void executeAsynchronously(final String cql) throws DataAccessException;

	/**
	 * Executes the provided CQL Query, and extracts the results with the ResultSetExtractor
	 * 
	 * @param cql The Query
	 * @param rse The implementation for extracting the results
	 * 
	 * @return
	 * @throws DataAccessException
	 */
	<T> T query(final String cql, ResultSetExtractor<T> rse) throws DataAccessException;

	/**
	 * Executes the provided CQL Query asynchronously, and extracts the results with the ResultSetFutureExtractor
	 * 
	 * @param cql The Query
	 * @param rse The implementation for extracting the results
	 * @return
	 * @throws DataAccessException
	 */
	<T> T queryAsynchronously(final String cql, ResultSetFutureExtractor<T> rse) throws DataAccessException;

	void query(final String cql, RowCallbackHandler rch) throws DataAccessException;

	void process(ResultSet resultSet, RowCallbackHandler rch) throws DataAccessException;

	<T> List<T> query(final String cql, RowMapper<T> rowMapper) throws DataAccessException;

	<T> List<T> process(ResultSet resultSet, RowMapper<T> rowMapper) throws DataAccessException;

	<T> T queryForObject(final String cql, RowMapper<T> rowMapper) throws DataAccessException;

	<T> T processOne(ResultSet resultSet, RowMapper<T> rowMapper) throws DataAccessException;

	<T> T queryForObject(final String cql, Class<T> requiredType) throws DataAccessException;

	<T> T processOne(ResultSet resultSet, Class<T> requiredType) throws DataAccessException;

	Map<String, Object> queryForMap(final String cql) throws DataAccessException;

	Map<String, Object> processMap(ResultSet resultSet) throws DataAccessException;

	<T> List<T> queryForList(final String cql, Class<T> elementType) throws DataAccessException;

	<T> List<T> processList(ResultSet resultSet, Class<T> elementType) throws DataAccessException;

	List<Map<String, Object>> queryForListOfMap(final String cql) throws DataAccessException;

	List<Map<String, Object>> processListOfMap(ResultSet resultSet) throws DataAccessException;

	<T> T execute(String cql, PreparedStatementCallback<T> action) throws DataAccessException;

	<T> T execute(PreparedStatementCreator psc, PreparedStatementCallback<T> action) throws DataAccessException;

	<T> T query(final String cql, PreparedStatementBinder psb, ResultSetExtractor<T> rse) throws DataAccessException;

	void query(final String cql, PreparedStatementBinder psb, RowCallbackHandler rch) throws DataAccessException;

	<T> List<T> query(final String cql, PreparedStatementBinder psb, RowMapper<T> rowMapper) throws DataAccessException;

	<T> T query(PreparedStatementCreator psc, ResultSetExtractor<T> rse) throws DataAccessException;

	void query(PreparedStatementCreator psc, RowCallbackHandler rch) throws DataAccessException;

	<T> List<T> query(PreparedStatementCreator psc, RowMapper<T> rowMapper) throws DataAccessException;

	<T> T query(PreparedStatementCreator psc, final PreparedStatementBinder psb, final ResultSetExtractor<T> rse)
			throws DataAccessException;

	void query(PreparedStatementCreator psc, final PreparedStatementBinder psb, final RowCallbackHandler rch)
			throws DataAccessException;

	<T> List<T> query(PreparedStatementCreator psc, final PreparedStatementBinder psb, final RowMapper<T> rowMapper)
			throws DataAccessException;

	/**
	 * Describe the current Ring
	 * 
	 * @return The list of ring tokens that are active in the cluster
	 */
	List<RingMember> describeRing() throws DataAccessException;

	<T> Collection<T> describeRing(HostMapper<T> hostMapper) throws DataAccessException;

}
