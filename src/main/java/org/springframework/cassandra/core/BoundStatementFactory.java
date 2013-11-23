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

import java.util.LinkedList;
import java.util.List;

import org.springframework.util.CollectionUtils;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.DriverException;

/**
 * <b>This is the primary class in core for binding many values to a Cassandra PreparedStatement.</b>
 * 
 * <p>
 * This factory will hold a cached version of the PreparedStatement, and bind many value sets to that statement
 * returning a BoundStatement that can be passed to a Session.execute(Query).
 * </p>
 * 
 * @author David Webb
 * 
 */
public class BoundStatementFactory implements PreparedStatementCreator, CqlProvider {

	private final String cql;
	private PreparedStatement preparedStatement;
	private List<List<?>> values = new LinkedList<List<?>>();

	public BoundStatementFactory(String cql) {
		this.cql = cql;
	}

	public void addValues(List<CqlParameterValue>... values) {
		this.values.add(CollectionUtils.arrayToList(values));
	}

	public void addValues(Object[]... values) {
		for (int i = 0; values != null && i < values.length; i++) {
			this.values.add(CollectionUtils.arrayToList(values[i]));
		}
	}

	public void replaceValues(List<CqlParameterValue>... values) {
		this.values = CollectionUtils.arrayToList(values);
	}

	public void replaceValues(Object[]... values) {
		noValues();
		for (int i = 0; values != null && i < values.length; i++) {
			this.values.add(CollectionUtils.arrayToList(values[i]));
		}

	}

	public void noValues() {
		this.values = new LinkedList<List<?>>();
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CqlProvider#getCql()
	 */
	@Override
	public String getCql() {
		return this.cql;
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.PreparedStatementCreator#createPreparedStatement(com.datastax.driver.core.Session)
	 */
	@Override
	public PreparedStatement createPreparedStatement(Session session) throws DriverException {
		if (preparedStatement == null) {
			preparedStatement = session.prepare(this.cql);
		}
		return preparedStatement;
	}

	/**
	 * Bind all values with the single CQL (PreparedStatement) and return BoundStatements read for execution.
	 * 
	 * @return
	 * @throws DriverException
	 */
	public List<BoundStatement> bindValues() throws DriverException {

		LinkedList<BoundStatement> boundStatements = new LinkedList<BoundStatement>();

		for (List<?> list : this.values) {

			// Test the type of the first value
			Object v = list.get(0);

			Object[] vls;
			if (v instanceof CqlParameterValue) {
				LinkedList<Object> valuesList = new LinkedList<Object>();
				for (Object value : list) {
					valuesList.add(((CqlParameterValue) value).getValue());
				}
				vls = valuesList.toArray();
			} else {
				vls = list.toArray();
			}

			boundStatements.add(preparedStatement.bind(vls));

		}

		return boundStatements;
	}
}
