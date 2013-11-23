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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.util.Assert;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.DriverException;

/**
 * @author David Webb
 * 
 */
public class PreparedStatementCreatorFactory {

	/**
	 * The CQL, which won't change when the parameters change
	 */
	private final String cql;

	/** List of CqlParameter objects. May not be {@code null}. */
	private final List<CqlParameter> declaredParameters;

	/**
	 * Create a new factory.
	 */
	public PreparedStatementCreatorFactory(String cql) {
		this.cql = cql;
		this.declaredParameters = new LinkedList<CqlParameter>();
	}

	/**
	 * Create a new factory with the given CQL and parameters.
	 * 
	 * @param cql CQL
	 * @param declaredParameters list of {@link CqlParameter} objects
	 * @see CqlParameter
	 */
	public PreparedStatementCreatorFactory(String cql, List<CqlParameter> declaredParameters) {
		this.cql = cql;
		this.declaredParameters = declaredParameters;
	}

	/**
	 * Return a new PreparedStatementBinder for the given parameters.
	 * 
	 * @param params list of parameters (may be {@code null})
	 */
	public PreparedStatementBinder newPreparedStatementBinder(List<CqlParameterValue> params) {
		return new PreparedStatementCreatorImpl(params != null ? params : Collections.emptyList());
	}

	/**
	 * Return a new PreparedStatementBinder for the given parameters.
	 * 
	 * @param params the parameter array (may be {@code null})
	 */
	public PreparedStatementBinder newPreparedStatementBinder(Object[] params) {
		return new PreparedStatementCreatorImpl(params != null ? Arrays.asList(params) : Collections.emptyList());
	}

	/**
	 * Return a new PreparedStatementCreator for the given parameters.
	 * 
	 * @param params list of parameters (may be {@code null})
	 */
	public PreparedStatementCreator newPreparedStatementCreator(List<CqlParameterValue> params) {
		return new PreparedStatementCreatorImpl(params != null ? params : Collections.emptyList());
	}

	/**
	 * Return a new PreparedStatementCreator for the given parameters.
	 * 
	 * @param params the parameter array (may be {@code null})
	 */
	public PreparedStatementCreator newPreparedStatementCreator(Object[] params) {
		return new PreparedStatementCreatorImpl(params != null ? Arrays.asList(params) : Collections.emptyList());
	}

	/**
	 * Return a new PreparedStatementCreator for the given parameters.
	 * 
	 * @param sqlToUse the actual SQL statement to use (if different from the factory's, for example because of named
	 *          parameter expanding)
	 * @param params the parameter array (may be {@code null})
	 */
	public PreparedStatementCreator newPreparedStatementCreator(String sqlToUse, Object[] params) {
		return new PreparedStatementCreatorImpl(sqlToUse, params != null ? Arrays.asList(params) : Collections.emptyList());
	}

	/**
	 * PreparedStatementCreator implementation returned by this class.
	 */
	private class PreparedStatementCreatorImpl implements PreparedStatementCreator, PreparedStatementBinder, CqlProvider {

		private final String actualCql;

		private final List<?> parameters;

		public PreparedStatementCreatorImpl(List<?> parameters) {
			this(cql, parameters);
		}

		/**
		 * @param actualCql
		 * @param parameters
		 */
		public PreparedStatementCreatorImpl(String actualCql, List<?> parameters) {
			this.actualCql = actualCql;
			Assert.notNull(parameters, "Parameters List must not be null");
			this.parameters = parameters;
			if (this.parameters.size() != declaredParameters.size()) {
				Set<String> names = new HashSet<String>();
				for (int i = 0; i < parameters.size(); i++) {
					Object param = parameters.get(i);
					if (param instanceof CqlParameterValue) {
						names.add(((CqlParameterValue) param).getName());
					} else {
						names.add("Parameter #" + i);
					}
				}
				if (names.size() != declaredParameters.size()) {
					throw new InvalidDataAccessApiUsageException("CQL [" + cql + "]: given " + names.size()
							+ " parameters but expected " + declaredParameters.size());
				}
			}
		}

		/* (non-Javadoc)
		 * @see org.springframework.cassandra.core.PreparedStatementCreator#createPreparedStatement(com.datastax.driver.core.Session)
		 */
		@Override
		public PreparedStatement createPreparedStatement(Session session) throws DriverException {
			return session.prepare(this.actualCql);
		}

		/* (non-Javadoc)
		 * @see org.springframework.cassandra.core.PreparedStatementBinder#bindValues(com.datastax.driver.core.PreparedStatement)
		 */
		@Override
		public BoundStatement bindValues(PreparedStatement ps) throws DriverException {
			if (this.parameters == null || this.parameters.size() == 0) {
				return ps.bind();
			}

			// Test the type of the first value
			Object v = this.parameters.get(0);
			Object[] values;
			if (v instanceof CqlParameterValue) {
				LinkedList<Object> valuesList = new LinkedList<Object>();
				for (Object value : this.parameters) {
					valuesList.add(((CqlParameterValue) value).getValue());
				}
				values = valuesList.toArray();
			} else {
				values = this.parameters.toArray();
			}

			return ps.bind(values);
		}

		/* (non-Javadoc)
		 * @see org.springframework.cassandra.core.CqlProvider#getCql()
		 */
		@Override
		public String getCql() {
			return cql;
		}

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append("PreparedStatementCreatorFactory.PreparedStatementCreatorImpl: cql=[");
			sb.append(cql).append("]; parameters=").append(this.parameters);
			return sb.toString();
		}

	}
}
