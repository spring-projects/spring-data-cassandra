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

import java.util.List;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.DriverException;

/**
 * @author David Webb
 * 
 */
public class PreparedStatementCreatorImpl implements PreparedStatementCreator, PreparedStatementBinder {

	private final String cql;
	private List<Object> values;

	public PreparedStatementCreatorImpl(String cql) {
		this.cql = cql;
	}

	public PreparedStatementCreatorImpl(String cql, List<Object> values) {
		this.cql = cql;
		this.values = values;
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.PreparedStatementSetter#setValues(com.datastax.driver.core.PreparedStatement)
	 */
	@Override
	public BoundStatement bindValues(PreparedStatement ps) throws DriverException {
		// Nothing to set if there are no values
		if (values == null) {
			return new BoundStatement(ps);
		}

		return ps.bind(values.toArray());

	}

	public String getCql() {
		return this.cql;
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.PreparedStatementCreator#createPreparedStatement(com.datastax.driver.core.Session)
	 */
	@Override
	public PreparedStatement createPreparedStatement(Session session) throws DriverException {
		return session.prepare(this.cql);
	}

}
