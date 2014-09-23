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

import org.springframework.util.Assert;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.DriverException;

/**
 * This Prepared Statement Creator simply prepares a statement from the CQL string. This should not be used in
 * Production systems with high volume reads and writes. Use {@link CachedPreparedStatementCreator} When preparing
 * statements with Cassandra, each Statement should be prepared once and only once due to the overhead of preparing the
 * statement.
 * 
 * @author David Webb
 */
public class SimplePreparedStatementCreator implements PreparedStatementCreator {

	private final String cql;

	/**
	 * Create a PreparedStatementCreator from the provided CQL.
	 * 
	 * @param cql
	 */
	public SimplePreparedStatementCreator(String cql) {
		Assert.notNull(cql, "CQL is required to create a PreparedStatement");
		this.cql = cql;
	}

	public String getCql() {
		return this.cql;
	}

	@Override
	public PreparedStatement createPreparedStatement(Session session) throws DriverException {
		return session.prepare(this.cql);
	}

}
