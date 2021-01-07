/*
 * Copyright 2013-2021 the original author or authors.
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
package org.springframework.data.cassandra.core.cql;

import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

/**
 * Trivial implementation of {@link PreparedStatementCreator}. This prepared statement creator simply prepares a
 * statement from the CQL string. Exposes the given CQL statement through {@link CqlProvider#getCql()},
 *
 * @author David Webb
 * @author Mark Paluch
 * @see CqlProvider
 */
public class SimplePreparedStatementCreator implements PreparedStatementCreator, CqlProvider {

	private final SimpleStatement statement;

	/**
	 * Create a {@link SimplePreparedStatementCreator} given {@code cql}.
	 *
	 * @param cql must not be {@literal null}.
	 */
	public SimplePreparedStatementCreator(String cql) {

		Assert.notNull(cql, "CQL is required to create a PreparedStatement");

		this.statement = SimpleStatement.newInstance(cql);
	}

	/**
	 * Create a {@link SimplePreparedStatementCreator} given {@code cql}.
	 *
	 * @param statement must not be {@literal null}.
	 * @since 3.1
	 */
	public SimplePreparedStatementCreator(SimpleStatement statement) {

		Assert.notNull(statement, "CQL is required to create a PreparedStatement");

		this.statement = statement;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.PreparedStatementCreator#createPreparedStatement(com.datastax.oss.driver.api.core.CqlSession)
	 */
	@Override
	public PreparedStatement createPreparedStatement(CqlSession session) throws DriverException {
		return session.prepare(this.statement);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlProvider#getCql()
	 */
	public String getCql() {
		return this.statement.getQuery();
	}
}
