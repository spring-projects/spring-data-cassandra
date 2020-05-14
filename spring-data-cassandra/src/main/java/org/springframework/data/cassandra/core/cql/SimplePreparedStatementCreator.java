/*
 * Copyright 2013-2020 the original author or authors.
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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;

import org.springframework.util.Assert;

/**
 * Trivial implementation of {@link PreparedStatementCreator}. This prepared statement creator simply prepares a
 * statement from the CQL string.
 * <p>
 * This implementation is useful for testing. It should not be used in production systems with high volume reads and
 * writes. Use {@link CachedPreparedStatementCreator} When preparing statements with Cassandra, each Statement should be
 * prepared once and only once due to the overhead of preparing the statement.
 *
 * @author David Webb
 * @author Mark Paluch
 */
public class SimplePreparedStatementCreator implements PreparedStatementCreator, CqlProvider {

	private final String cql;

	/**
	 * Create a {@link SimplePreparedStatementCreator} given {@code cql}.
	 *
	 * @param cql must not be {@literal null}.
	 */
	public SimplePreparedStatementCreator(String cql) {

		Assert.notNull(cql, "CQL is required to create a PreparedStatement");

		this.cql = cql;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cqlProvider#getCql()
	 */
	public String getCql() {
		return this.cql;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.PreparedStatementCreator#createPreparedStatement(com.datastax.oss.driver.api.core.CqlSession)
	 */
	@Override
	public PreparedStatement createPreparedStatement(CqlSession session) throws DriverException {
		return session.prepare(this.cql);
	}
}
