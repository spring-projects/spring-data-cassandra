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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.DriverException;

/**
 * Created a PreparedStatement and retrieved the PreparedStatement from cache if the statement has been prepared
 * previously. In general, this creator should be used over the {@link SimplePreparedStatementCreator} as it provides
 * better performance.
 * 
 * <p>
 * There is overhead in Cassandra when Preparing a Statement. This is negligible on a single data center configuration,
 * but when your cluster spans multiple data centers, preparing the same statement over and over again is not necessary
 * and causes performance issues in high throughput use cases.
 * </p>
 * 
 * @author David Webb
 * 
 */
public class CachedPreparedStatementCreator implements PreparedStatementCreator, CqlProvider {

	private static Logger log = LoggerFactory.getLogger(CachedPreparedStatementCreator.class);

	private final String cql;

	private PreparedStatement cache;

	/**
	 * Create a CachedPreparedStatementCreator from the provided CQL.
	 * 
	 * @param cql
	 */
	public CachedPreparedStatementCreator(String cql) {
		Assert.notNull(cql, "CQL is required to create a PreparedStatement");
		this.cql = cql;
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.PreparedStatementCreator#createPreparedStatement(com.datastax.driver.core.Session)
	 */
	@Override
	public PreparedStatement createPreparedStatement(Session session) throws DriverException {
		if (cache == null) {
			log.debug("PreparedStatement cache is null, preparing new Statement");
			cache = session.prepare(getCql());
		} else {
			log.debug("Using cached PreparedStatement");
		}
		return cache;
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.CqlProvider#getCql()
	 */
	@Override
	public String getCql() {
		return this.cql;
	}

}
