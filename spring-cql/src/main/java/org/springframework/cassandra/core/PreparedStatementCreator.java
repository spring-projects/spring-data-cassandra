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

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.DriverException;

/**
 * Creates a PreparedStatement for the usage with the DataStax Java Driver
 * 
 * @author David Webb
 */
public interface PreparedStatementCreator {

	/**
	 * Create a statement in this session. Allows implementations to use PreparedStatements. The CassandraTemlate will
	 * attempt to cache the PreparedStatement for future use without the overhead of re-preparing on the entire cluster.
	 * 
	 * @param session Session to use to create statement
	 * @return a prepared statement
	 * @throws DriverException there is no need to catch DriverException that may be thrown in the implementation of this
	 *           method. The CassandraTemlate class will handle them.
	 */
	PreparedStatement createPreparedStatement(Session session) throws DriverException;

}
