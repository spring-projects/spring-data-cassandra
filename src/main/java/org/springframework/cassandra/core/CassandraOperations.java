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


import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;

/**
 * Operations for interacting with Cassandra. These operations are used by the Repository implementation, but can also
 * be used directly when that is desired by the developer.
 * 
 * @author Alex Shvid
 * @author David Webb
 * @author Matthew Adams
 * 
 */
public interface CassandraOperations {

	<T> T execute(SessionCallback<T> sessionCallback);

	void execute(final String cql);

	RuntimeException potentiallyConvertRuntimeException(RuntimeException ex);

	/**
	 * Execute query and return Cassandra ResultSet
	 * 
	 * @param cql must not be {@literal null}.
	 * @return
	 */
	ResultSet executeQuery(final String cql);

	/**
	 * Execute async query and return Cassandra ResultSetFuture
	 * 
	 * @param cql must not be {@literal null}.
	 * @return
	 */
	ResultSetFuture executeQueryAsynchronously(final String cql);

}
