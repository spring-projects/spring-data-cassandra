/*
 * Copyright 2017-2021 the original author or authors.
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
package org.springframework.data.cassandra.test.util;

import com.datastax.oss.driver.api.core.CqlSession;

/**
 * Delegate to prepare a keyspace to give tests a keyspace context.
 *
 * @author Mark Paluch
 */
class TestKeyspaceDelegate {

	private static final String CREATE_KEYSPACE_CQL = "CREATE KEYSPACE %s WITH durable_writes = false AND replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};";

	private static final String DROP_KEYSPACE_CQL = "DROP KEYSPACE %s;";
	static final String DROP_KEYSPACE_IF_EXISTS_CQL = String.format(DROP_KEYSPACE_CQL, "IF EXISTS %s");
	static final String USE_KEYSPACE_CQL = "USE %s;";

	static void before(CqlSession session, String keyspaceName) {

		session.execute(String.format(CREATE_KEYSPACE_CQL, keyspaceName));
		session.execute(String.format(USE_KEYSPACE_CQL, keyspaceName));
	}

	static void after(CqlSession session, String keyspaceName) {

		session.execute(String.format(USE_KEYSPACE_CQL, "system"));
		session.execute(String.format(DROP_KEYSPACE_CQL, keyspaceName));
	}
}
