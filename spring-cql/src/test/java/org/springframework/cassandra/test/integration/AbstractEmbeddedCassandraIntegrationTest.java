/*
 * Copyright 2013-2016 the original author or authors.
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
package org.springframework.cassandra.test.integration;

import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.springframework.cassandra.core.SessionCallback;
import org.springframework.cassandra.test.integration.support.CassandraConnectionProperties;
import org.springframework.cassandra.test.unit.support.Utils;
import org.springframework.dao.DataAccessException;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 * Abstract base integration test class that starts an embedded Cassandra instance.
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
public class AbstractEmbeddedCassandraIntegrationTest {

	protected static CassandraConnectionProperties PROPS = new CassandraConnectionProperties();
	private static String CASSANDRA_CONFIG = "embedded-cassandra.yaml";

	/**
	 * The {@link Cluster} that's connected to Cassandra.
	 */
	protected Cluster cluster;

	/**
	 * The session connected to the system keyspace.
	 */
	protected Session system;

	@Rule public final CassandraRule cassandraRule = new CassandraRule(CASSANDRA_CONFIG)
			.before(new SessionCallback<Object>() {
				@Override
				public Object doInSession(Session s) throws DataAccessException {
					AbstractEmbeddedCassandraIntegrationTest.this.cluster = s.getCluster();
					AbstractEmbeddedCassandraIntegrationTest.this.system = s;
					return null;
				}
			});

	public static String randomKeyspaceName() {
		return Utils.randomKeyspaceName();
	}

	public static String uuid() {
		return UUID.randomUUID().toString();
	}

	public static Cluster cluster() {
		return Cluster.builder().addContactPoint(PROPS.getCassandraHost()).withPort(PROPS.getCassandraPort()).build();
	}

	/**
	 * Starts the embedded Cassandra instance if it's needed.
	 *
	 * @throws Exception
	 */
	@BeforeClass
	public static void startCassandraIfNeeded() throws Exception {

		// initialize Cassandra Rule here to start before anything else is started.
		// A @Rule would be the better option but there one thing to solve before:
		// The Spring container boots before the TestRule.before method is called
		// - a custom runner might not be the best solution, so that's the only pain-point in starting the embedded server.

		new CassandraRule(CASSANDRA_CONFIG).startCassandraIfNeeded();
	}
}
