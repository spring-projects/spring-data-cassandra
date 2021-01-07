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

import java.util.UUID;

import org.junit.ClassRule;
import org.junit.Rule;

import org.springframework.data.cassandra.support.CqlDataSet;

import com.datastax.driver.core.Cluster;

/**
 * Abstract base integration test class that starts an embedded Cassandra instance. Test clients can use the
 * {@link #cluster} instance to create sessions and get access. Expect the {@link #cluster} instance to be closed once
 * the test has been run.
 * <p>
 * This class is intended to be subclassed by integration test classes.
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
public abstract class AbstractEmbeddedCassandraIntegrationTest {

	/**
	 * Initiate a Cassandra environment in this test scope.
	 */
	@ClassRule
	public static final CassandraRule cassandraEnvironment =
			new CassandraRule("embedded-cassandra.yaml");

	/**
	 * Create and return a random {@link UUID} as a {@link String}.
	 *
	 * @return a random {@link UUID} as a {@link String}.
	 * @see java.util.UUID#randomUUID()
	 */
	public static String uuid() {
		return UUID.randomUUID().toString();
	}

	/**
	 * Initiate a Cassandra environment in test scope.
	 */
	@Rule
	public final CassandraRule cassandraRule = cassandraEnvironment.testInstance().before(session -> {
		AbstractEmbeddedCassandraIntegrationTest.this.cluster = session.getCluster();
		return null;
	});

	/**
	 * The {@link Cluster} connected to Cassandra.
	 */
	protected Cluster cluster;

	/**
	 * Returns the {@link Cluster} instance.
	 *
	 * @return an instance of {@link Cluster} connected to Cassandra.
	 */
	public Cluster getCluster() {
		return this.cluster;
	}

	/**
	 * Executes a CQL script from a classpath resource in the given {@code keyspace}.
	 *
	 * @param cqlResourceName {@link String resource name} of the CQL script to apply.
	 * @param keyspace {@link String name} of the Cassandra Keyspace in which to apply the CQL script.
	 */
	public void execute(String cqlResourceName, String keyspace) {

		CqlDataSet cqlDataSet = CqlDataSet.fromClassPath(cqlResourceName).executeIn(keyspace);

		this.cassandraRule.execute(cqlDataSet);
	}
}
