/*
 * Copyright 2017-2018 the original author or authors.
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
package org.springframework.data.cassandra.test.util;

import org.junit.rules.ExternalResource;
import org.springframework.data.cassandra.support.RandomKeySpaceName;
import org.springframework.util.Assert;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 * Class rule to prepare a keyspace to give tests a keyspace context. This rule uses {@link CassandraRule} to obtain a
 * Cassandra connection context. It can be used as {@link org.junit.ClassRule} and {@link org.junit.rules.TestRule}.
 * <p>
 * This rule maintains the keyspace throughout the test lifecycle. The keyspace is created when running the preparing
 * {@link #before()} methods. At the same time, the {@link #getSession() session} is logged into the created keyspace
 * and can be used for further interaction during the test. {@link #after()} the test is finished this rule drops the
 * keyspace.
 * <p>
 * Neither {@link Cluster} nor {@link Session} should be closed outside by any caller otherwise the rule cannot perform
 * its cleanup after the test run.
 *
 * @author Mark Paluch
 */
public class KeyspaceRule extends ExternalResource {

	private final CassandraRule cassandraRule;
	private Session session;
	private final String keyspaceName;

	/**
	 * Create a {@link KeyspaceRule} initialized with a {@link CassandraRule} for creating a keyspace using a random name.
	 *
	 * @param cassandraRule
	 */
	public KeyspaceRule(CassandraRule cassandraRule) {
		this(cassandraRule, RandomKeySpaceName.create());
	}

	/**
	 * Create a {@link KeyspaceRule} initialized with a {@link CassandraRule} for creating a keyspace using the given
	 * {@code keyspaceName}.
	 *
	 * @param cassandraRule
	 * @param keyspaceName
	 */
	public KeyspaceRule(CassandraRule cassandraRule, String keyspaceName) {

		Assert.notNull(cassandraRule, "CassandraRule must not be null!");
		Assert.hasText(keyspaceName, "KeyspaceName must not be empty!");

		this.keyspaceName = keyspaceName;
		this.cassandraRule = cassandraRule;
	}

	@Override
	protected void before() throws Throwable {

		// Support initialized and initializing CassandraRule.
		if (cassandraRule.getCluster() != null) {
			this.session = cassandraRule.getSession();
		} else {
			cassandraRule.before(session -> {
				KeyspaceRule.this.session = cassandraRule.getSession();
				return null;
			});
		}

		Assert.state(session != null, "Session was not initialized");

		session.execute(String.format("CREATE KEYSPACE %s WITH durable_writes = false AND "
				+ "replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};", keyspaceName));
		session.execute(String.format("USE %s;", keyspaceName));
	}

	@Override
	protected void after() {

		session.execute("USE system;");
		session.execute(String.format("DROP KEYSPACE %s;", keyspaceName));
	}

	/**
	 * Returns the {@link Session}. The session state can be initialized and pointing to a keyspace other than
	 * {@code system}.
	 *
	 * @return
	 */
	public Session getSession() {
		return session;
	}

	/**
	 * Returns the keyspace name.
	 *
	 * @return
	 */
	public String getKeyspaceName() {
		return keyspaceName;
	}
}
