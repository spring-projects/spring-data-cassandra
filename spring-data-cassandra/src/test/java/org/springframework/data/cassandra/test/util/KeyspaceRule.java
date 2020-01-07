/*
 * Copyright 2017-2020 the original author or authors.
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

import org.junit.rules.ExternalResource;

import org.springframework.data.cassandra.support.RandomKeyspaceName;
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

	static final String CREATE_KEYSPACE_CQL =
		"CREATE KEYSPACE %s WITH durable_writes = false AND replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};";

	static final String DROP_KEYSPACE_CQL = "DROP KEYSPACE %s;";
	static final String DROP_KEYSPACE_IF_EXISTS_CQL = String.format(DROP_KEYSPACE_CQL, "IF EXISTS %s");
	static final String USE_KEYSPACE_CQL = "USE %s;";

	private final CassandraRule cassandraRule;

	private Session session;

	private final String keyspaceName;

	/**
	 * Constructs a new instance of {@link KeyspaceRule} initialized with a {@link CassandraRule}
	 * to create a Cassandra Keyspace using a random name.
	 *
	 * @param cassandraRule {@link CassandraRule} used to setup the Cassandra environment.
	 * @throws IllegalArgumentException if {@link CassandraRule} is {@literal null}.
	 * @see org.springframework.data.cassandra.support.RandomKeyspaceName
	 * @see org.springframework.data.cassandra.test.util.CassandraRule
	 */
	public KeyspaceRule(CassandraRule cassandraRule) {
		this(cassandraRule, RandomKeyspaceName.create());
	}

	/**
	 * Constructs a new instance of {@link KeyspaceRule} initialized with a {@link CassandraRule}
	 * to create a Cassandra Keyspace with the given {@code keyspaceName}.
	 *
	 * @param cassandraRule {@link CassandraRule} used to setup the Cassandra environment.
	 * @param keyspaceName {@link String name} of the Cassandra Keyspace to use in tests.
	 * @throws IllegalArgumentException if {@link CassandraRule} is {@literal null}
	 * or the Keyspace name is not specified.
	 * @see org.springframework.data.cassandra.test.util.CassandraRule
	 * @see org.springframework.data.cassandra.test.util.CassandraRule
	 */
	public KeyspaceRule(CassandraRule cassandraRule, String keyspaceName) {

		Assert.notNull(cassandraRule, "CassandraRule must not be null");
		Assert.hasText(keyspaceName, "KeyspaceName must not be empty");

		this.cassandraRule = cassandraRule;
		this.keyspaceName = keyspaceName;

		this.cassandraRule.before(session -> {
			KeyspaceRule.this.session = this.cassandraRule.getSession();
			return null;
		});
	}

	/**
	 * Returns the {@link String name} of the Cassandra Keyspace.
	 *
	 * @return the {@link String name} of the Cassandra keyspace.
	 */
	public String getKeyspaceName() {
		return this.keyspaceName;
	}

	/**
	 * Returns the {@link Session}.
	 *
	 * The {@link Session} state can be initialized and pointing to a Keyspace other than {@code system}.
	 *
	 * @return the current Cassandr {@link Session}.
	 * @see com.datastax.driver.core.Session
	 */
	public Session getSession() {
		return this.session;
	}

	private Session resolveSession() {

		Session session = getSession();

		this.session = session != null ? session : this.cassandraRule.getSession();

		Assert.state(this.session != null, "Session was not initialized");

		return this.session;
	}

	@Override
	protected void before() {

		Session session = resolveSession();

		session.execute(String.format(CREATE_KEYSPACE_CQL, this.keyspaceName));
		session.execute(String.format(USE_KEYSPACE_CQL, this.keyspaceName));
	}

	@Override
	protected void after() {

		Session session = getSession();

		session.execute(String.format(USE_KEYSPACE_CQL, "system"));
		session.execute(String.format(DROP_KEYSPACE_CQL, this.keyspaceName));
	}
}
