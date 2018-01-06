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

import org.junit.ClassRule;
import org.springframework.util.Assert;

import com.datastax.driver.core.Session;

/**
 * Abstract base integration test class that provides a keyspace during the test runtime.
 * <p>
 * Keyspaces are created and removed by this base class. The {@link #getKeyspace() keyspace} and {@link #getSession()
 * session} are provided by this class during the test lifecycle (before test/test/after test). The keyspace is retained
 * until the whole test class is completed. Any tables created in a test will be visible in subsequent tests of the same
 * class.
 * <p>
 * This class is intended to be subclassed by integration test classes.
 *
 * @author Matthew T. Adams
 * @author David Webb
 * @author Mark Paluch
 */
public abstract class AbstractKeyspaceCreatingIntegrationTest extends AbstractEmbeddedCassandraIntegrationTest {

	/**
	 * Class rule to prepare a keyspace to give tests a keyspace context. The keyspace name is random and changes per
	 * test.
	 */
	@ClassRule public static final KeyspaceRule keyspaceRule = new KeyspaceRule(cassandraEnvironment);

	/**
	 * The session that's connected to the keyspace used in the current instance's test.
	 */
	protected Session session;

	/**
	 * The name of the keyspace to use for this test instance.
	 */
	protected final String keyspace;

	/**
	 * Create a new {@link AbstractKeyspaceCreatingIntegrationTest}.
	 */
	public AbstractKeyspaceCreatingIntegrationTest() {
		this(keyspaceRule.getKeyspaceName());
	}

	private AbstractKeyspaceCreatingIntegrationTest(final String keyspace) {

		Assert.hasText(keyspace, "Keyspace must not be empty");

		this.keyspace = keyspace;
		this.session = keyspaceRule.getSession();

		cassandraRule.before(session -> {

			if (!keyspace.equals(session.getLoggedKeyspace())) {
				session.execute(String.format("USE %s;", keyspace));
			}
			return null;
		});
	}

	/**
	 * Returns the {@link Session}. The session is logged into the {@link #getKeyspace()}.
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
	public String getKeyspace() {
		return keyspace;
	}

	/**
	 * Drop a Keyspace if it exists.
	 *
	 * @param keyspace
	 */
	public void dropKeyspace(String keyspace) {
		session.execute("DROP KEYSPACE IF EXISTS " + keyspace);
	}
}
