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
 * @author John Blum
 */
@TestKeyspaceName
public abstract class AbstractKeyspaceCreatingIntegrationTests extends IntegrationTestsSupport {

	/**
	 * The Session that's connected to the Cassandra Keyspace used in tests. Must not be closed.
	 */
	protected @TestKeyspace CqlSession session;

	/**
	 * The name of the Cassanda Keyspace to use for this test.
	 */
	protected @TestKeyspace String keyspace;

	/**
	 * Returns the configured {@link String name} of the Cassandra Keyspace used for tests.
	 *
	 * @return the confiured {@link String name} of the Cassandra Keyspace used for tests.
	 */
	protected String getKeyspace() {
		return this.keyspace;
	}

	/**
	 * Returns the configured {@link CqlSession}. The {@link CqlSession} is logged into the {@link #getKeyspace()}.
	 *
	 * @return the configured {@link Session}.
	 * @see com.datastax.driver.core.Session
	 */
	protected CqlSession getSession() {
		return this.session;
	}

	@SuppressWarnings("unused")
	protected void dropKeyspace() {
		dropKeyspace(getKeyspace());
	}

	/**
	 * Drops the given Keyspace by {@link String name} if it exists.
	 *
	 * @param keyspace {@link String name} of the Keyspace to drop.
	 */
	protected void dropKeyspace(String keyspace) {
		this.session.execute(String.format(TestKeyspaceDelegate.DROP_KEYSPACE_IF_EXISTS_CQL, keyspace));
	}
}
