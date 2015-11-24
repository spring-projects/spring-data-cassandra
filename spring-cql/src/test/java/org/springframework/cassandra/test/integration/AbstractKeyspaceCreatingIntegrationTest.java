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
package org.springframework.cassandra.test.integration;

import org.junit.After;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cassandra.core.CqlOperations;
import org.springframework.cassandra.core.CqlTemplate;
import org.springframework.util.StringUtils;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Session.State;

/**
 * Abstract base integration test class that creates a keyspace
 * 
 * @author Matthew T. Adams
 * @author David Webb
 */
public abstract class AbstractKeyspaceCreatingIntegrationTest extends AbstractEmbeddedCassandraIntegrationTest {

	static Logger log = LoggerFactory.getLogger(AbstractKeyspaceCreatingIntegrationTest.class);

	/**
	 * The session that's connected to the keyspace used in the current instance's test.
	 */
	protected static Session session;

	/**
	 * The name of the keyspace to use for this test instance.
	 */
	protected String keyspace;

	public AbstractKeyspaceCreatingIntegrationTest() {
		this(randomKeyspaceName());
	}

	public AbstractKeyspaceCreatingIntegrationTest(String keyspace) {

		this.keyspace = keyspace;
		ensureKeyspaceAndSession();
	}

	/**
	 * Returns whether we're currently connected to the keyspace.
	 */
	public static boolean connected() {
		return session != null;
	}

	/**
	 * Whether to drop the keyspace that was created after the test has completed. Subclasses should override and return
	 * true, since this default implementation returns false.
	 */
	public boolean dropKeyspaceAfterTest() {
		return false;
	}

	public void ensureKeyspaceAndSession() {

		// ensure that test keyspace exists

		if (!StringUtils.hasText(keyspace)) {
			keyspace = null;
		}

		if (keyspace != null) {
			// see if we need to create the keyspace
			KeyspaceMetadata kmd = cluster.getMetadata().getKeyspace(keyspace);
			if (kmd == null) { // then create keyspace

				String cql = "CREATE KEYSPACE " + keyspace
						+ " WITH durable_writes = false AND replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};";
				log.info("creating keyspace {} via CQL [{}]", keyspace, cql);

				system.execute(cql);
			}
		}

		// keyspace now exists; ensure the session is using it
		if (session == null) {

			log.info("connecting to keyspace {}", keyspace == null ? "system" : keyspace + "...");

			session = keyspace == null ? cluster.connect() : cluster.connect(keyspace);

			log.info("connected to keyspace {}", keyspace == null ? "system" : keyspace);

		} else {

			debugSession();

			log.info("session already connected to a keyspace; attempting to change to use {}", keyspace);

			String cql = "USE " + (keyspace == null ? "system" : keyspace) + ";";

			log.debug(cql);

			getTemplate().execute(cql);

			log.info("now using keyspace " + keyspace);

		}
	}

	protected static CqlOperations getTemplate() {

		return new CqlTemplate(session);
	}

	protected static void debugSession() {
		if (session == null) {
			log.warn("Session is null...cannot debug that");
			return;
		}

		State state = session.getState();

		for (Host h : state.getConnectedHosts()) {

			log.debug(String.format("Session Host dc [%s], rack [%s], ver [%s], state [%s]", h.getDatacenter(), h.getRack(),
					h.getCassandraVersion(), h.getState()));
		}

	}

	@After
	public void after() {
		if (dropKeyspaceAfterTest() && keyspace != null) {

			session.execute("USE system");

			log.info("dropping keyspace {} ...", keyspace);

			system.execute("DROP KEYSPACE " + keyspace);

			log.info("dropped keyspace {}", keyspace);
		}
	}
}
