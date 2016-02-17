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

import org.junit.After;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cassandra.core.CqlTemplate;
import org.springframework.cassandra.core.SessionCallback;
import org.springframework.dao.DataAccessException;
import org.springframework.util.Assert;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Session.State;

/**
 * Abstract base integration test class that creates a keyspace
 * 
 * @author Matthew T. Adams
 * @author David Webb
 * @author Mark Paluch
 */
public abstract class AbstractKeyspaceCreatingIntegrationTest extends AbstractEmbeddedCassandraIntegrationTest {

	static Logger log = LoggerFactory.getLogger(AbstractKeyspaceCreatingIntegrationTest.class);

	/**
	 * The session that's connected to the keyspace used in the current instance's test.
	 */
	protected Session session;

	/**
	 * The name of the keyspace to use for this test instance.
	 */
	protected final String keyspace;

	public AbstractKeyspaceCreatingIntegrationTest() {
		this(randomKeyspaceName());
	}

	public AbstractKeyspaceCreatingIntegrationTest(String keyspace) {
		Assert.hasText(keyspace, "Keyspace must not be empty");
		this.keyspace = keyspace;
		cassandraRule.before(new SessionCallback<Object>() {
			@Override
			public Object doInSession(Session s) throws DataAccessException {
				AbstractKeyspaceCreatingIntegrationTest.this.session = s;
		ensureKeyspaceAndSession(s);
				return null;
			}
		});

	}

	private void ensureKeyspaceAndSession(Session session) {

		// ensure that test keyspace exists
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


		debugSession();
		log.info("session already connected to a keyspace; attempting to change to use {}", keyspace);

		String cql = "USE " + (keyspace == null ? "system" : keyspace) + ";";
		log.debug(cql);

		new CqlTemplate(session).execute(cql);
		log.info("now using keyspace " + keyspace);
	}

	protected void debugSession() {
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
	public void dropKeyspaceAfterTest() {

		if (keyspace != null && session != null) {

			session.execute("USE system");
			log.info("dropping keyspace {} ...", keyspace);
			dropKeyspace(keyspace);
			log.info("dropped keyspace {}", keyspace);
		}
	}

	/**
	 * Drop a Keyspace if it exists.
	 * @param keyspace
     */
	public void dropKeyspace(String keyspace) {
		session.execute("DROP KEYSPACE IF EXISTS " + keyspace);
	}
}
