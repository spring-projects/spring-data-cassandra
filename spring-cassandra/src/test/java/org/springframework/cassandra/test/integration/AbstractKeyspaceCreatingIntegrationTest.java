package org.springframework.cassandra.test.integration;

import org.junit.After;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;

/**
 * Abstract base integration test class that creates a keyspace
 * 
 * @author Matthew T. Adams
 */
public abstract class AbstractKeyspaceCreatingIntegrationTest extends AbstractEmbeddedCassandraIntegrationTest {

	static Logger log = LoggerFactory.getLogger(AbstractKeyspaceCreatingIntegrationTest.class);

	/**
	 * The session that's connected to the keyspace used in the current instance's test.
	 */
	protected static Session SESSION;

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
		return SESSION != null;
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
			KeyspaceMetadata kmd = CLUSTER.getMetadata().getKeyspace(keyspace);
			if (kmd == null) { // then create keyspace

				String cql = "CREATE KEYSPACE " + keyspace
						+ " WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};";
				log.info("creating keyspace {} via CQL [{}]", keyspace, cql);

				SYSTEM.execute(cql);
			}
		}

		// keyspace now exists; ensure the session is using it
		if (SESSION == null) {

			log.info("connecting to keyspace {}", keyspace == null ? "system" : keyspace + "...");

			SESSION = keyspace == null ? CLUSTER.connect() : CLUSTER.connect(keyspace);

			log.info("connected to keyspace {}", keyspace == null ? "system" : keyspace);

		} else {

			log.info("session already connected to a keyspace; attempting to change to use {}", keyspace);

			String cql = "USE " + (keyspace == null ? "system" : keyspace) + ";";
			SESSION.execute(cql);

			log.info("now using keyspace " + keyspace);
		}
	}

	@After
	public void after() {
		if (dropKeyspaceAfterTest() && keyspace != null) {

			SESSION.execute("USE system");

			log.info("dropping keyspace {} ...", keyspace);

			SYSTEM.execute("DROP KEYSPACE " + keyspace);

			log.info("dropped keyspace {}", keyspace);
		}
	}
}
