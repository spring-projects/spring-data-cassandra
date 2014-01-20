package org.springframework.cassandra.test.integration;

import java.io.IOException;
import java.util.UUID;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;

public abstract class AbstractEmbeddedCassandraIntegrationTest {

	private static Logger log = LoggerFactory.getLogger(AbstractEmbeddedCassandraIntegrationTest.class);

	protected final static String CASSANDRA_CONFIG = "spring-cassandra.yaml";
	protected final static String CASSANDRA_HOST = "localhost";
	protected final static int CASSANDRA_NATIVE_PORT = 9042;

	@BeforeClass
	public static void startCassandra() throws ConfigurationException, TTransportException, IOException,
			InterruptedException {
		log.info("Starting Cassandra Embedded Server");
		EmbeddedCassandraServerHelper.startEmbeddedCassandra(CASSANDRA_CONFIG);
	}

	public AbstractEmbeddedCassandraIntegrationTest() {
		if (session == null) {
			connect();
		}
	}

	public AbstractEmbeddedCassandraIntegrationTest(String keyspace) {
		this.keyspace = keyspace;
		if (session == null) {
			connect();
		}
	}

	/**
	 * Whether to clear the cluster before the next test.
	 */
	protected boolean clear = false;
	/**
	 * Whether to connect to Cassandra.
	 */
	protected boolean connect = true;
	/**
	 * The {@link Cluster} that's connected to Cassandra.
	 */
	protected Cluster cluster;
	/**
	 * If not <code>null</code>, get a {@link Session} for the from the {@link #cluster}.
	 */
	protected String keyspace = "ks" + UUID.randomUUID().toString().replace("-", "");

	/**
	 * The {@link Session} for the {@link #keyspace} from the {@link #cluster}.
	 */
	protected Session session;

	protected String keyspace() {
		return keyspace;
	}

	/**
	 * Returns whether we're currently connected to the cluster.
	 */
	public boolean connected() {
		return session != null;
	}

	public Cluster cluster() {
		return Cluster.builder().addContactPoint(CASSANDRA_HOST).withPort(CASSANDRA_NATIVE_PORT).build();
	}

	public void connect() {

		if (connect && !connected()) {

			log.info("Connecting to Cassandra");

			cluster = cluster();

			if (keyspace() == null) {
				session = cluster.connect();
			} else {

				KeyspaceMetadata kmd = cluster.getMetadata().getKeyspace(keyspace());
				if (kmd == null) { // then create keyspace
					session = cluster.connect();
					session.execute("CREATE KEYSPACE " + keyspace()
							+ " WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};");
					session.execute("USE " + keyspace() + ";");
				} else {// else keyspace already exists
					session = cluster.connect(keyspace());
				}
			}
		}
	}

	@After
	public void after() {
		log.info("After: clear -> " + clear + ", connected -> " + connected());
		if (clear && connected()) {
			log.info("Cleaning Cassandra");
			EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
		}
	}

}
