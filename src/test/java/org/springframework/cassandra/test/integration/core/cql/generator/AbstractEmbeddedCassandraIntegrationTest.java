package org.springframework.cassandra.test.integration.core.cql.generator;

import java.io.IOException;
import java.util.UUID;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.Before;
import org.junit.BeforeClass;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;

public abstract class AbstractEmbeddedCassandraIntegrationTest {

	@BeforeClass
	public static void beforeClass() throws ConfigurationException, TTransportException, IOException,
			InterruptedException {
		EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra.yaml");
	}

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

	/**
	 * Returns whether we're currently connected to the cluster.
	 */
	public boolean connected() {
		return session != null;
	}

	public Cluster cluster() {
		return Cluster.builder().addContactPoint("localhost").withPort(9042).build();
	}

	@Before
	public void before() {
		if (connect && !connected()) {
			cluster = cluster();

			if (keyspace == null) {
				session = cluster.connect();
			} else {

				KeyspaceMetadata kmd = cluster.getMetadata().getKeyspace(keyspace);
				if (kmd == null) { // then create keyspace
					session = cluster.connect();
					session.execute("CREATE KEYSPACE " + keyspace
							+ " WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};");
					session.execute("USE " + keyspace + ";");
				} // else keyspace already exists
			}
		}
	}
}
