package org.springframework.cassandra.test.integration;

import java.io.IOException;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cassandra.test.integration.support.SpringCassandraBuildProperties;
import org.springframework.cassandra.test.unit.support.Utils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 * Abstract base integration test class that starts an embedded Cassandra instance.
 * 
 * @author Matthew T. Adams
 */
public class AbstractEmbeddedCassandraIntegrationTest {

	static Logger log = LoggerFactory.getLogger(AbstractEmbeddedCassandraIntegrationTest.class);

	protected static String CASSANDRA_CONFIG = "spring-cassandra.yaml";
	protected static String CASSANDRA_HOST = "localhost";

	protected static SpringCassandraBuildProperties PROPS = new SpringCassandraBuildProperties();
	protected static int CASSANDRA_NATIVE_PORT = PROPS.getCassandraPort();

	/**
	 * The session connected to the system keyspace.
	 */
	protected static Session SYSTEM;
	/**
	 * The {@link Cluster} that's connected to Cassandra.
	 */
	protected static Cluster CLUSTER;

	public static String randomKeyspaceName() {
		return Utils.randomKeyspaceName();
	}

	@BeforeClass
	public static void startCassandra() throws ConfigurationException, TTransportException, IOException,
			InterruptedException {

		EmbeddedCassandraServerHelper.startEmbeddedCassandra(CASSANDRA_CONFIG);
	}

	public static Cluster cluster() {
		return Cluster.builder().addContactPoint(CASSANDRA_HOST).withPort(CASSANDRA_NATIVE_PORT).build();
	}

	/**
	 * Ensures that the cluster is created and that the session {@link #SYSTEM} is connected to it.
	 */
	public static void ensureClusterConnection() {

		// check cluster
		if (CLUSTER == null) {
			CLUSTER = cluster();
		}

		// check system session connected
		if (SYSTEM == null) {
			SYSTEM = CLUSTER.connect();
		}
	}

	public AbstractEmbeddedCassandraIntegrationTest() {
		ensureClusterConnection();
	}
}
