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

import java.io.IOException;
import java.util.UUID;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cassandra.test.integration.support.SpringCqlBuildProperties;
import org.springframework.cassandra.test.unit.support.Utils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.springframework.util.SocketUtils;

/**
 * Abstract base integration test class that starts an embedded Cassandra instance.
 * 
 * @author Matthew T. Adams
 */
public class AbstractEmbeddedCassandraIntegrationTest {

	public static String uuid() {
		return UUID.randomUUID().toString();
	}

	static Logger log = LoggerFactory.getLogger(AbstractEmbeddedCassandraIntegrationTest.class);

	protected static String CASSANDRA_CONFIG = "spring-cassandra.yaml";
	protected static SpringCqlBuildProperties PROPS = new SpringCqlBuildProperties();
	protected static int CASSANDRA_NATIVE_PORT = PROPS.getCassandraPort();
	protected static String CASSANDRA_HOST = PROPS.getCassandraHost();

	/**
	 * The {@link Cluster} that's connected to Cassandra.
	 */
	protected static Cluster cluster;

	/**
	 * The session connected to the system keyspace.
	 */
	protected static Session system;



	public static String randomKeyspaceName() {
		return Utils.randomKeyspaceName();
	}

	@BeforeClass
	public static void startCassandra() throws TTransportException, IOException, InterruptedException,
			ConfigurationException {

		if(PROPS.getCassandraType() == SpringCqlBuildProperties.CassandraType.EMBEDDED) {
			System.setProperty("com.sun.management.jmxremote.port", "" + SocketUtils.findAvailableTcpPort(1024));
			EmbeddedCassandraServerHelper.startEmbeddedCassandra(CASSANDRA_CONFIG);
		}
	}

	public static Cluster cluster() {
		return Cluster.builder().addContactPoint(CASSANDRA_HOST).withPort(CASSANDRA_NATIVE_PORT).build();
	}

	/**
	 * Ensures that the cluster is created and that the session {@link #SYSTEM} is connected to it.
	 */
	public static void ensureClusterConnection() {

		// check cluster
		if (cluster == null) {
			final Cluster cluster = cluster();
			final Session session = cluster.connect();

			Runtime.getRuntime().addShutdownHook(new Thread(){
				@Override
				public void run() {
					session.close();
					cluster.close();
				}
			});

			AbstractEmbeddedCassandraIntegrationTest.cluster = cluster;
			AbstractEmbeddedCassandraIntegrationTest.system = session;
		}
	}

	public AbstractEmbeddedCassandraIntegrationTest() {
		ensureClusterConnection();
	}
}
