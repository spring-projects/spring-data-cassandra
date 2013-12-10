package org.springframework.cassandra.test.integration.config;

import static org.junit.Assert.assertNotNull;

import com.datastax.driver.core.Session;

public class IntegrationTestUtils {

	public static void assertSession(Session session) {
		assertNotNull(session);
	}

	public static void assertKeyspaceExists(String keyspace, Session session) {
		assertNotNull(session.getCluster().getMetadata().getKeyspace(keyspace));
	}
}
