package org.springframework.cassandra.test.integration.config;

import org.junit.Test;
import org.springframework.test.context.ContextConfiguration;

@ContextConfiguration(classes = KeyspaceCreatingConfig.class)
public class KeyspaceCreatingConfigTest extends AbstractIntegrationTest {

	@Test
	public void test() {
		IntegrationTestUtils.assertKeyspaceExists(KeyspaceCreatingConfig.KEYSPACE, session);
	}
}
