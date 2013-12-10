package org.springframework.cassandra.test.integration.config.java;

import org.junit.Test;
import org.springframework.cassandra.test.integration.config.IntegrationTestUtils;
import org.springframework.test.context.ContextConfiguration;

@ContextConfiguration(classes = KeyspaceCreatingConfig.class)
public class KeyspaceCreatingConfigTest extends AbstractIntegrationTest {

	@Test
	public void test() {
		IntegrationTestUtils.assertKeyspaceExists(KeyspaceCreatingConfig.KEYSPACE, session);
	}
}
