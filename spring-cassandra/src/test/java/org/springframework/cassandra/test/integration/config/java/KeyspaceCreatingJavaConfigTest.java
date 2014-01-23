package org.springframework.cassandra.test.integration.config.java;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.cassandra.test.integration.config.IntegrationTestUtils;
import org.springframework.test.context.ContextConfiguration;

@ContextConfiguration(classes = KeyspaceCreatingJavaConfig.class)
public class KeyspaceCreatingJavaConfigTest extends AbstractIntegrationTest {

	@Test
	public void test() {
		Assert.assertNotNull(session);
		IntegrationTestUtils.assertKeyspaceExists(KeyspaceCreatingJavaConfig.KEYSPACE_NAME, session);
	}
}
