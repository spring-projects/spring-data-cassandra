package org.springframework.cassandra.test.integration.config.java;

import javax.inject.Inject;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.cassandra.test.integration.config.IntegrationTestUtils;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.datastax.driver.core.Session;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = KeyspaceCreatingJavaConfig.class)
public class KeyspaceCreatingJavaConfigTest extends AbstractIntegrationTest {

	@Inject
	protected Session session;

	@Override
	protected String keyspace() {
		return null;
	}

	@Test
	public void test() {
		Assert.assertNotNull(session);
		IntegrationTestUtils.assertKeyspaceExists(KeyspaceCreatingJavaConfig.KEYSPACE_NAME, session);
	}
}
