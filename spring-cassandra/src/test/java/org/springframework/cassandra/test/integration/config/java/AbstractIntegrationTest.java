package org.springframework.cassandra.test.integration.config.java;

import javax.inject.Inject;

import org.junit.Before;
import org.junit.runner.RunWith;
import org.springframework.cassandra.test.integration.AbstractEmbeddedCassandraIntegrationTest;
import org.springframework.cassandra.test.integration.config.IntegrationTestUtils;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.datastax.driver.core.Session;

@RunWith(SpringJUnit4ClassRunner.class)
public abstract class AbstractIntegrationTest extends AbstractEmbeddedCassandraIntegrationTest {

	@Inject
	protected Session session;

	@Before
	public void assertSession() {
		IntegrationTestUtils.assertSession(session);
	}
}
