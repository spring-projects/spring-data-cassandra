package org.springframework.cassandra.test.integration.config.xml;

import javax.inject.Inject;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.cassandra.test.integration.AbstractEmbeddedCassandraIntegrationTest;
import org.springframework.cassandra.test.integration.config.IntegrationTestUtils;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.datastax.driver.core.Session;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class FullySpecifiedKeyspaceCreatingXmlConfigTest extends AbstractEmbeddedCassandraIntegrationTest {

	@Override
	protected String keyspace() {
		return null;
	}

	@Inject
	Session s;

	@Test
	public void test() {
		IntegrationTestUtils.assertKeyspaceExists("full1", s);
		IntegrationTestUtils.assertKeyspaceExists("full2", s);
		IntegrationTestUtils.assertKeyspaceExists("script1", s);
		IntegrationTestUtils.assertKeyspaceExists("script2", s);
	}
}
