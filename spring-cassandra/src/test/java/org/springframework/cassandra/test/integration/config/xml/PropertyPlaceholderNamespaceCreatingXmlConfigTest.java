package org.springframework.cassandra.test.integration.config.xml;

import static org.junit.Assert.assertNotNull;

import javax.inject.Inject;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.cassandra.core.CqlOperations;
import org.springframework.cassandra.test.integration.AbstractKeyspaceCreatingIntegrationTest;
import org.springframework.cassandra.test.integration.config.IntegrationTestUtils;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.datastax.driver.core.Session;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class PropertyPlaceholderNamespaceCreatingXmlConfigTest extends AbstractKeyspaceCreatingIntegrationTest {

	@Inject
	Session s;

	@Inject
	CqlOperations ops;

	@Test
	public void test() {
		IntegrationTestUtils.assertSession(s);

		IntegrationTestUtils.assertKeyspaceExists("ppncxct", s);

		assertNotNull(ops);
	}

}
