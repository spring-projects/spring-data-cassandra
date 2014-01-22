package org.springframework.cassandra.test.integration.config.xml;

import javax.inject.Inject;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.cassandra.test.integration.AbstractKeyspaceCreatingIntegrationTest;
import org.springframework.cassandra.test.integration.config.IntegrationTestUtils;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.datastax.driver.core.Session;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath:/org/springframework/cassandra/test/integration/config/xml/XmlConfigTest-context.xml")
public class XmlConfigTest extends AbstractKeyspaceCreatingIntegrationTest {

	public static final String KEYSPACE = "xmlconfigtest";

	@Inject
	Session s;

	public XmlConfigTest() {
		super(KEYSPACE);
	}

	@Test
	public void test() {
		IntegrationTestUtils.assertSession(s);
		IntegrationTestUtils.assertKeyspaceExists(KEYSPACE, s);
	}
}
