package org.springframework.cassandra.test.integration.config.java;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cassandra.config.java.AbstractCqlTemplateConfiguration;
import org.springframework.cassandra.core.CqlTemplate;
import org.springframework.cassandra.test.integration.AbstractKeyspaceCreatingIntegrationTest;
import org.springframework.cassandra.test.integration.config.IntegrationTestUtils;
import org.springframework.cassandra.test.unit.support.Utils;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class CqlTemplateConfigIntegrationTest extends AbstractKeyspaceCreatingIntegrationTest {

	public static final String KEYSPACE_NAME = Utils.randomKeyspaceName();

	@Configuration
	public static class Config extends AbstractCqlTemplateConfiguration {

		@Override
		protected String getKeyspaceName() {
			return KEYSPACE_NAME;
		}

		@Override
		protected int getPort() {
			return CASSANDRA_NATIVE_PORT;
		}
	}

	@Autowired
	CqlTemplate template;

	public CqlTemplateConfigIntegrationTest() {
		super(KEYSPACE_NAME);
	}

	@Test
	public void test() {
		IntegrationTestUtils.assertCqlTemplate(template);
	}
}
