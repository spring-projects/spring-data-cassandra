package org.springframework.data.cassandra.test.integration.forcequote.config;

import org.junit.Test;
import org.springframework.test.context.ContextConfiguration;

@ContextConfiguration
public class ForceQuotedRepositoryXmlConfigIntegrationTests extends ForceQuotedRepositoryIntegrationTestsDelegator {

	@Test
	public void testExplicitPropertiesWithXmlValues() {
		// these values must match the values in
		// src/test/resources/org/springframework/data/cassandra/test/integration/forcequote/config/ForceQuotedRepositoryXmlConfigIntegrationTests-context.xml
		tests.testExplicitProperties("XmlStringValue", "XmlPrimaryKey");
	}
}
