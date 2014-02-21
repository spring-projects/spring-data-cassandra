package org.springframework.data.cassandra.test.integration.forcequote.config;

import org.junit.Test;
import org.springframework.test.context.ContextConfiguration;

@ContextConfiguration
public class ForceQuotedRepositoryXmlConfigIntegrationTests extends ForceQuotedRepositoryIntegrationTestsDelegator {

	// these values must match the values in
	// src/test/resources/org/springframework/data/cassandra/test/integration/forcequote/config/ForceQuotedRepositoryXmlConfigIntegrationTests-context.xml

	@Test
	public void testExplicit() {
		tests.testExplicit("Zz");
	}

	@Test
	public void testExplicitPropertiesWithXmlValues() {
		tests.testExplicitProperties("XmlStringValue", "XmlPrimaryKey");
	}
}
