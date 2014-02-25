package org.springframework.data.cassandra.test.integration.forcequote.compositeprimarykey;

import org.junit.Test;
import org.springframework.test.context.ContextConfiguration;

@ContextConfiguration
public class ForceQuotedCompositePrimaryKeyRepositoryXmlConfigIntegrationTests extends
		ForceQuotedCompositePrimaryKeyRepositoryIntegrationTestsDelegator {

	@Test
	public void testExplicit() {
		testExplicit(String.format("\"%s\"", "XmlExplicitTable"), String.format("\"%s\"", "XmlExplicitStringValue"),
				String.format("\"%s\"", "XmlExplicitKeyZero"), String.format("\"%s\"", "XmlExplicitKeyOne"));
	}
}
