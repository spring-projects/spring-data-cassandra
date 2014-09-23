package org.springframework.data.cassandra.test.integration.forcequote.config;

import org.junit.Test;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;
import org.springframework.data.cassandra.test.integration.support.IntegrationTestConfig;
import org.springframework.test.context.ContextConfiguration;

@ContextConfiguration
public class ForceQuotedRepositoryJavaConfigIntegrationTests extends ForceQuotedRepositoryIntegrationTestsDelegator {

	@Configuration
	@EnableCassandraRepositories(basePackageClasses = ForceQuotedRepositoryIntegrationTests.class)
	public static class Config extends IntegrationTestConfig {}

	@Test
	public void testExplicit() {
		tests.testExplicit(Explicit.TABLE_NAME);
	}

	@Test
	public void testExplicitPropertiesWithJavaValues() {
		tests.testExplicitProperties(ExplicitProperties.EXPLICIT_STRING_VALUE, ExplicitProperties.EXPLICIT_PRIMARY_KEY);
	}
}
