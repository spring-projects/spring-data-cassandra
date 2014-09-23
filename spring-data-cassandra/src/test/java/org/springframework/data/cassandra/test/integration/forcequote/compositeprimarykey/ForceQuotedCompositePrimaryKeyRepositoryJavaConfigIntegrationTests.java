package org.springframework.data.cassandra.test.integration.forcequote.compositeprimarykey;

import org.junit.Test;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;
import org.springframework.data.cassandra.test.integration.support.IntegrationTestConfig;
import org.springframework.test.context.ContextConfiguration;

@ContextConfiguration
public class ForceQuotedCompositePrimaryKeyRepositoryJavaConfigIntegrationTests extends
		ForceQuotedCompositePrimaryKeyRepositoryIntegrationTestsDelegator {

	@Configuration
	@EnableCassandraRepositories(basePackageClasses = ImplicitRepository.class)
	public static class Config extends IntegrationTestConfig {}

	@Test
	public void testExplicit() {
		testExplicit(String.format("\"%s\"", Explicit.TABLE_NAME),
				String.format("\"%s\"", Explicit.STRING_VALUE_COLUMN_NAME),
				String.format("\"%s\"", ExplicitKey.EXPLICIT_KEY_ZERO), String.format("\"%s\"", ExplicitKey.EXPLICIT_KEY_ONE));
	}
}
