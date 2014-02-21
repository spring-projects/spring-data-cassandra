package org.springframework.data.cassandra.test.integration.forcequote.compositeprimarykey;

import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;
import org.springframework.data.cassandra.test.integration.support.IntegrationTestConfig;
import org.springframework.test.context.ContextConfiguration;

@ContextConfiguration
public class ForceQuotedCompositePrimaryKeyRepositoryJavaConfigIntegrationTests extends
		ForceQuotedCompositePrimaryKeyRepositoryIntegrationTestsDelegator {

	@Configuration
	@EnableCassandraRepositories(basePackageClasses = ImplicitRepository.class)
	public static class Config extends IntegrationTestConfig {
	}
}
