package org.springframework.data.cassandra.test.integration.querymethods.declared.named;

import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;
import org.springframework.data.cassandra.test.integration.querymethods.declared.QueryIntegrationTests;
import org.springframework.test.context.ContextConfiguration;

@ContextConfiguration
public class NamedQueryIntegrationTests extends QueryIntegrationTests {

	@Configuration
	@EnableCassandraRepositories(basePackageClasses = PersonRepositoryWithNamedQueries.class,
			namedQueriesLocation = "classpath:META-INF/PersonRepositoryWithNamedQueries.properties")
	public static class Config extends QueryIntegrationTests.Config {}
}
