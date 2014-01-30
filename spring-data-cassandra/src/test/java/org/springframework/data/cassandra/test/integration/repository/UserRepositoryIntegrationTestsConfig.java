package org.springframework.data.cassandra.test.integration.repository;

import static org.springframework.cassandra.core.keyspace.CreateKeyspaceSpecification.createKeyspace;

import java.util.ArrayList;
import java.util.List;

import org.springframework.cassandra.core.keyspace.CreateKeyspaceSpecification;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.config.SchemaAction;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;
import org.springframework.data.cassandra.test.integration.support.AbstractDataTestJavaConfig;

@Configuration
@EnableCassandraRepositories(basePackageClasses = UserRepository.class)
public class UserRepositoryIntegrationTestsConfig extends AbstractDataTestJavaConfig {

	@Override
	protected String getKeyspaceName() {
		return UserRepositoryIntegrationTests.class.getSimpleName();
	}

	@Override
	protected List<CreateKeyspaceSpecification> getKeyspaceCreations() {
		List<CreateKeyspaceSpecification> creates = new ArrayList<CreateKeyspaceSpecification>();

		creates.add(createKeyspace().name(getKeyspaceName()).withSimpleReplication());

		return creates;
	}

	@Override
	public SchemaAction getSchemaAction() {
		return SchemaAction.RECREATE;
	}

	@Override
	public String getMappingBasePackage() {
		return User.class.getPackage().getName();
	}
}
