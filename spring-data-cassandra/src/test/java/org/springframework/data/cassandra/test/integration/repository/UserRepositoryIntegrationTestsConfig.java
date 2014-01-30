package org.springframework.data.cassandra.test.integration.repository;

import java.util.ArrayList;
import java.util.List;

import org.springframework.cassandra.core.keyspace.CreateKeyspaceSpecification;
import org.springframework.data.cassandra.test.integration.support.AbstractDataTestJavaConfig;

public class UserRepositoryIntegrationTestsConfig extends AbstractDataTestJavaConfig {

	@Override
	protected String getKeyspaceName() {
		return UserRepositoryIntegrationTests.class.getSimpleName();
	}

	@Override
	protected List<CreateKeyspaceSpecification> getKeyspaceCreations() {
		List<CreateKeyspaceSpecification> creates = new ArrayList<CreateKeyspaceSpecification>();

		// TODO

		return creates;
	}
}
