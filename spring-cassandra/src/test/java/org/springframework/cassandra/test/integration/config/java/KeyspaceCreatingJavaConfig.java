package org.springframework.cassandra.test.integration.config.java;

import java.util.ArrayList;
import java.util.List;

import org.springframework.cassandra.config.KeyspaceAttributes;
import org.springframework.cassandra.core.keyspace.CreateKeyspaceSpecification;
import org.springframework.cassandra.core.keyspace.KeyspaceOption;
import org.springframework.cassandra.test.integration.support.AbstractTestJavaConfig;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KeyspaceCreatingJavaConfig extends AbstractTestJavaConfig {

	public static final String KEYSPACE_NAME = "foo";

	@Override
	protected String getKeyspaceName() {
		return KEYSPACE_NAME;
	}

	@Override
	protected List<CreateKeyspaceSpecification> getKeyspaceCreations() {
		ArrayList<CreateKeyspaceSpecification> list = new ArrayList<CreateKeyspaceSpecification>();

		CreateKeyspaceSpecification specification = CreateKeyspaceSpecification.createKeyspace().name(getKeyspaceName());
		specification.with(KeyspaceOption.REPLICATION, KeyspaceAttributes.newSimpleReplication(1L));

		list.add(specification);
		return list;
	}
}
