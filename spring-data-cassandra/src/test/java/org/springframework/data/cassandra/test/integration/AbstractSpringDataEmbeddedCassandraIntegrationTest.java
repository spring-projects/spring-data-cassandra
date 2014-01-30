package org.springframework.data.cassandra.test.integration;

import org.springframework.cassandra.test.integration.AbstractEmbeddedCassandraIntegrationTest;
import org.springframework.data.cassandra.test.integration.support.SpringDataBuildProperties;

public class AbstractSpringDataEmbeddedCassandraIntegrationTest extends AbstractEmbeddedCassandraIntegrationTest {

	static {
		// override necessary superclass statics

		SpringDataBuildProperties props = new SpringDataBuildProperties();
		CASSANDRA_NATIVE_PORT = props.getCassandraPort();
	}
}
