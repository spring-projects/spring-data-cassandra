package org.springframework.cassandra.test.integration.config.java;

import org.springframework.cassandra.config.java.AbstractCassandraConfiguration;
import org.springframework.context.annotation.Configuration;

@Configuration
public class Config extends AbstractCassandraConfiguration {

	@Override
	protected String getKeyspaceName() {
		return null;
	}
}
