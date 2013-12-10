package org.springframework.cassandra.test.integration.config.java;

import org.springframework.context.annotation.Configuration;

@Configuration
public class Config extends AbstractIntegrationTestConfiguration {

	@Override
	protected String getKeyspaceName() {
		return null;
	}
}
