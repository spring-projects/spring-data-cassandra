package org.springframework.cassandra.test.integration.config.java;

import org.springframework.context.annotation.Configuration;
import org.springframework.cassandra.test.integration.support.AbstractTestJavaConfig;

@Configuration
public class Config extends AbstractTestJavaConfig {

	@Override
	protected String getKeyspaceName() {
		return null;
	}
}
