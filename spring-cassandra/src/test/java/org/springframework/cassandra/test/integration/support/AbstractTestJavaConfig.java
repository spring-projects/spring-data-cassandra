package org.springframework.cassandra.test.integration.support;

import org.springframework.cassandra.config.java.AbstractCassandraConfiguration;
import org.springframework.context.annotation.Configuration;

@Configuration
public abstract class AbstractTestJavaConfig extends AbstractCassandraConfiguration {

	public static BuildProperties PROPS = new BuildProperties();
	public static final int PORT = PROPS.getCassandraPort();

	@Override
	protected int getPort() {
		return PORT;
	}
}
