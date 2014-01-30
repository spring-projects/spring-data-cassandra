package org.springframework.data.cassandra.test.integration.support;

import org.springframework.cassandra.config.java.AbstractCassandraConfiguration;

public abstract class AbstractDataTestJavaConfig extends AbstractCassandraConfiguration {

	public static SpringDataBuildProperties PROPS = new SpringDataBuildProperties();
	public static final int PORT = PROPS.getCassandraPort();

	@Override
	protected int getPort() {
		return PORT;
	}
}
