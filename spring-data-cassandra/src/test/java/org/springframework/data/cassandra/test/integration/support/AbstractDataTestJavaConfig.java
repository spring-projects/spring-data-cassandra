package org.springframework.data.cassandra.test.integration.support;

import org.springframework.data.cassandra.config.java.AbstractSpringDataCassandraConfiguration;

public abstract class AbstractDataTestJavaConfig extends AbstractSpringDataCassandraConfiguration {

	public static final SpringDataBuildProperties PROPS = new SpringDataBuildProperties();
	public static final int PORT = PROPS.getCassandraPort();

	@Override
	protected int getPort() {
		return PORT;
	}
}
