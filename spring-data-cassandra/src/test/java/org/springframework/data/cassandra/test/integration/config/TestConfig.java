package org.springframework.data.cassandra.test.integration.config;

import org.springframework.cassandra.test.unit.support.Utils;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.config.java.AbstractSpringDataCassandraConfiguration;
import org.springframework.data.cassandra.test.integration.support.SpringDataCassandraBuildProperties;

/**
 * Setup any spring configuration for unit tests
 * 
 * @author David Webb
 * @author Matthew T. Adams
 */
@Configuration
public class TestConfig extends AbstractSpringDataCassandraConfiguration {

	public static final SpringDataCassandraBuildProperties PROPS = new SpringDataCassandraBuildProperties();
	public static final int PORT = PROPS.getCassandraPort();
	public static final int RPC_PORT = PROPS.getCassandraRpcPort();

	public static final String KEYSPACE_NAME = Utils.randomKeyspaceName();

	@Override
	protected String getKeyspaceName() {
		return KEYSPACE_NAME;
	}

	@Override
	protected int getPort() {
		return PORT;
	}
}
