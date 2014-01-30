package org.springframework.data.cassandra.test.integration.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.config.java.AbstractSpringDataCassandraConfiguration;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.CassandraAdminOperations;
import org.springframework.data.cassandra.core.CassandraAdminTemplate;
import org.springframework.data.cassandra.mapping.DefaultCassandraMappingContext;
import org.springframework.data.cassandra.test.integration.support.SpringDataBuildProperties;

/**
 * Setup any spring configuration for unit tests
 * 
 * @author David Webb
 * @author Matthew T. Adams
 */
@Configuration
public class TestConfig extends AbstractSpringDataCassandraConfiguration {

	public static final SpringDataBuildProperties PROPS = new SpringDataBuildProperties();
	public static final int PORT = PROPS.getCassandraPort();
	public static final int RPC_PORT = PROPS.getCassandraRpcPort();

	public static final String KEYSPACE_NAME = "test";

	@Override
	protected String getKeyspaceName() {
		return KEYSPACE_NAME;
	}

	@Override
	protected int getPort() {
		return PORT;
	}

	@Bean
	public CassandraConverter cassandraConverter() {
		return new MappingCassandraConverter(new DefaultCassandraMappingContext());
	}

	@Override
	@Bean
	public CassandraAdminOperations cassandraTemplate() throws Exception {
		return new CassandraAdminTemplate(session().getObject(), converter());
	}
}
