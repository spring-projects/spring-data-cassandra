package org.springframework.data.cassandra.test.integration.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.config.java.AbstractSpringDataCassandraConfiguration;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.CassandraDataOperations;
import org.springframework.data.cassandra.core.CassandraDataTemplate;
import org.springframework.data.cassandra.mapping.CassandraMappingContext;

/**
 * Setup any spring configuration for unit tests
 * 
 * @author David Webb
 * @author Matthew T. Adams
 */
@Configuration
public class TestConfig extends AbstractSpringDataCassandraConfiguration {

	public static final String keyspaceName = "test";

	@Override
	protected String getKeyspaceName() {
		return keyspaceName;
	}

	@Bean
	public CassandraConverter cassandraConverter() {
		return new MappingCassandraConverter(new CassandraMappingContext());
	}

	@Bean
	public CassandraDataOperations cassandraDataTemplate() throws Exception {
		return new CassandraDataTemplate(session().getObject(), converter(), keyspaceName);
	}
}
