package org.springframework.data.cassandra.test.integration.config;

import org.springframework.cassandra.config.CassandraSessionFactoryBean;
import org.springframework.cassandra.core.CassandraOperations;
import org.springframework.cassandra.core.CassandraTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.config.java.AbstractSpringDataCassandraConfiguration;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.CassandraDataOperations;
import org.springframework.data.cassandra.core.CassandraDataTemplate;
import org.springframework.data.cassandra.mapping.CassandraMappingContext;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;

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

	@Override
	@Bean
	public Cluster cluster() {

		Builder builder = Cluster.builder();
		builder.addContactPoint("127.0.0.1").withPort(9042);
		return builder.build();
	}

	@Bean
	public CassandraSessionFactoryBean sessionFactoryBean() {

		CassandraSessionFactoryBean bean = new CassandraSessionFactoryBean();
		bean.setCluster(cluster());
		bean.setKeyspaceName(getKeyspaceName());
		return bean;
	}

	@Bean
	public CassandraOperations cassandraTemplate() {

		CassandraOperations template = new CassandraTemplate(sessionFactoryBean().getObject());
		return template;
	}

	@Bean
	public CassandraConverter cassandraConverter() {

		CassandraConverter converter = new MappingCassandraConverter(new CassandraMappingContext());

		return converter;

	}

	@Bean
	public CassandraDataOperations cassandraDataTemplate() throws ClassNotFoundException {

		CassandraDataOperations template = new CassandraDataTemplate(sessionFactoryBean().getObject(), converter(),
				keyspaceName);

		return template;

	}
}
