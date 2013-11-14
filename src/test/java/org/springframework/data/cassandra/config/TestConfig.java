package org.springframework.data.cassandra.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.core.CassandraKeyspaceFactoryBean;
import org.springframework.data.cassandra.core.CassandraTemplate;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;

/**
 * Setup any spring configuration for unit tests
 * 
 * @author David Webb
 * 
 */
@Configuration
public class TestConfig extends AbstractCassandraConfiguration {

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.config.AbstractCassandraConfiguration#getKeyspaceName()
	 */
	@Override
	protected String getKeyspaceName() {
		return "test";
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.config.AbstractCassandraConfiguration#cluster()
	 */
	@Override
	@Bean
	public Cluster cluster() {

		Builder builder = Cluster.builder();

		builder.addContactPoint("127.0.0.1");

		return builder.build();
	}

	@Bean
	public CassandraKeyspaceFactoryBean keyspaceFactoryBean() {

		CassandraKeyspaceFactoryBean bean = new CassandraKeyspaceFactoryBean();
		bean.setCluster(cluster());
		bean.setKeyspace("test");

		return bean;

	}

	@Bean
	public CassandraTemplate cassandraTemplate() {

		CassandraTemplate template = new CassandraTemplate(keyspaceFactoryBean().getObject());

		return template;

	}

}
