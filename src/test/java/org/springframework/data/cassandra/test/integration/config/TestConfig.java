package org.springframework.data.cassandra.test.integration.config;

import org.springframework.cassandra.core.CassandraOperations;
import org.springframework.cassandra.core.CassandraTemplate;
import org.springframework.cassandra.core.SessionFactoryBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.config.AbstractCassandraConfiguration;
import org.springframework.data.cassandra.core.CassandraDataOperations;
import org.springframework.data.cassandra.core.CassandraDataTemplate;
import org.springframework.data.cassandra.core.CassandraKeyspaceFactoryBean;

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

	public static final String keyspace = "test";

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.config.AbstractCassandraConfiguration#getKeyspaceName()
	 */
	@Override
	protected String getKeyspaceName() {
		return keyspace;
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
	public SessionFactoryBean sessionFactoryBean() {

		SessionFactoryBean bean = new SessionFactoryBean(keyspaceFactoryBean().getObject());
		return bean;

	}

	@Bean
	public CassandraOperations cassandraTemplate() {

		CassandraOperations template = new CassandraTemplate(sessionFactoryBean().getObject());
		return template;
	}

	@Bean
	public CassandraDataOperations cassandraDataTemplate() {

		CassandraDataOperations template = new CassandraDataTemplate(keyspaceFactoryBean().getObject().getSession(),
				keyspaceFactoryBean().getObject().getCassandraConverter(), keyspaceFactoryBean().getObject().getKeyspace());

		return template;

	}
}
