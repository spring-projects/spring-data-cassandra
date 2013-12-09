package org.springframework.cassandra.test.integration.config;

import org.springframework.cassandra.config.java.AbstractCassandraConfiguration;
import org.springframework.context.annotation.Configuration;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;

@Configuration
public class Config extends AbstractCassandraConfiguration {

	public Cluster cluster;

	@Override
	protected String getKeyspaceName() {
		return null;
	}

	@Override
	public Cluster cluster() {
		Builder builder = Cluster.builder();

		builder.addContactPoint("localhost").withPort(9042);

		return cluster = builder.build();
	}
}
