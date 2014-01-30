package org.springframework.cassandra.config.java;

import org.springframework.cassandra.config.CassandraSessionFactoryBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.datastax.driver.core.Cluster;

/**
 * Base class for Spring Cassandra configuration that can handle creating namespaces, execute arbitrary CQL on startup &
 * shutdown, and optionally drop namespaces.
 * 
 * @author Matthew T. Adams
 */
@Configuration
public abstract class AbstractSessionConfiguration extends AbstractClusterConfiguration {

	protected abstract String getKeyspaceName();

	@Bean
	public CassandraSessionFactoryBean session() throws Exception {

		Cluster cluster = cluster().getObject();

		CassandraSessionFactoryBean bean = new CassandraSessionFactoryBean();
		bean.setCluster(cluster);
		bean.setKeyspaceName(getKeyspaceName());

		return bean;
	}
}
