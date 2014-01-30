package org.springframework.cassandra.test.integration.config.java;

import org.springframework.cassandra.config.CassandraSessionFactoryBean;
import org.springframework.cassandra.config.java.AbstractSessionConfiguration;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;

@Configuration
public abstract class AbstractKeyspaceCreatingConfiguration extends AbstractSessionConfiguration {

	@Override
	public CassandraSessionFactoryBean session() throws Exception {

		createKeyspaceIfNecessary();

		return super.session();
	}

	protected void createKeyspaceIfNecessary() throws Exception {
		String keyspace = getKeyspaceName();
		if (!StringUtils.hasText(keyspace)) {
			return;
		}

		Session system = cluster().getObject().connect();
		KeyspaceMetadata kmd = system.getCluster().getMetadata().getKeyspace(keyspace);
		if (kmd != null) {
			return;
		}

		// TODO: use KeyspaceBuilder to build keyspace with attributes & options

		system.execute("CREATE KEYSPACE " + keyspace
				+ " WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
		system.shutdown();
	}
}
