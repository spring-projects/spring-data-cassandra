package org.springframework.cassandra.test.integration.config.java;

import org.springframework.cassandra.config.KeyspaceAttributes;
import org.springframework.cassandra.config.PoolingOptionsConfig;
import org.springframework.cassandra.config.SocketOptionsConfig;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;

@Configuration
public abstract class AbstractKeyspaceCreatingConfiguration extends AbstractIntegrationTestConfiguration {

	@Override
	public Session session() {

		createKeyspaceIfNecessary();

		return super.session();
	}

	protected void createKeyspaceIfNecessary() {
		String keyspace = getKeyspaceName();
		if (!StringUtils.hasText(keyspace)) {
			return;
		}

		Session system = cluster().connect();
		KeyspaceMetadata kmd = system.getCluster().getMetadata().getKeyspace(keyspace);
		if (kmd != null) {
			return;
		}

		// TODO: use KeyspaceBuilder to build keyspace with attributes & options

		system.execute("CREATE KEYSPACE " + keyspace
				+ " WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
		system.shutdown();
	}

	protected KeyspaceAttributes getKeyspaceAttributes() {
		return null;
	}

	protected PoolingOptionsConfig getLocalPoolingOptionsConfig() {
		return null;
	}

	protected PoolingOptionsConfig getRemotePoolingOptionsConfig() {
		return null;
	}

	protected SocketOptionsConfig getSocketOptionsConfig() {
		return null;
	}
}
