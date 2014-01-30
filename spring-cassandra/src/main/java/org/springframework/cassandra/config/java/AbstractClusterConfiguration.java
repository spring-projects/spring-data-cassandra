package org.springframework.cassandra.config.java;

import java.util.Collections;
import java.util.List;

import org.springframework.cassandra.config.CassandraClusterFactoryBean;
import org.springframework.cassandra.config.CompressionType;
import org.springframework.cassandra.core.keyspace.CreateKeyspaceSpecification;
import org.springframework.cassandra.core.keyspace.DropKeyspaceSpecification;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.datastax.driver.core.policies.RetryPolicy;

/**
 * Base class for Spring Cassandra configuration that can handle creating namespaces, execute arbitrary CQL on startup &
 * shutdown, and optionally drop namespaces.
 * 
 * @author Matthew T. Adams
 */
@Configuration
public abstract class AbstractClusterConfiguration {

	@Bean
	public CassandraClusterFactoryBean cluster() throws Exception {

		CassandraClusterFactoryBean bean = new CassandraClusterFactoryBean();
		bean.setAuthProvider(getAuthProvider());
		bean.setCompressionType(getCompressionType());
		bean.setContactPoints(getContactPoints());
		bean.setKeyspaceCreations(getKeyspaceCreations());
		bean.setKeyspaceDrops(getKeyspaceDrops());
		bean.setLoadBalancingPolicy(getLoadBalancingPolicy());
		bean.setMetricsEnabled(getMetricsEnabled());
		bean.setPort(getPort());
		bean.setReconnectionPolicy(getReconnectionPolicy());
		bean.setPoolingOptions(getPoolingOptions());
		bean.setRetryPolicy(getRetryPolicy());
		bean.setShutdownScripts(getShutdownScripts());
		bean.setSocketOptions(getSocketOptions());
		bean.setStartupScripts(getStartupScripts());

		return bean;
	}

	protected List<String> getStartupScripts() {
		return Collections.emptyList();
	}

	protected SocketOptions getSocketOptions() {
		return null;
	}

	protected List<String> getShutdownScripts() {
		return Collections.emptyList();
	}

	protected ReconnectionPolicy getReconnectionPolicy() {
		return null;
	}

	protected RetryPolicy getRetryPolicy() {
		return null;
	}

	protected PoolingOptions getPoolingOptions() {
		return null;
	}

	protected int getPort() {
		return CassandraClusterFactoryBean.DEFAULT_PORT;
	}

	protected boolean getMetricsEnabled() {
		return CassandraClusterFactoryBean.DEFAULT_METRICS_ENABLED;
	}

	protected LoadBalancingPolicy getLoadBalancingPolicy() {
		return null;
	}

	protected List<DropKeyspaceSpecification> getKeyspaceDrops() {
		return Collections.emptyList();
	}

	protected List<CreateKeyspaceSpecification> getKeyspaceCreations() {
		return Collections.emptyList();
	}

	protected String getContactPoints() {
		return CassandraClusterFactoryBean.DEFAULT_CONTACT_POINTS;
	}

	protected CompressionType getCompressionType() {
		return null;
	}

	protected AuthProvider getAuthProvider() {
		return null;
	}
}
