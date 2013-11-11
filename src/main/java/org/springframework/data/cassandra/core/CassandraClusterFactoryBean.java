/*
 * Copyright 2011-2013 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.cassandra.config.CompressionType;
import org.springframework.data.cassandra.config.PoolingOptionsConfig;
import org.springframework.data.cassandra.config.SocketOptionsConfig;
import org.springframework.util.StringUtils;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.datastax.driver.core.policies.RetryPolicy;

/**
 * Convenient factory for configuring a Cassandra Cluster.
 * 
 * @author Alex Shvid
 */

public class CassandraClusterFactoryBean implements FactoryBean<Cluster>,
		InitializingBean, DisposableBean, PersistenceExceptionTranslator {

	private static final int DEFAULT_PORT = 9042;
	
	private Cluster cluster;
	
	private String contactPoints;
	private int port = DEFAULT_PORT;
	private CompressionType compressionType;
	
	private PoolingOptionsConfig localPoolingOptions;
	private PoolingOptionsConfig remotePoolingOptions;
	private SocketOptionsConfig socketOptions;
	
	private AuthProvider authProvider;
	private LoadBalancingPolicy loadBalancingPolicy;
	private ReconnectionPolicy reconnectionPolicy;
	private RetryPolicy retryPolicy;
	
	private boolean metricsEnabled = true;
	
	private final PersistenceExceptionTranslator exceptionTranslator = new CassandraExceptionTranslator();

	public Cluster getObject() throws Exception {
		return cluster;
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#getObjectType()
	 */
	public Class<? extends Cluster> getObjectType() {
		return Cluster.class;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#isSingleton()
	 */
	public boolean isSingleton() {
		return true;
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.springframework.dao.support.PersistenceExceptionTranslator#translateExceptionIfPossible(java.lang.RuntimeException)
	 */
	public DataAccessException translateExceptionIfPossible(RuntimeException ex) {
		return exceptionTranslator.translateExceptionIfPossible(ex);
	}
	
	/* 
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() throws Exception {
		
		if (!StringUtils.hasText(contactPoints)) {
			throw new IllegalArgumentException(
					"at least one server is required");
		}
		
		Cluster.Builder builder = Cluster.builder();

		builder.addContactPoints(StringUtils.commaDelimitedListToStringArray(contactPoints)).withPort(port);
		
		if (compressionType != null) {
			builder.withCompression(convertCompressionType(compressionType));
		}

		if (localPoolingOptions != null) {
			builder.withPoolingOptions(configPoolingOptions(HostDistance.LOCAL, localPoolingOptions));
		}

		if (remotePoolingOptions != null) {
			builder.withPoolingOptions(configPoolingOptions(HostDistance.REMOTE, remotePoolingOptions));
		}

		if (socketOptions != null) {
			builder.withSocketOptions(configSocketOptions(socketOptions));
		}
		
		if (authProvider != null) {
			builder.withAuthProvider(authProvider);
		}
		
		if (loadBalancingPolicy != null) {
			builder.withLoadBalancingPolicy(loadBalancingPolicy);
		}
		
		if (reconnectionPolicy != null) {
			builder.withReconnectionPolicy(reconnectionPolicy);
		}
		
		if (retryPolicy != null) {
			builder.withRetryPolicy(retryPolicy);
		}
		
		if (!metricsEnabled) {
			builder.withoutMetrics();
		}	
		
		Cluster cluster = builder.build();
		
		// initialize property
		this.cluster = cluster;
	}
	
	/* 
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.DisposableBean#destroy()
	 */
	public void destroy() throws Exception {
		this.cluster.shutdown();
	}

	public void setContactPoints(String contactPoints) {
		this.contactPoints = contactPoints;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public void setCompressionType(CompressionType compressionType) {
		this.compressionType = compressionType;
	}
	
	public void setLocalPoolingOptions(PoolingOptionsConfig localPoolingOptions) {
		this.localPoolingOptions = localPoolingOptions;
	}

	public void setRemotePoolingOptions(PoolingOptionsConfig remotePoolingOptions) {
		this.remotePoolingOptions = remotePoolingOptions;
	}

	public void setSocketOptions(SocketOptionsConfig socketOptions) {
		this.socketOptions = socketOptions;
	}

	public void setAuthProvider(AuthProvider authProvider) {
		this.authProvider = authProvider;
	}
	
	public void setLoadBalancingPolicy(LoadBalancingPolicy loadBalancingPolicy) {
		this.loadBalancingPolicy = loadBalancingPolicy;
	}

	public void setReconnectionPolicy(ReconnectionPolicy reconnectionPolicy) {
		this.reconnectionPolicy = reconnectionPolicy;
	}

	public void setRetryPolicy(RetryPolicy retryPolicy) {
		this.retryPolicy = retryPolicy;
	}

	public void setMetricsEnabled(boolean metricsEnabled) {
		this.metricsEnabled = metricsEnabled;
	}

	private static Compression convertCompressionType(CompressionType type) {
		switch(type) {
		case none:
			return Compression.NONE;		
		case snappy:
			return Compression.SNAPPY;
		}
		throw new IllegalArgumentException("unknown compression type " + type);
	}
	
	private static PoolingOptions configPoolingOptions(HostDistance hostDistance, PoolingOptionsConfig config) {
		PoolingOptions poolingOptions = new PoolingOptions();

		if (config.getMinSimultaneousRequests() != null) {
			poolingOptions.setMinSimultaneousRequestsPerConnectionThreshold(hostDistance, config.getMinSimultaneousRequests());
		}
		if (config.getMaxSimultaneousRequests() != null) {
			poolingOptions.setMaxSimultaneousRequestsPerConnectionThreshold(hostDistance, config.getMaxSimultaneousRequests());
		}
		if (config.getCoreConnections() != null) {
			poolingOptions.setCoreConnectionsPerHost(hostDistance, config.getCoreConnections());
		}
		if (config.getMaxConnections() != null) {
			poolingOptions.setMaxConnectionsPerHost(hostDistance, config.getMaxConnections());
		}
		
		return poolingOptions;
	}
	
	private static SocketOptions configSocketOptions(SocketOptionsConfig config) {
		SocketOptions socketOptions = new SocketOptions();
		
		if (config.getConnectTimeoutMls() != null) {
			socketOptions.setConnectTimeoutMillis(config.getConnectTimeoutMls());
		}
		if (config.getKeepAlive() != null) {
			socketOptions.setKeepAlive(config.getKeepAlive());
		}
		if (config.getReuseAddress() != null) {
			socketOptions.setReuseAddress(config.getReuseAddress());
		}
		if (config.getSoLinger() != null) {
			socketOptions.setSoLinger(config.getSoLinger());
		}
		if (config.getTcpNoDelay() != null) {
			socketOptions.setTcpNoDelay(config.getTcpNoDelay());
		}
		if (config.getReceiveBufferSize() != null) {
			socketOptions.setReceiveBufferSize(config.getReceiveBufferSize());
		}
		if (config.getSendBufferSize() != null) {
			socketOptions.setSendBufferSize(config.getSendBufferSize());
		}
		
		return socketOptions;
	}
}
