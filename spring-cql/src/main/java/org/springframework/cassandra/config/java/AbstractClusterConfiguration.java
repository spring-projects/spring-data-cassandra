/*
 * Copyright 2013-2014 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.cassandra.config.java;

import java.util.Collections;
import java.util.List;

import org.springframework.cassandra.config.CassandraCqlClusterFactoryBean;
import org.springframework.cassandra.config.CompressionType;
import org.springframework.cassandra.core.keyspace.CreateKeyspaceSpecification;
import org.springframework.cassandra.core.keyspace.DropKeyspaceSpecification;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolVersion;
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
	public CassandraCqlClusterFactoryBean cluster() {

		CassandraCqlClusterFactoryBean bean = new CassandraCqlClusterFactoryBean();
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
		bean.setProtocolVersion(getProtocolVersion());

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
		return CassandraCqlClusterFactoryBean.DEFAULT_PORT;
	}

	protected boolean getMetricsEnabled() {
		return CassandraCqlClusterFactoryBean.DEFAULT_METRICS_ENABLED;
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
		return CassandraCqlClusterFactoryBean.DEFAULT_CONTACT_POINTS;
	}

	protected CompressionType getCompressionType() {
		return null;
	}

	protected AuthProvider getAuthProvider() {
		return null;
	}

	protected ProtocolVersion getProtocolVersion(){
		return null;
    }
}
