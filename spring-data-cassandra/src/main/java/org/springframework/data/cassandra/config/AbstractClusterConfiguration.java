/*
 * Copyright 2013-2018 the original author or authors.
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
package org.springframework.data.cassandra.config;

import java.util.Collections;
import java.util.List;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.core.cql.keyspace.CreateKeyspaceSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.DropKeyspaceSpecification;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.NettyOptions;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.TimestampGenerator;
import com.datastax.driver.core.policies.AddressTranslator;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.policies.SpeculativeExecutionPolicy;

/**
 * Base class for Spring Cassandra configuration that can handle creating namespaces, execute arbitrary CQL on startup &
 * shutdown, and optionally drop keyspaces.
 *
 * @author Matthew T. Adams
 * @author Jorge Davison
 * @author Mark Paluch
 * @author John Blum
 */
@Configuration
public abstract class AbstractClusterConfiguration {

	/**
	 * Returns the initialized {@link Cluster} instance.
	 *
	 * @return the {@link Cluster}.
	 * @throws IllegalStateException if the cluster factory is not initialized.
	 */
	protected Cluster getRequiredCluster() {

		CassandraClusterFactoryBean factoryBean = cluster();
		Assert.state(factoryBean.getObject() != null, "Cluster factory not initialized");

		return factoryBean.getObject();
	}

	/**
	 * Creates a {@link CassandraClusterFactoryBean} that provides a Cassandra {@link com.datastax.driver.core.Cluster}.
	 * The lifecycle of {@link CassandraClusterFactoryBean} executes {@link #getStartupScripts() startup} and
	 * {@link #getShutdownScripts() shutdown} scripts.
	 *
	 * @return the {@link CassandraClusterFactoryBean}.
	 * @see #cluster()
	 * @see #getStartupScripts()
	 * @see #getShutdownScripts()
	 */
	@Bean
	public CassandraClusterFactoryBean cluster() {

		CassandraClusterFactoryBean bean = new CassandraClusterFactoryBean();

		bean.setAddressTranslator(getAddressTranslator());
		bean.setAuthProvider(getAuthProvider());
		bean.setClusterBuilderConfigurer(getClusterBuilderConfigurer());
		bean.setClusterName(getClusterName());
		bean.setCompressionType(getCompressionType());
		bean.setContactPoints(getContactPoints());
		bean.setLoadBalancingPolicy(getLoadBalancingPolicy());
		bean.setMaxSchemaAgreementWaitSeconds(getMaxSchemaAgreementWaitSeconds());
		bean.setMetricsEnabled(getMetricsEnabled());
		bean.setNettyOptions(getNettyOptions());
		bean.setPoolingOptions(getPoolingOptions());
		bean.setPort(getPort());
		bean.setProtocolVersion(getProtocolVersion());
		bean.setQueryOptions(getQueryOptions());
		bean.setReconnectionPolicy(getReconnectionPolicy());
		bean.setRetryPolicy(getRetryPolicy());
		bean.setSpeculativeExecutionPolicy(getSpeculativeExecutionPolicy());
		bean.setSocketOptions(getSocketOptions());
		bean.setTimestampGenerator(getTimestampGenerator());

		bean.setKeyspaceCreations(getKeyspaceCreations());
		bean.setKeyspaceDrops(getKeyspaceDrops());
		bean.setStartupScripts(getStartupScripts());
		bean.setShutdownScripts(getShutdownScripts());

		return bean;
	}

	/**
	 * Returns the {@link AddressTranslator}.
	 *
	 * @return the {@link AddressTranslator}; may be {@literal null}.
	 * @since 1.5
	 */
	@Nullable
	protected AddressTranslator getAddressTranslator() {
		return null;
	}

	/**
	 * Returns the {@link AuthProvider}.
	 *
	 * @return the {@link AuthProvider}, may be {@literal null}.
	 */
	@Nullable
	protected AuthProvider getAuthProvider() {
		return null;
	}

	/**
	 * Returns the {@link ClusterBuilderConfigurer}.
	 *
	 * @return the {@link ClusterBuilderConfigurer}; may be {@literal null}.
	 * @since 1.5
	 */
	@Nullable
	protected ClusterBuilderConfigurer getClusterBuilderConfigurer() {
		return null;
	}

	/**
	 * Returns the cluster name.
	 *
	 * @return the cluster name; may be {@literal null}.
	 * @since 1.5
	 */
	@Nullable
	protected String getClusterName() {
		return null;
	}

	/**
	 * Returns the {@link CompressionType}.
	 *
	 * @return the {@link CompressionType}, may be {@literal null}.
	 */
	@Nullable
	protected CompressionType getCompressionType() {
		return null;
	}

	/**
	 * Returns the Cassandra contact points. Defaults to {@code localhost}
	 *
	 * @return the Cassandra contact points
	 * @see CassandraClusterFactoryBean#DEFAULT_CONTACT_POINTS
	 */
	protected String getContactPoints() {
		return CassandraClusterFactoryBean.DEFAULT_CONTACT_POINTS;
	}

	/**
	 * Returns the {@link LoadBalancingPolicy}.
	 *
	 * @return the {@link LoadBalancingPolicy}, may be {@literal null}.
	 */
	@Nullable
	protected LoadBalancingPolicy getLoadBalancingPolicy() {
		return null;
	}

	/**
	 * Returns the maximum schema agreement wait in seconds.
	 *
	 * @return the maximum schema agreement wait in seconds; default to {@literal 10} seconds.
	 */
	protected int getMaxSchemaAgreementWaitSeconds() {
		return CassandraClusterFactoryBean.DEFAULT_MAX_SCHEMA_AGREEMENT_WAIT_SECONDS;
	}

	/**
	 * Returns the whether to enable metrics. Defaults to {@literal true}
	 *
	 * @return {@literal true} to enable metrics.
	 * @see CassandraClusterFactoryBean#DEFAULT_METRICS_ENABLED
	 */
	protected boolean getMetricsEnabled() {
		return CassandraClusterFactoryBean.DEFAULT_METRICS_ENABLED;
	}

	/**
	 * Returns the {@link NettyOptions}. Defaults to {@link NettyOptions#DEFAULT_INSTANCE}.
	 *
	 * @return the {@link NettyOptions} to customize netty behavior.
	 * @since 1.5
	 */
	protected NettyOptions getNettyOptions() {
		return NettyOptions.DEFAULT_INSTANCE;
	}

	/**
	 * Returns the {@link PoolingOptions}.
	 *
	 * @return the {@link PoolingOptions}, may be {@literal null}.
	 */
	@Nullable
	protected PoolingOptions getPoolingOptions() {
		return null;
	}

	/**
	 * Returns the Cassandra port. Defaults to {@code 9042}.
	 *
	 * @return the Cassandra port
	 * @see CassandraClusterFactoryBean#DEFAULT_PORT
	 */
	protected int getPort() {
		return CassandraClusterFactoryBean.DEFAULT_PORT;
	}

	/**
	 * Returns the {@link ProtocolVersion}. Defaults to {@link ProtocolVersion#NEWEST_SUPPORTED}.
	 *
	 * @return the {@link ProtocolVersion}.
	 * @see ProtocolVersion#NEWEST_SUPPORTED.
	 */
	protected ProtocolVersion getProtocolVersion() {
		return ProtocolVersion.NEWEST_SUPPORTED;
	}

	/**
	 * Returns the {@link QueryOptions}.
	 *
	 * @return the {@link QueryOptions}, may be {@literal null}.
	 * @since 1.5
	 */
	@Nullable
	protected QueryOptions getQueryOptions() {
		return null;
	}

	/**
	 * Returns the {@link ReconnectionPolicy}.
	 *
	 * @return the {@link ReconnectionPolicy}, may be {@literal null}.
	 */
	@Nullable
	protected ReconnectionPolicy getReconnectionPolicy() {
		return null;
	}

	/**
	 * Returns the {@link RetryPolicy}.
	 *
	 * @return the {@link RetryPolicy}, may be {@literal null}.
	 */
	@Nullable
	protected RetryPolicy getRetryPolicy() {
		return null;
	}

	/**
	 * Returns the {@link SpeculativeExecutionPolicy}.
	 *
	 * @return the {@link SpeculativeExecutionPolicy}; may be {@literal null}.
	 * @since 1.5
	 */
	@Nullable
	protected SpeculativeExecutionPolicy getSpeculativeExecutionPolicy() {
		return null;
	}

	/**
	 * Returns the {@link SocketOptions}.
	 *
	 * @return the {@link SocketOptions}, may be {@literal null}.
	 */
	@Nullable
	protected SocketOptions getSocketOptions() {
		return null;
	}

	/**
	 * Returns the {@link TimestampGenerator}.
	 *
	 * @return the {@link TimestampGenerator}; may be {@literal null}.
	 * @since 1.5
	 */
	@Nullable
	protected TimestampGenerator getTimestampGenerator() {
		return null;
	}

	/**
	 * Returns the list of keyspace creations to be run right after {@link com.datastax.driver.core.Cluster}
	 * initialization.
	 *
	 * @return the list of keyspace creations, may be empty but never {@link null}
	 */
	protected List<CreateKeyspaceSpecification> getKeyspaceCreations() {
		return Collections.emptyList();
	}

	/**
	 * Returns the list of keyspace drops to be run before {@link com.datastax.driver.core.Cluster} shutdown.
	 *
	 * @return the list of keyspace drops, may be empty but never {@link null}
	 */
	protected List<DropKeyspaceSpecification> getKeyspaceDrops() {
		return Collections.emptyList();
	}

	/**
	 * Returns the list of startup scripts to be run after {@link #getKeyspaceCreations() keyspace creations} and after
	 * {@link com.datastax.driver.core.Cluster} initialization.
	 *
	 * @return the list of startup scripts, may be empty but never {@link null}
	 */
	protected List<String> getStartupScripts() {
		return Collections.emptyList();
	}

	/**
	 * Returns the list of shutdown scripts to be run after {@link #getKeyspaceDrops() keyspace drops} and right before
	 * {@link com.datastax.driver.core.Cluster} shutdown.
	 *
	 * @return the list of shutdown scripts, may be empty but never {@link null}
	 */
	protected List<String> getShutdownScripts() {
		return Collections.emptyList();
	}
}
