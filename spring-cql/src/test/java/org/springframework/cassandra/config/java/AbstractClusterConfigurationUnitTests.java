/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.cassandra.config.java;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.springframework.cassandra.config.CassandraCqlClusterFactoryBean;
import org.springframework.cassandra.config.CompressionType;
import org.springframework.cassandra.core.keyspace.CreateKeyspaceSpecification;
import org.springframework.cassandra.core.keyspace.DropKeyspaceSpecification;
import org.springframework.test.util.ReflectionTestUtils;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;

/**
 * Unit tests for {@link AbstractClusterConfiguration}.
 *
 * @author Mark Paluch
 * @soundtrack Max Graham Feat Neev Kennedy - So Caught Up (Dns Project Remix)
 * @see DATACASS-226
 */
public class AbstractClusterConfigurationUnitTests {

	/**
	 * @see DATACASS-226
	 * @throws Exception
	 */
	@Test
	public void shouldInitializeWithoutAnyOptions() throws Exception {

		CassandraCqlClusterFactoryBean bean = new CassandraCqlClusterFactoryBean();
		bean.afterPropertiesSet();

		AbstractClusterConfiguration clusterConfiguration = new AbstractClusterConfiguration() {};

		Cluster cluster = getCluster(clusterConfiguration);
		assertThat(cluster, is(not(nullValue())));
		assertThat(cluster.isClosed(), is(false));
		assertThat(getConfiguration(cluster).getMetricsOptions(), is(not(nullValue())));
		assertThat(getConfiguration(cluster).getMetricsOptions().isJMXReportingEnabled(), is(true));
	}

	/**
	 * @see DATACASS-226
	 * @throws Exception
	 */
	@Test
	public void shouldSetCompressionType() throws Exception {

		final CompressionType compressionType = CompressionType.SNAPPY;

		AbstractClusterConfiguration clusterConfiguration = new AbstractClusterConfiguration() {
			@Override
			protected CompressionType getCompressionType() {
				return compressionType;
			}
		};

		Cluster cluster = getCluster(clusterConfiguration);
		assertThat(getConfiguration(cluster).getProtocolOptions().getCompression(), is(Compression.SNAPPY));
	}

	/**
	 * @see DATACASS-226
	 * @throws Exception
	 */
	@Test
	public void shouldSetPoolingOptions() throws Exception {

		final PoolingOptions poolingOptions = new PoolingOptions();

		AbstractClusterConfiguration clusterConfiguration = new AbstractClusterConfiguration() {
			@Override
			protected PoolingOptions getPoolingOptions() {
				return poolingOptions;
			}
		};

		Cluster cluster = getCluster(clusterConfiguration);
		assertThat(getConfiguration(cluster).getPoolingOptions(), is(poolingOptions));
	}

	/**
	 * @see DATACASS-226
	 * @throws Exception
	 */
	@Test
	public void shouldSetSocketOptions() throws Exception {

		final SocketOptions socketOptions = new SocketOptions();

		AbstractClusterConfiguration clusterConfiguration = new AbstractClusterConfiguration() {
			@Override
			protected SocketOptions getSocketOptions() {
				return socketOptions;
			}
		};

		Cluster cluster = getCluster(clusterConfiguration);
		assertThat(getConfiguration(cluster).getSocketOptions(), is(socketOptions));
	}

	/**
	 * @see DATACASS-226
	 * @throws Exception
	 */
	@Test
	public void shouldSetQueryOptions() throws Exception {

		final QueryOptions queryOptions = new QueryOptions();

		AbstractClusterConfiguration clusterConfiguration = new AbstractClusterConfiguration() {
			@Override
			protected QueryOptions getQueryOptions() {
				return queryOptions;
			}
		};

		Cluster cluster = getCluster(clusterConfiguration);
		assertThat(getConfiguration(cluster).getQueryOptions(), is(queryOptions));
	}

	/**
	 * @see DATACASS-226
	 * @throws Exception
	 */
	@Test
	public void shouldSetAuthProvider() throws Exception {

		final AuthProvider authProvider = new PlainTextAuthProvider("x", "y");

		AbstractClusterConfiguration clusterConfiguration = new AbstractClusterConfiguration() {
			@Override
			protected AuthProvider getAuthProvider() {
				return authProvider;
			}
		};

		Cluster cluster = getCluster(clusterConfiguration);
		assertThat(getConfiguration(cluster).getProtocolOptions().getAuthProvider(), is(authProvider));
	}

	/**
	 * @see DATACASS-226
	 * @throws Exception
	 */
	@Test
	public void shouldSetLoadBalancingPolicy() throws Exception {

		final LoadBalancingPolicy loadBalancingPolicy = new RoundRobinPolicy();

		AbstractClusterConfiguration clusterConfiguration = new AbstractClusterConfiguration() {
			@Override
			protected LoadBalancingPolicy getLoadBalancingPolicy() {
				return loadBalancingPolicy;
			}
		};

		Cluster cluster = getCluster(clusterConfiguration);
		assertThat(getConfiguration(cluster).getPolicies().getLoadBalancingPolicy(), is(loadBalancingPolicy));
	}

	/**
	 * @see DATACASS-226
	 * @throws Exception
	 */
	@Test
	public void shouldSetReconnectionPolicy() throws Exception {

		final ReconnectionPolicy reconnectionPolicy = new ExponentialReconnectionPolicy(1, 2);

		AbstractClusterConfiguration clusterConfiguration = new AbstractClusterConfiguration() {
			@Override
			protected ReconnectionPolicy getReconnectionPolicy() {
				return reconnectionPolicy;
			}
		};

		Cluster cluster = getCluster(clusterConfiguration);
		assertThat(getConfiguration(cluster).getPolicies().getReconnectionPolicy(), is(reconnectionPolicy));
	}

	/**
	 * @see DATACASS-226
	 * @throws Exception
	 */
	@Test
	public void shouldSetProtocolVersion() throws Exception {

		AbstractClusterConfiguration clusterConfiguration = new AbstractClusterConfiguration() {
			@Override
			protected ProtocolVersion getProtocolVersion() {
				return ProtocolVersion.V2;
			}
		};

		Cluster cluster = getCluster(clusterConfiguration);
		assertThat(ReflectionTestUtils.getField(getConfiguration(cluster).getProtocolOptions(), "initialProtocolVersion"),
				is((Object) ProtocolVersion.V2));
	}

	/**
	 * @see DATACASS-226
	 * @throws Exception
	 */
	@Test
	public void shouldDisableMetrics() throws Exception {

		AbstractClusterConfiguration clusterConfiguration = new AbstractClusterConfiguration() {
			@Override
			protected boolean getMetricsEnabled() {
				return false;
			}
		};

		Cluster cluster = getCluster(clusterConfiguration);
		assertThat(getConfiguration(cluster).getMetricsOptions(), is(nullValue()));
	}

	/**
	 * @see DATACASS-226
	 * @throws Exception
	 */
	@Test
	public void shouldSetKeyspaceCreations() throws Exception {

		final List<CreateKeyspaceSpecification> specification = Collections
				.singletonList(CreateKeyspaceSpecification.createKeyspace());
		AbstractClusterConfiguration clusterConfiguration = new AbstractClusterConfiguration() {
			@Override
			protected List<CreateKeyspaceSpecification> getKeyspaceCreations() {
				return specification;
			}
		};

		assertThat(clusterConfiguration.cluster().getKeyspaceCreations(), is(equalTo(specification)));
	}

	/**
	 * @see DATACASS-226
	 * @throws Exception
	 */
	@Test
	public void shouldSetKeyspaceDrops() throws Exception {

		final List<DropKeyspaceSpecification> specification = Collections
				.singletonList(DropKeyspaceSpecification.dropKeyspace());
		AbstractClusterConfiguration clusterConfiguration = new AbstractClusterConfiguration() {
			@Override
			protected List<DropKeyspaceSpecification> getKeyspaceDrops() {
				return specification;
			}
		};

		assertThat(clusterConfiguration.cluster().getKeyspaceDrops(), is(equalTo(specification)));
	}

	/**
	 * @see DATACASS-226
	 * @throws Exception
	 */
	@Test
	public void shouldSetStartupScripts() throws Exception {

		final List<String> scripts = Collections.singletonList("USE BLUE_METH; CREATE TABLE...");
		AbstractClusterConfiguration clusterConfiguration = new AbstractClusterConfiguration() {
			@Override
			protected List<String> getStartupScripts() {
				return scripts;
			}
		};

		assertThat(clusterConfiguration.cluster().getStartupScripts(), is(equalTo(scripts)));
	}

	/**
	 * @see DATACASS-226
	 * @throws Exception
	 */
	@Test
	public void shouldSetShutdownScripts() throws Exception {

		final List<String> scripts = Collections.singletonList("USE BLUE_METH; DROP TABLE...");
		AbstractClusterConfiguration clusterConfiguration = new AbstractClusterConfiguration() {
			@Override
			protected List<String> getShutdownScripts() {
				return scripts;
			}
		};

		assertThat(clusterConfiguration.cluster().getShutdownScripts(), is(equalTo(scripts)));
	}

	private Configuration getConfiguration(Cluster cluster) throws Exception {
		return cluster.getConfiguration();
	}

	private Cluster getCluster(AbstractClusterConfiguration clusterConfiguration) throws Exception {

		CassandraCqlClusterFactoryBean cluster = clusterConfiguration.cluster();
		cluster.afterPropertiesSet();
		return cluster.getObject();
	}
}
