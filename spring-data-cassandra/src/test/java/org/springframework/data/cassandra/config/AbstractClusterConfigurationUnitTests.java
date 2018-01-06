/*
 * Copyright 2016-2018 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;

import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.springframework.data.cassandra.core.cql.keyspace.CreateKeyspaceSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.DropKeyspaceSpecification;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.TimestampGenerator;
import com.datastax.driver.core.policies.AddressTranslator;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.Policies;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.SpeculativeExecutionPolicy;

/**
 * Unit tests for {@link AbstractClusterConfiguration}.
 *
 * @author Mark Paluch
 * @author John Blum
 * @soundtrack Max Graham Feat Neev Kennedy - So Caught Up (Dns Project Remix)
 */
public class AbstractClusterConfigurationUnitTests {

	@Test // DATACASS-226
	public void shouldInitializeWithoutAnyOptions() throws Exception {

		CassandraClusterFactoryBean bean = new CassandraClusterFactoryBean();
		bean.afterPropertiesSet();

		AbstractClusterConfiguration clusterConfiguration = new AbstractClusterConfiguration() {};

		Cluster cluster = getCluster(clusterConfiguration);
		assertThat(cluster).isNotNull();
		assertThat(cluster.isClosed()).isFalse();
		assertThat(getConfiguration(cluster).getMetricsOptions()).isNotNull();
		assertThat(getConfiguration(cluster).getMetricsOptions().isJMXReportingEnabled()).isTrue();
	}

	@Test // DATACASS-226
	public void shouldSetCompressionType() throws Exception {

		final CompressionType compressionType = CompressionType.SNAPPY;

		AbstractClusterConfiguration clusterConfiguration = new AbstractClusterConfiguration() {
			@Override
			protected CompressionType getCompressionType() {
				return compressionType;
			}
		};

		Cluster cluster = getCluster(clusterConfiguration);
		assertThat(getConfiguration(cluster).getProtocolOptions().getCompression()).isEqualTo(Compression.SNAPPY);
	}

	@Test // DATACASS-226
	public void shouldSetPoolingOptions() throws Exception {

		final PoolingOptions poolingOptions = new PoolingOptions();

		AbstractClusterConfiguration clusterConfiguration = new AbstractClusterConfiguration() {
			@Override
			protected PoolingOptions getPoolingOptions() {
				return poolingOptions;
			}
		};

		Cluster cluster = getCluster(clusterConfiguration);
		assertThat(getConfiguration(cluster).getPoolingOptions()).isEqualTo(poolingOptions);
	}

	@Test // DATACASS-226
	public void shouldSetSocketOptions() throws Exception {

		final SocketOptions socketOptions = new SocketOptions();

		AbstractClusterConfiguration clusterConfiguration = new AbstractClusterConfiguration() {
			@Override
			protected SocketOptions getSocketOptions() {
				return socketOptions;
			}
		};

		Cluster cluster = getCluster(clusterConfiguration);
		assertThat(getConfiguration(cluster).getSocketOptions()).isEqualTo(socketOptions);
	}

	@Test // DATACASS-226
	public void shouldSetQueryOptions() throws Exception {

		final QueryOptions queryOptions = new QueryOptions();

		AbstractClusterConfiguration clusterConfiguration = new AbstractClusterConfiguration() {
			@Override
			protected QueryOptions getQueryOptions() {
				return queryOptions;
			}
		};

		Cluster cluster = getCluster(clusterConfiguration);
		assertThat(getConfiguration(cluster).getQueryOptions()).isEqualTo(queryOptions);
	}

	@Test // DATACASS-226
	public void shouldSetAuthProvider() throws Exception {

		final AuthProvider authProvider = new PlainTextAuthProvider("x", "y");

		AbstractClusterConfiguration clusterConfiguration = new AbstractClusterConfiguration() {
			@Override
			protected AuthProvider getAuthProvider() {
				return authProvider;
			}
		};

		Cluster cluster = getCluster(clusterConfiguration);
		assertThat(getConfiguration(cluster).getProtocolOptions().getAuthProvider()).isEqualTo(authProvider);
	}

	@Test // DATACASS-226
	public void shouldSetLoadBalancingPolicy() throws Exception {

		final LoadBalancingPolicy loadBalancingPolicy = new RoundRobinPolicy();

		AbstractClusterConfiguration clusterConfiguration = new AbstractClusterConfiguration() {
			@Override
			protected LoadBalancingPolicy getLoadBalancingPolicy() {
				return loadBalancingPolicy;
			}
		};

		Cluster cluster = getCluster(clusterConfiguration);
		assertThat(getConfiguration(cluster).getPolicies().getLoadBalancingPolicy()).isEqualTo(loadBalancingPolicy);
	}

	@Test // DATACASS-226
	public void shouldSetReconnectionPolicy() throws Exception {

		final ReconnectionPolicy reconnectionPolicy = new ExponentialReconnectionPolicy(1, 2);

		AbstractClusterConfiguration clusterConfiguration = new AbstractClusterConfiguration() {
			@Override
			protected ReconnectionPolicy getReconnectionPolicy() {
				return reconnectionPolicy;
			}
		};

		Cluster cluster = getCluster(clusterConfiguration);
		assertThat(getConfiguration(cluster).getPolicies().getReconnectionPolicy()).isEqualTo(reconnectionPolicy);
	}

	@Test // DATACASS-226
	public void shouldSetProtocolVersion() throws Exception {

		AbstractClusterConfiguration clusterConfiguration = new AbstractClusterConfiguration() {
			@Override
			protected ProtocolVersion getProtocolVersion() {
				return ProtocolVersion.V2;
			}
		};

		Cluster cluster = getCluster(clusterConfiguration);
		assertThat(getConfiguration(cluster).getProtocolOptions()).extracting("initialProtocolVersion")
				.contains(ProtocolVersion.V2);
	}

	@Test // DATACASS-226
	public void shouldDisableMetrics() throws Exception {

		AbstractClusterConfiguration clusterConfiguration = new AbstractClusterConfiguration() {
			@Override
			protected boolean getMetricsEnabled() {
				return false;
			}
		};

		Cluster cluster = getCluster(clusterConfiguration);
		assertThat(getConfiguration(cluster).getMetricsOptions().isEnabled()).isFalse();
	}

	@Test // DATACASS-226
	public void shouldSetKeyspaceCreations() {

		List<CreateKeyspaceSpecification> specification = Collections
				.singletonList(CreateKeyspaceSpecification.createKeyspace("foo"));
		AbstractClusterConfiguration clusterConfiguration = new AbstractClusterConfiguration() {
			@Override
			protected List<CreateKeyspaceSpecification> getKeyspaceCreations() {
				return specification;
			}
		};

		assertThat(clusterConfiguration.cluster().getKeyspaceCreations()).isEqualTo(specification);
	}

	@Test // DATACASS-226
	public void shouldSetKeyspaceDrops() {

		List<DropKeyspaceSpecification> specification = Collections
				.singletonList(DropKeyspaceSpecification.dropKeyspace("foo"));
		AbstractClusterConfiguration clusterConfiguration = new AbstractClusterConfiguration() {
			@Override
			protected List<DropKeyspaceSpecification> getKeyspaceDrops() {
				return specification;
			}
		};

		assertThat(clusterConfiguration.cluster().getKeyspaceDrops()).isEqualTo(specification);
	}

	@Test // DATACASS-226
	public void shouldSetStartupScripts() {

		List<String> scripts = Collections.singletonList("USE BLUE_METH; CREATE TABLE...");
		AbstractClusterConfiguration clusterConfiguration = new AbstractClusterConfiguration() {
			@Override
			protected List<String> getStartupScripts() {
				return scripts;
			}
		};

		assertThat(clusterConfiguration.cluster().getStartupScripts()).isEqualTo(scripts);
	}

	@Test // DATACASS-226
	public void shouldSetShutdownScripts() {

		List<String> scripts = Collections.singletonList("USE BLUE_METH; DROP TABLE...");
		AbstractClusterConfiguration clusterConfiguration = new AbstractClusterConfiguration() {
			@Override
			protected List<String> getShutdownScripts() {
				return scripts;
			}
		};

		assertThat(clusterConfiguration.cluster().getShutdownScripts()).isEqualTo(scripts);
	}

	/**
	 * <a href="https://jira.spring.io/browse/DATACASS-316">DATACASS-316</a>
	 */
	@Test
	public void shouldSetAddressTranslator() throws Exception {

		final AddressTranslator mockAddressTranslator = mock(AddressTranslator.class);

		AbstractClusterConfiguration clusterConfiguration = new AbstractClusterConfiguration() {
			@Override
			protected AddressTranslator getAddressTranslator() {
				return mockAddressTranslator;
			}
		};

		assertThat(getPolicies(getCluster(clusterConfiguration)).getAddressTranslator()).isEqualTo(mockAddressTranslator);
	}

	/**
	 * <a href="https://jira.spring.io/browse/DATACASS-325">DATACASS-325</a>
	 */
	@Test
	public void shouldSetAndApplyClusterBuilderConfigurer() throws Exception {

		final ClusterBuilderConfigurer mockClusterBuilderConfigurer = mock(ClusterBuilderConfigurer.class);

		AbstractClusterConfiguration clusterConfiguration = new AbstractClusterConfiguration() {
			@Override
			protected ClusterBuilderConfigurer getClusterBuilderConfigurer() {
				return mockClusterBuilderConfigurer;
			}
		};

		assertThat(getCluster(clusterConfiguration)).isNotNull();
		verify(mockClusterBuilderConfigurer, times(1)).configure(isA(Cluster.Builder.class));
	}

	/**
	 * <a href="https://jira.spring.io/browse/DATACASS-120">DATACASS-120</a>
	 * <a href="https://jira.spring.io/browse/DATACASS-317">DATACASS-317</a>
	 */
	@Test
	public void shouldSetClusterName() throws Exception {

		AbstractClusterConfiguration clusterConfiguration = new AbstractClusterConfiguration() {
			@Override
			protected String getClusterName() {
				return "testCluster";
			}
		};

		assertThat(getCluster(clusterConfiguration).getClusterName()).isEqualTo("testCluster");
	}

	/**
	 * <a href="https://jira.spring.io/browse/DATACASS-319">DATACASS-319</a>
	 */
	@Test
	public void shouldSetMaxSchemaAgreementWaitInSeconds() throws Exception {

		AbstractClusterConfiguration clusterConfiguration = new AbstractClusterConfiguration() {
			@Override
			protected int getMaxSchemaAgreementWaitSeconds() {
				return 30;
			}
		};

		assertThat(getProtocolOptions(getCluster(clusterConfiguration)).getMaxSchemaAgreementWaitSeconds()).isEqualTo(30);
	}

	/**
	 * <a href="https://jira.spring.io/browse/DATACASS-320">DATACASS-320</a>
	 */
	@Test
	public void shouldSetSpeculativeExecutionPolicy() throws Exception {

		final SpeculativeExecutionPolicy mockSpeculativeExecutionPolicy = mock(SpeculativeExecutionPolicy.class);

		AbstractClusterConfiguration clusterConfiguration = new AbstractClusterConfiguration() {
			@Override
			protected SpeculativeExecutionPolicy getSpeculativeExecutionPolicy() {
				return mockSpeculativeExecutionPolicy;
			}
		};

		assertThat(getPolicies(getCluster(clusterConfiguration)).getSpeculativeExecutionPolicy())
				.isEqualTo(mockSpeculativeExecutionPolicy);
	}

	/**
	 * <a href="https://jira.spring.io/browse/DATACASS-238">DATACASS-238</a>
	 */
	@Test
	public void shouldSetTimestampGenerator() throws Exception {

		final TimestampGenerator mockTimestampGenerator = mock(TimestampGenerator.class);

		AbstractClusterConfiguration clusterConfiguration = new AbstractClusterConfiguration() {
			@Override
			protected TimestampGenerator getTimestampGenerator() {
				return mockTimestampGenerator;
			}
		};

		assertThat(getPolicies(getCluster(clusterConfiguration)).getTimestampGenerator()).isEqualTo(mockTimestampGenerator);
	}

	private Policies getPolicies(Cluster cluster) {
		return getConfiguration(cluster).getPolicies();
	}

	private ProtocolOptions getProtocolOptions(Cluster cluster) {
		return getConfiguration(cluster).getProtocolOptions();
	}

	private Configuration getConfiguration(Cluster cluster) {
		return cluster.getConfiguration();
	}

	private Cluster getCluster(AbstractClusterConfiguration clusterConfiguration) throws Exception {
		CassandraClusterFactoryBean cluster = clusterConfiguration.cluster();
		cluster.afterPropertiesSet();
		return cluster.getObject();
	}
}
