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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.anyString;

import org.junit.Test;
import org.springframework.data.cassandra.support.IntegrationTestNettyOptions;
import org.springframework.test.util.ReflectionTestUtils;

import com.datastax.driver.core.*;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.policies.AddressTranslator;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.Policies;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.SpeculativeExecutionPolicy;

/**
 * Unit tests for {@link CassandraClusterFactoryBean}.
 *
 * @author Mark Paluch
 * @author John Blum
 */
public class CassandraClusterFactoryBeanUnitTests {

	@Test // DATACASS-226
	public void shouldInitializeWithoutAnyOptions() throws Exception {

		CassandraClusterFactoryBean bean = new CassandraClusterFactoryBean();
		bean.afterPropertiesSet();

		assertThat(bean.getObject()).isNotNull();
		assertThat(bean.getObject().isClosed()).isFalse();
		assertThat(getConfiguration(bean).getMetricsOptions()).isNotNull();
		assertThat(getConfiguration(bean).getMetricsOptions().isJMXReportingEnabled()).isTrue();
	}

	@Test // DATACASS-226
	public void shouldShutdownClusterInstance() throws Exception {

		CassandraClusterFactoryBean bean = new CassandraClusterFactoryBean();
		bean.afterPropertiesSet();
		bean.destroy();

		assertThat(bean.getObject().isClosed()).isTrue();
	}

	@Test // DATACASS-217
	public void shouldSetLZ4CompressionType() throws Exception {

		CompressionType compressionType = CompressionType.LZ4;

		CassandraClusterFactoryBean bean = new CassandraClusterFactoryBean();
		bean.setCompressionType(compressionType);
		bean.afterPropertiesSet();

		assertThat(getProtocolOptions(bean).getCompression()).isEqualTo(Compression.LZ4);
	}

	@Test // DATACASS-226
	public void shouldSetSnappyCompressionType() throws Exception {

		CompressionType compressionType = CompressionType.SNAPPY;

		CassandraClusterFactoryBean bean = new CassandraClusterFactoryBean();
		bean.setCompressionType(compressionType);
		bean.afterPropertiesSet();

		assertThat(getConfiguration(bean).getProtocolOptions().getCompression()).isEqualTo(Compression.SNAPPY);
	}

	@Test // DATACASS-226
	public void shouldSetPoolingOptions() throws Exception {

		PoolingOptions poolingOptions = new PoolingOptions();

		CassandraClusterFactoryBean bean = new CassandraClusterFactoryBean();
		bean.setPoolingOptions(poolingOptions);
		bean.afterPropertiesSet();

		assertThat(getConfiguration(bean).getPoolingOptions()).isEqualTo(poolingOptions);
	}

	@Test // DATACASS-226
	public void shouldSetSocketOptions() throws Exception {

		SocketOptions socketOptions = new SocketOptions();

		CassandraClusterFactoryBean bean = new CassandraClusterFactoryBean();
		bean.setSocketOptions(socketOptions);
		bean.afterPropertiesSet();

		assertThat(getConfiguration(bean).getSocketOptions()).isEqualTo(socketOptions);
	}

	@Test // DATACASS-226
	public void shouldSetQueryOptions() throws Exception {

		QueryOptions queryOptions = new QueryOptions();

		CassandraClusterFactoryBean bean = new CassandraClusterFactoryBean();
		bean.setQueryOptions(queryOptions);
		bean.afterPropertiesSet();

		assertThat(getConfiguration(bean).getQueryOptions()).isEqualTo(queryOptions);
	}

	@Test // DATACASS-226, DATACASS-548
	public void shouldConfigureDefaultQueryOptions() throws Exception {

		CassandraClusterFactoryBean bean = new CassandraClusterFactoryBean();
		bean.afterPropertiesSet();

		assertThat(getConfiguration(bean).getQueryOptions()).isNotNull();
		assertThat(getConfiguration(bean).getQueryOptions()).isEqualTo(new QueryOptions());
	}

	@Test // DATACASS-226
	public void shouldSetAuthProvider() throws Exception {

		AuthProvider authProvider = new PlainTextAuthProvider("x", "y");

		CassandraClusterFactoryBean bean = new CassandraClusterFactoryBean();
		bean.setAuthProvider(authProvider);
		bean.afterPropertiesSet();

		assertThat(getConfiguration(bean).getProtocolOptions().getAuthProvider()).isEqualTo(authProvider);
	}

	@Test // DATACASS-226, DATACASS-263
	public void shouldSetAuthenticationProvider() throws Exception {

		AuthProvider authProvider = new PlainTextAuthProvider("x", "y");

		CassandraClusterFactoryBean bean = new CassandraClusterFactoryBean();
		bean.setAuthProvider(authProvider);
		bean.afterPropertiesSet();

		AuthProvider result = getConfiguration(bean).getProtocolOptions().getAuthProvider();
		assertThat(result).isEqualTo(authProvider);
	}

	@Test // DATACASS-226, DATACASS-263
	public void shouldSetAuthentication() throws Exception {

		CassandraClusterFactoryBean bean = new CassandraClusterFactoryBean();
		bean.setUsername("user");
		bean.setPassword("password");
		bean.afterPropertiesSet();

		AuthProvider result = getConfiguration(bean).getProtocolOptions().getAuthProvider();
		assertThat(result).isNotNull();
		assertThat(ReflectionTestUtils.getField(result, "username")).isEqualTo((Object) "user");
		assertThat(ReflectionTestUtils.getField(result, "password")).isEqualTo((Object) "password");
	}

	@Test // DATACASS-226
	public void shouldSetLoadBalancingPolicy() throws Exception {

		LoadBalancingPolicy loadBalancingPolicy = new RoundRobinPolicy();

		CassandraClusterFactoryBean bean = new CassandraClusterFactoryBean();
		bean.setLoadBalancingPolicy(loadBalancingPolicy);
		bean.afterPropertiesSet();

		assertThat(getConfiguration(bean).getPolicies().getLoadBalancingPolicy()).isEqualTo(loadBalancingPolicy);
	}

	@Test // DATACASS-226
	public void shouldSetReconnectionPolicy() throws Exception {

		ReconnectionPolicy reconnectionPolicy = new ExponentialReconnectionPolicy(1, 2);

		CassandraClusterFactoryBean bean = new CassandraClusterFactoryBean();
		bean.setReconnectionPolicy(reconnectionPolicy);
		bean.afterPropertiesSet();

		assertThat(getConfiguration(bean).getPolicies().getReconnectionPolicy()).isEqualTo(reconnectionPolicy);
	}

	@Test // DATACASS-226
	public void shouldSetProtocolVersion() throws Exception {

		CassandraClusterFactoryBean bean = new CassandraClusterFactoryBean();
		bean.setProtocolVersion(ProtocolVersion.V2);
		bean.afterPropertiesSet();

		assertThat(getConfiguration(bean).getProtocolOptions()).extracting("initialProtocolVersion")
				.contains(ProtocolVersion.V2);
	}

	@Test // DATACASS-226
	public void shouldSetSslOptions() throws Exception {

		SSLOptions sslOptions = RemoteEndpointAwareJdkSSLOptions.builder().build();

		CassandraClusterFactoryBean bean = new CassandraClusterFactoryBean();
		bean.setSslEnabled(true);
		bean.setSslOptions(sslOptions);
		bean.afterPropertiesSet();

		assertThat(getConfiguration(bean).getProtocolOptions().getSSLOptions()).isEqualTo(sslOptions);
	}

	@Test // DATACASS-226
	public void shouldDisableMetrics() throws Exception {

		CassandraClusterFactoryBean bean = new CassandraClusterFactoryBean();
		bean.setMetricsEnabled(false);
		bean.afterPropertiesSet();

		assertThat(getConfiguration(bean).getMetricsOptions().isEnabled()).isFalse();
	}

	@Test // DATACASS-226
	public void shouldDisableJmxReporting() throws Exception {

		CassandraClusterFactoryBean bean = new CassandraClusterFactoryBean();
		bean.setJmxReportingEnabled(false);
		bean.afterPropertiesSet();

		assertThat(getConfiguration(bean).getMetricsOptions().isJMXReportingEnabled()).isFalse();
	}

	@Test
	public void shouldCallClusterBuilderConfigurer() throws Exception {

		ClusterBuilderConfigurer mockClusterBuilderConfigurer = mock(ClusterBuilderConfigurer.class);
		CassandraClusterFactoryBean bean = new CassandraClusterFactoryBean();

		bean.setClusterBuilderConfigurer(mockClusterBuilderConfigurer);
		bean.afterPropertiesSet();

		verify(mockClusterBuilderConfigurer, times(1)).configure(isA(Cluster.Builder.class));
	}

	@Test // DATACASS-316
	public void shouldSetAddressTranslator() throws Exception {

		AddressTranslator mockAddressTranslator = mock(AddressTranslator.class);
		CassandraClusterFactoryBean bean = new CassandraClusterFactoryBean();

		bean.setAddressTranslator(mockAddressTranslator);
		bean.afterPropertiesSet();

		assertThat(getPolicies(bean).getAddressTranslator()).isEqualTo(mockAddressTranslator);
	}

	@Test // DATACASS-317
	public void shouldSetClusterNameWithBeanNameProperty() throws Exception {

		Cluster.Builder mockClusterBuilder = mock(Cluster.Builder.class);
		Cluster mockCluster = mock(Cluster.class);

		when(mockClusterBuilder.addContactPoints(anyString())).thenReturn(mockClusterBuilder);
		when(mockClusterBuilder.build()).thenReturn(mockCluster);

		CassandraClusterFactoryBean bean = spy(new CassandraClusterFactoryBean());

		when(bean.newClusterBuilder()).thenReturn(mockClusterBuilder);

		bean.setBeanName("ABC");
		bean.setClusterName(" ");
		bean.afterPropertiesSet();

		verify(bean, times(1)).newClusterBuilder();
		verify(mockClusterBuilder, times(1)).withClusterName(eq("ABC"));
	}

	@Test // DATACASS-317
	public void shouldSetClusterNameWithClusterNameProperty() throws Exception {

		Cluster.Builder mockClusterBuilder = mock(Cluster.Builder.class);
		Cluster mockCluster = mock(Cluster.class);

		when(mockClusterBuilder.addContactPoints(anyString())).thenReturn(mockClusterBuilder);
		when(mockClusterBuilder.build()).thenReturn(mockCluster);

		CassandraClusterFactoryBean bean = spy(new CassandraClusterFactoryBean());

		when(bean.newClusterBuilder()).thenReturn(mockClusterBuilder);

		bean.setBeanName("ABC");
		bean.setClusterName("XYZ");
		bean.afterPropertiesSet();

		verify(bean,times(1)).newClusterBuilder();
		verify(mockClusterBuilder, times(1)).withClusterName(eq("XYZ"));
	}

	@Test // DATACASS-319
	public void shouldSetMaxSchemaAgreementWaitSeconds() throws Exception {

		CassandraClusterFactoryBean bean = new CassandraClusterFactoryBean();

		bean.setMaxSchemaAgreementWaitSeconds(20);
		bean.afterPropertiesSet();

		assertThat(getProtocolOptions(bean).getMaxSchemaAgreementWaitSeconds()).isEqualTo(20);
	}

	@Test // DATACASS-320
	public void shouldSetSpeculativeExecutionPolicy() throws Exception {

		SpeculativeExecutionPolicy mockSpeculativeExecutionPolicy = mock(SpeculativeExecutionPolicy.class);
		CassandraClusterFactoryBean bean = new CassandraClusterFactoryBean();

		bean.setSpeculativeExecutionPolicy(mockSpeculativeExecutionPolicy);
		bean.afterPropertiesSet();

		assertThat(getPolicies(bean).getSpeculativeExecutionPolicy()).isEqualTo(mockSpeculativeExecutionPolicy);
	}

	@Test // DATACASS-238
	public void shouldSetTimestampGenerator() throws Exception {

		TimestampGenerator mockTimestampGenerator = mock(TimestampGenerator.class);
		CassandraClusterFactoryBean bean = new CassandraClusterFactoryBean();

		bean.setTimestampGenerator(mockTimestampGenerator);
		bean.afterPropertiesSet();

		assertThat(getPolicies(bean).getTimestampGenerator()).isEqualTo(mockTimestampGenerator);
	}

	@Test
	public void configuredProtocolVersionShouldBeSet() throws Exception {

		CassandraClusterFactoryBean bean = new CassandraClusterFactoryBean();

		bean.setNettyOptions(IntegrationTestNettyOptions.INSTANCE);
		bean.setProtocolVersion(ProtocolVersion.V2);
		bean.afterPropertiesSet();

		assertThat(getProtocolVersionEnum(bean)).isEqualTo(ProtocolVersion.V2);
	}

	@Test
	public void defaultProtocolVersionShouldBeSet() throws Exception {

		CassandraClusterFactoryBean bean = new CassandraClusterFactoryBean();

		bean.afterPropertiesSet();

		assertThat(getProtocolVersionEnum(bean)).isNull();
	}

	private ProtocolVersion getProtocolVersionEnum(CassandraClusterFactoryBean cassandraClusterFactoryBean)
			throws Exception {

		// initialize connection factory
		return (ProtocolVersion) ReflectionTestUtils.getField(
				cassandraClusterFactoryBean.getObject().getConfiguration().getProtocolOptions(), "initialProtocolVersion");
	}

	private Policies getPolicies(CassandraClusterFactoryBean bean) throws Exception {
		return getConfiguration(bean).getPolicies();
	}

	private ProtocolOptions getProtocolOptions(CassandraClusterFactoryBean bean) throws Exception {
		return getConfiguration(bean).getProtocolOptions();
	}

	private Configuration getConfiguration(CassandraClusterFactoryBean bean) throws Exception {
		return bean.getObject().getConfiguration();
	}
}
