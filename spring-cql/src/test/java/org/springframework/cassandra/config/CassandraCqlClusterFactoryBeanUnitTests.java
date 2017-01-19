/*
 * Copyright 2016-2017 the original author or authors.
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
package org.springframework.cassandra.config;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.Test;
import org.mockito.Matchers;
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
 * Unit tests for {@link CassandraCqlClusterFactoryBean}.
 *
 * @author Mark Paluch
 * @author John Blum
 */
public class CassandraCqlClusterFactoryBeanUnitTests {

	@Test // DATACASS-226
	public void shouldInitializeWithoutAnyOptions() throws Exception {

		CassandraCqlClusterFactoryBean bean = new CassandraCqlClusterFactoryBean();
		bean.afterPropertiesSet();

		assertThat(bean.getObject()).isNotNull();
		assertThat(bean.getObject().isClosed()).isFalse();
		assertThat(getConfiguration(bean).getMetricsOptions()).isNotNull();
		assertThat(getConfiguration(bean).getMetricsOptions().isJMXReportingEnabled()).isTrue();
	}

	@Test // DATACASS-226
	public void shouldShutdownClusterInstance() throws Exception {

		CassandraCqlClusterFactoryBean bean = new CassandraCqlClusterFactoryBean();
		bean.afterPropertiesSet();
		bean.destroy();

		assertThat(bean.getObject().isClosed()).isTrue();
	}

	@Test // DATACASS-217
	public void shouldSetLZ4CompressionType() throws Exception {

		CompressionType compressionType = CompressionType.LZ4;

		CassandraCqlClusterFactoryBean bean = new CassandraCqlClusterFactoryBean();
		bean.setCompressionType(compressionType);
		bean.afterPropertiesSet();

		assertThat(getProtocolOptions(bean).getCompression()).isEqualTo(Compression.LZ4);
	}

	@Test // DATACASS-226
	public void shouldSetSnappyCompressionType() throws Exception {

		CompressionType compressionType = CompressionType.SNAPPY;

		CassandraCqlClusterFactoryBean bean = new CassandraCqlClusterFactoryBean();
		bean.setCompressionType(compressionType);
		bean.afterPropertiesSet();

		assertThat(getConfiguration(bean).getProtocolOptions().getCompression()).isEqualTo(Compression.SNAPPY);
	}

	@Test // DATACASS-226
	public void shouldSetPoolingOptions() throws Exception {

		PoolingOptions poolingOptions = new PoolingOptions();

		CassandraCqlClusterFactoryBean bean = new CassandraCqlClusterFactoryBean();
		bean.setPoolingOptions(poolingOptions);
		bean.afterPropertiesSet();

		assertThat(getConfiguration(bean).getPoolingOptions()).isEqualTo(poolingOptions);
	}

	@Test // DATACASS-226
	public void shouldSetSocketOptions() throws Exception {

		SocketOptions socketOptions = new SocketOptions();

		CassandraCqlClusterFactoryBean bean = new CassandraCqlClusterFactoryBean();
		bean.setSocketOptions(socketOptions);
		bean.afterPropertiesSet();

		assertThat(getConfiguration(bean).getSocketOptions()).isEqualTo(socketOptions);
	}

	@Test // DATACASS-226
	public void shouldSetQueryOptions() throws Exception {

		QueryOptions queryOptions = new QueryOptions();

		CassandraCqlClusterFactoryBean bean = new CassandraCqlClusterFactoryBean();
		bean.setQueryOptions(queryOptions);
		bean.afterPropertiesSet();

		assertThat(getConfiguration(bean).getQueryOptions()).isEqualTo(queryOptions);
	}

	@Test // DATACASS-226
	public void defaultQueryOptionsShouldHaveOwnObjectIdentity() throws Exception {

		QueryOptions queryOptions = new QueryOptions();

		CassandraCqlClusterFactoryBean bean = new CassandraCqlClusterFactoryBean();
		bean.afterPropertiesSet();

		assertThat(getConfiguration(bean).getQueryOptions()).isNotNull();
		assertThat(getConfiguration(bean).getQueryOptions()).isNotEqualTo(queryOptions);
	}

	@Test // DATACASS-226
	public void shouldSetAuthProvider() throws Exception {

		AuthProvider authProvider = new PlainTextAuthProvider("x", "y");

		CassandraCqlClusterFactoryBean bean = new CassandraCqlClusterFactoryBean();
		bean.setAuthProvider(authProvider);
		bean.afterPropertiesSet();

		assertThat(getConfiguration(bean).getProtocolOptions().getAuthProvider()).isEqualTo(authProvider);
	}

	@Test // DATACASS-226, DATACASS-263
	public void shouldSetAuthenticationProvider() throws Exception {

		AuthProvider authProvider = new PlainTextAuthProvider("x", "y");

		CassandraCqlClusterFactoryBean bean = new CassandraCqlClusterFactoryBean();
		bean.setAuthProvider(authProvider);
		bean.afterPropertiesSet();

		AuthProvider result = getConfiguration(bean).getProtocolOptions().getAuthProvider();
		assertThat(result).isEqualTo(authProvider);
	}

	@Test // DATACASS-226, DATACASS-263
	public void shouldSetAuthentication() throws Exception {

		CassandraCqlClusterFactoryBean bean = new CassandraCqlClusterFactoryBean();
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

		CassandraCqlClusterFactoryBean bean = new CassandraCqlClusterFactoryBean();
		bean.setLoadBalancingPolicy(loadBalancingPolicy);
		bean.afterPropertiesSet();

		assertThat(getConfiguration(bean).getPolicies().getLoadBalancingPolicy()).isEqualTo(loadBalancingPolicy);
	}

	@Test // DATACASS-226
	public void shouldSetReconnectionPolicy() throws Exception {

		ReconnectionPolicy reconnectionPolicy = new ExponentialReconnectionPolicy(1, 2);

		CassandraCqlClusterFactoryBean bean = new CassandraCqlClusterFactoryBean();
		bean.setReconnectionPolicy(reconnectionPolicy);
		bean.afterPropertiesSet();

		assertThat(getConfiguration(bean).getPolicies().getReconnectionPolicy()).isEqualTo(reconnectionPolicy);
	}

	@Test // DATACASS-226
	public void shouldSetProtocolVersion() throws Exception {

		CassandraCqlClusterFactoryBean bean = new CassandraCqlClusterFactoryBean();
		bean.setProtocolVersion(ProtocolVersion.V2);
		bean.afterPropertiesSet();

		assertThat(getConfiguration(bean).getProtocolOptions()).extracting("initialProtocolVersion")
				.contains(ProtocolVersion.V2);
	}

	@Test // DATACASS-226
	public void shouldSetSslOptions() throws Exception {

		SSLOptions sslOptions = JdkSSLOptions.builder().build();

		CassandraCqlClusterFactoryBean bean = new CassandraCqlClusterFactoryBean();
		bean.setSslEnabled(true);
		bean.setSslOptions(sslOptions);
		bean.afterPropertiesSet();

		assertThat(getConfiguration(bean).getProtocolOptions().getSSLOptions()).isEqualTo(sslOptions);
	}

	@Test // DATACASS-226
	public void shouldDisableMetrics() throws Exception {

		CassandraCqlClusterFactoryBean bean = new CassandraCqlClusterFactoryBean();
		bean.setMetricsEnabled(false);
		bean.afterPropertiesSet();

		assertThat(getConfiguration(bean).getMetricsOptions().isEnabled()).isFalse();
	}

	@Test // DATACASS-226
	public void shouldDisableJmxReporting() throws Exception {

		CassandraCqlClusterFactoryBean bean = new CassandraCqlClusterFactoryBean();
		bean.setJmxReportingEnabled(false);
		bean.afterPropertiesSet();

		assertThat(getConfiguration(bean).getMetricsOptions().isJMXReportingEnabled()).isFalse();
	}

	@Test
	public void shouldCallClusterBuilderConfigurer() throws Exception {

		ClusterBuilderConfigurer mockClusterBuilderConfigurer = mock(ClusterBuilderConfigurer.class);
		CassandraCqlClusterFactoryBean bean = new CassandraCqlClusterFactoryBean();

		bean.setClusterBuilderConfigurer(mockClusterBuilderConfigurer);
		bean.afterPropertiesSet();

		verify(mockClusterBuilderConfigurer, times(1)).configure(isA(Cluster.Builder.class));
	}

	@Test // DATACASS-316
	public void shouldSetAddressTranslator() throws Exception {

		AddressTranslator mockAddressTranslator = mock(AddressTranslator.class);
		CassandraCqlClusterFactoryBean bean = new CassandraCqlClusterFactoryBean();

		bean.setAddressTranslator(mockAddressTranslator);
		bean.afterPropertiesSet();

		assertThat(getPolicies(bean).getAddressTranslator()).isEqualTo(mockAddressTranslator);
	}

	@Test // DATACASS-317
	public void shouldSetClusterNameWithBeanNameProperty() throws Exception {

		final Cluster.Builder mockClusterBuilder = mock(Cluster.Builder.class);

		when(mockClusterBuilder.addContactPoints(Matchers.<String[]> anyVararg())).thenReturn(mockClusterBuilder);

		CassandraCqlClusterFactoryBean bean = new CassandraCqlClusterFactoryBean() {
			@Override
			Cluster.Builder newClusterBuilder() {
				return mockClusterBuilder;
			}
		};

		bean.setBeanName("ABC");
		bean.setClusterName(" ");
		bean.afterPropertiesSet();

		verify(mockClusterBuilder, times(1)).withClusterName(eq("ABC"));
	}

	@Test // DATACASS-317
	public void shouldSetClusterNameWithClusterNameProperty() throws Exception {

		final Cluster.Builder mockClusterBuilder = mock(Cluster.Builder.class);

		when(mockClusterBuilder.addContactPoints(Matchers.<String[]> anyVararg())).thenReturn(mockClusterBuilder);

		CassandraCqlClusterFactoryBean bean = new CassandraCqlClusterFactoryBean() {
			@Override
			Cluster.Builder newClusterBuilder() {
				return mockClusterBuilder;
			}
		};

		bean.setBeanName("ABC");
		bean.setClusterName("XYZ");
		bean.afterPropertiesSet();

		verify(mockClusterBuilder, times(1)).withClusterName(eq("XYZ"));
	}

	@Test // DATACASS-319
	public void shouldSetMaxSchemaAgreementWaitSeconds() throws Exception {

		CassandraCqlClusterFactoryBean bean = new CassandraCqlClusterFactoryBean();

		bean.setMaxSchemaAgreementWaitSeconds(20);
		bean.afterPropertiesSet();

		assertThat(getProtocolOptions(bean).getMaxSchemaAgreementWaitSeconds()).isEqualTo(20);
	}

	@Test // DATACASS-320
	public void shouldSetSpeculativeExecutionPolicy() throws Exception {

		SpeculativeExecutionPolicy mockSpeculativeExecutionPolicy = mock(SpeculativeExecutionPolicy.class);
		CassandraCqlClusterFactoryBean bean = new CassandraCqlClusterFactoryBean();

		bean.setSpeculativeExecutionPolicy(mockSpeculativeExecutionPolicy);
		bean.afterPropertiesSet();

		assertThat(getPolicies(bean).getSpeculativeExecutionPolicy()).isEqualTo(mockSpeculativeExecutionPolicy);
	}

	@Test // DATACASS-238
	public void shouldSetTimestampGenerator() throws Exception {

		TimestampGenerator mockTimestampGenerator = mock(TimestampGenerator.class);
		CassandraCqlClusterFactoryBean bean = new CassandraCqlClusterFactoryBean();

		bean.setTimestampGenerator(mockTimestampGenerator);
		bean.afterPropertiesSet();

		assertThat(getPolicies(bean).getTimestampGenerator()).isEqualTo(mockTimestampGenerator);
	}

	private Policies getPolicies(CassandraCqlClusterFactoryBean bean) throws Exception {
		return getConfiguration(bean).getPolicies();
	}

	private ProtocolOptions getProtocolOptions(CassandraCqlClusterFactoryBean bean) throws Exception {
		return getConfiguration(bean).getProtocolOptions();
	}

	private Configuration getConfiguration(CassandraCqlClusterFactoryBean bean) throws Exception {
		return bean.getObject().getConfiguration();
	}
}
