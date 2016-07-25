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

package org.springframework.cassandra.config;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.isA;

import org.junit.Test;
import org.mockito.Matchers;
import org.springframework.test.util.ReflectionTestUtils;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.JdkSSLOptions;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.SSLOptions;
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
 * Unit tests for {@link CassandraCqlClusterFactoryBean}.
 *
 * @author Mark Paluch
 * @author John Blum
 * @see <a href="https://jira.spring.io/browse/DATACASS-226">DATACASS-226</a>
 */
public class CassandraCqlClusterFactoryBeanUnitTests {

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-226">DATACASS-226</a>
	 * @throws Exception
	 */
	@Test
	public void shouldInitializeWithoutAnyOptions() throws Exception {

		CassandraCqlClusterFactoryBean bean = new CassandraCqlClusterFactoryBean();
		bean.afterPropertiesSet();

		assertThat(bean.getObject(), is(not(nullValue())));
		assertThat(bean.getObject().isClosed(), is(false));
		assertThat(getConfiguration(bean).getMetricsOptions(), is(not(nullValue())));
		assertThat(getConfiguration(bean).getMetricsOptions().isJMXReportingEnabled(), is(true));
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-226">DATACASS-226</a>
	 * @throws Exception
	 */
	@Test
	public void shouldShutdownClusterInstance() throws Exception {

		CassandraCqlClusterFactoryBean bean = new CassandraCqlClusterFactoryBean();
		bean.afterPropertiesSet();
		bean.destroy();

		assertThat(bean.getObject().isClosed(), is(true));
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-226">DATACASS-226</a>
	 * @throws Exception
	 */
	@Test
	public void shouldSetCompressionType() throws Exception {

		CompressionType compressionType = CompressionType.SNAPPY;

		CassandraCqlClusterFactoryBean bean = new CassandraCqlClusterFactoryBean();
		bean.setCompressionType(compressionType);
		bean.afterPropertiesSet();

		assertThat(getConfiguration(bean).getProtocolOptions().getCompression(), is(Compression.SNAPPY));
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-226">DATACASS-226</a>
	 * @throws Exception
	 */
	@Test
	public void shouldSetPoolingOptions() throws Exception {

		PoolingOptions poolingOptions = new PoolingOptions();

		CassandraCqlClusterFactoryBean bean = new CassandraCqlClusterFactoryBean();
		bean.setPoolingOptions(poolingOptions);
		bean.afterPropertiesSet();

		assertThat(getConfiguration(bean).getPoolingOptions(), is(poolingOptions));
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-226">DATACASS-226</a>
	 * @throws Exception
	 */
	@Test
	public void shouldSetSocketOptions() throws Exception {

		SocketOptions socketOptions = new SocketOptions();

		CassandraCqlClusterFactoryBean bean = new CassandraCqlClusterFactoryBean();
		bean.setSocketOptions(socketOptions);
		bean.afterPropertiesSet();

		assertThat(getConfiguration(bean).getSocketOptions(), is(socketOptions));
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-226">DATACASS-226</a>
	 * @throws Exception
	 */
	@Test
	public void shouldSetQueryOptions() throws Exception {

		QueryOptions queryOptions = new QueryOptions();

		CassandraCqlClusterFactoryBean bean = new CassandraCqlClusterFactoryBean();
		bean.setQueryOptions(queryOptions);
		bean.afterPropertiesSet();

		assertThat(getConfiguration(bean).getQueryOptions(), is(queryOptions));
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-226">DATACASS-226</a>
	 * @throws Exception
	 */
	@Test
	public void defaultQueryOptionsShouldHaveOwnObjectIdentity() throws Exception {

		QueryOptions queryOptions = new QueryOptions();

		CassandraCqlClusterFactoryBean bean = new CassandraCqlClusterFactoryBean();
		bean.afterPropertiesSet();

		assertThat(getConfiguration(bean).getQueryOptions(), is(not(nullValue())));
		assertThat(getConfiguration(bean).getQueryOptions(), is(not(queryOptions)));
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-226">DATACASS-226</a>
	 * @throws Exception
	 */
	@Test
	public void shouldSetAuthProvider() throws Exception {

		AuthProvider authProvider = new PlainTextAuthProvider("x", "y");

		CassandraCqlClusterFactoryBean bean = new CassandraCqlClusterFactoryBean();
		bean.setAuthProvider(authProvider);
		bean.afterPropertiesSet();

		assertThat(getConfiguration(bean).getProtocolOptions().getAuthProvider(), is(authProvider));
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-226">DATACASS-226</a>
	 * @see <a href="https://jira.spring.io/browse/DATACASS-263">DATACASS-263</a>
	 * @throws Exception
	 */
	@Test
	public void shouldSetAuthenticationProvider() throws Exception {

		AuthProvider authProvider = new PlainTextAuthProvider("x", "y");

		CassandraCqlClusterFactoryBean bean = new CassandraCqlClusterFactoryBean();
		bean.setAuthProvider(authProvider);
		bean.afterPropertiesSet();

		AuthProvider result = getConfiguration(bean).getProtocolOptions().getAuthProvider();
		assertThat(result, is(equalTo(authProvider)));
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-226">DATACASS-226</a>
	 * @see <a href="https://jira.spring.io/browse/DATACASS-263">DATACASS-263</a>
	 * @throws Exception
	 */
	@Test
	public void shouldSetAuthentication() throws Exception {

		CassandraCqlClusterFactoryBean bean = new CassandraCqlClusterFactoryBean();
		bean.setUsername("user");
		bean.setPassword("password");
		bean.afterPropertiesSet();

		AuthProvider result = getConfiguration(bean).getProtocolOptions().getAuthProvider();
		assertThat(result, is(not(nullValue())));
		assertThat(ReflectionTestUtils.getField(result, "username"), is(equalTo((Object) "user")));
		assertThat(ReflectionTestUtils.getField(result, "password"), is(equalTo((Object) "password")));
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-226">DATACASS-226</a>
	 * @throws Exception
	 */
	@Test
	public void shouldSetLoadBalancingPolicy() throws Exception {

		LoadBalancingPolicy loadBalancingPolicy = new RoundRobinPolicy();

		CassandraCqlClusterFactoryBean bean = new CassandraCqlClusterFactoryBean();
		bean.setLoadBalancingPolicy(loadBalancingPolicy);
		bean.afterPropertiesSet();

		assertThat(getConfiguration(bean).getPolicies().getLoadBalancingPolicy(), is(loadBalancingPolicy));
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-226">DATACASS-226</a>
	 * @throws Exception
	 */
	@Test
	public void shouldSetReconnectionPolicy() throws Exception {

		ReconnectionPolicy reconnectionPolicy = new ExponentialReconnectionPolicy(1, 2);

		CassandraCqlClusterFactoryBean bean = new CassandraCqlClusterFactoryBean();
		bean.setReconnectionPolicy(reconnectionPolicy);
		bean.afterPropertiesSet();

		assertThat(getConfiguration(bean).getPolicies().getReconnectionPolicy(), is(reconnectionPolicy));
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-226">DATACASS-226</a>
	 * @throws Exception
	 */
	@Test
	public void shouldSetProtocolVersion() throws Exception {

		CassandraCqlClusterFactoryBean bean = new CassandraCqlClusterFactoryBean();
		bean.setProtocolVersion(ProtocolVersion.V2);
		bean.afterPropertiesSet();

		assertThat(ReflectionTestUtils.getField(getConfiguration(bean).getProtocolOptions(), "initialProtocolVersion"),
				is((Object) ProtocolVersion.V2));
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-226">DATACASS-226</a>
	 * @throws Exception
	 */
	@Test
	public void shouldSetSslOptions() throws Exception {

		SSLOptions sslOptions = JdkSSLOptions.builder().build();

		CassandraCqlClusterFactoryBean bean = new CassandraCqlClusterFactoryBean();
		bean.setSslEnabled(true);
		bean.setSslOptions(sslOptions);
		bean.afterPropertiesSet();

		assertThat(getConfiguration(bean).getProtocolOptions().getSSLOptions(), is(sslOptions));
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-226">DATACASS-226</a>
	 * @throws Exception
	 */
	@Test
	public void shouldDisableMetrics() throws Exception {

		CassandraCqlClusterFactoryBean bean = new CassandraCqlClusterFactoryBean();
		bean.setMetricsEnabled(false);
		bean.afterPropertiesSet();

		assertThat(getConfiguration(bean).getMetricsOptions().isEnabled(), is(false));
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-226">DATACASS-226</a>
	 * @throws Exception
	 */
	@Test
	public void shouldDisableJmxReporting() throws Exception {

		CassandraCqlClusterFactoryBean bean = new CassandraCqlClusterFactoryBean();
		bean.setJmxReportingEnabled(false);
		bean.afterPropertiesSet();

		assertThat(getConfiguration(bean).getMetricsOptions().isJMXReportingEnabled(), is(false));
	}

	@Test
	public void shouldCallClusterBuilderConfigurer() throws Exception {

		ClusterBuilderConfigurer mockClusterBuilderConfigurer = mock(ClusterBuilderConfigurer.class);
		CassandraCqlClusterFactoryBean bean = new CassandraCqlClusterFactoryBean();

		bean.setClusterBuilderConfigurer(mockClusterBuilderConfigurer);
		bean.afterPropertiesSet();

		verify(mockClusterBuilderConfigurer, times(1)).configure(isA(Cluster.Builder.class));
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-316">DATACASS-316</a>
	 */
	@Test
	public void shouldSetAddressTranslator() throws Exception {

		AddressTranslator mockAddressTranslator = mock(AddressTranslator.class);
		CassandraCqlClusterFactoryBean bean = new CassandraCqlClusterFactoryBean();

		bean.setAddressTranslator(mockAddressTranslator);
		bean.afterPropertiesSet();

		assertThat(getPolicies(bean).getAddressTranslator(), is(equalTo(mockAddressTranslator)));
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-317">DATACASS-317</a>
	 */
	@Test
	public void shouldSetClusterNameWithBeanNameProperty() throws Exception {

		final Cluster.Builder mockClusterBuilder = mock(Cluster.Builder.class);

		when(mockClusterBuilder.addContactPoints(Matchers.<String[]>anyVararg())).thenReturn(mockClusterBuilder);

		CassandraCqlClusterFactoryBean bean = new CassandraCqlClusterFactoryBean() {
			@Override Cluster.Builder newClusterBuilder() {
				return mockClusterBuilder;
			}
		};

		bean.setBeanName("ABC");
		bean.setClusterName(" ");
		bean.afterPropertiesSet();

		verify(mockClusterBuilder, times(1)).withClusterName(eq("ABC"));
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-317">DATACASS-317</a>
	 */
	@Test
	public void shouldSetClusterNameWithClusterNameProperty() throws Exception {

		final Cluster.Builder mockClusterBuilder = mock(Cluster.Builder.class);

		when(mockClusterBuilder.addContactPoints(Matchers.<String[]>anyVararg())).thenReturn(mockClusterBuilder);

		CassandraCqlClusterFactoryBean bean = new CassandraCqlClusterFactoryBean() {
			@Override Cluster.Builder newClusterBuilder() {
				return mockClusterBuilder;
			}
		};

		bean.setBeanName("ABC");
		bean.setClusterName("XYZ");
		bean.afterPropertiesSet();

		verify(mockClusterBuilder, times(1)).withClusterName(eq("XYZ"));
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-319">DATACASS-319</a>
	 */
	@Test
	public void shouldSetMaxSchemaAgreementWaitSeconds() throws Exception {

		CassandraCqlClusterFactoryBean bean = new CassandraCqlClusterFactoryBean();

		bean.setMaxSchemaAgreementWaitSeconds(20);
		bean.afterPropertiesSet();

		assertThat(getProtocolOptions(bean).getMaxSchemaAgreementWaitSeconds(), is(equalTo(20)));
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-320">DATACASS-320</a>
	 */
	@Test
	public void shouldSetSpeculativeExecutionPolicy() throws Exception {

		SpeculativeExecutionPolicy mockSpeculativeExecutionPolicy = mock(SpeculativeExecutionPolicy.class);
		CassandraCqlClusterFactoryBean bean = new CassandraCqlClusterFactoryBean();

		bean.setSpeculativeExecutionPolicy(mockSpeculativeExecutionPolicy);
		bean.afterPropertiesSet();

		assertThat(getPolicies(bean).getSpeculativeExecutionPolicy(), is(equalTo(mockSpeculativeExecutionPolicy)));
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-238">DATACASS-238</a>
	 */
	@Test
	public void shouldSetTimestampGenerator() throws Exception {

		TimestampGenerator mockTimestampGenerator = mock(TimestampGenerator.class);
		CassandraCqlClusterFactoryBean bean = new CassandraCqlClusterFactoryBean();

		bean.setTimestampGenerator(mockTimestampGenerator);
		bean.afterPropertiesSet();

		assertThat(getPolicies(bean).getTimestampGenerator(), is(equalTo(mockTimestampGenerator)));
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
