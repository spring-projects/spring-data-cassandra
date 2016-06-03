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

import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;

/**
 * Unit tests for {@link CassandraCqlClusterFactoryBean}.
 *
 * @see DATACASS-226
 * @see DATACASS-263
 * @author Mark Paluch
 */
public class CassandraCqlClusterFactoryBeanUnitTests {

	/**
	 * @see DATACASS-226
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
	 * @see DATACASS-226
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
	 * @see DATACASS-226
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
	 * @see DATACASS-226
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
	 * @see DATACASS-226
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
	 * @see DATACASS-226
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
	 * @see DATACASS-226
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
	 * @see DATACASS-226
	 * @see DATACASS-263
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
	 * @see DATACASS-226
	 * @see DATACASS-263
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
	 * @see DATACASS-226
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
	 * @see DATACASS-226
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
	 * @see DATACASS-226
	 * @throws Exception
	 */
	@Test
	public void shouldSetSslOptions() throws Exception {

		SSLOptions sslOptions = new SSLOptions();

		CassandraCqlClusterFactoryBean bean = new CassandraCqlClusterFactoryBean();
		bean.setSslEnabled(true);
		bean.setSslOptions(sslOptions);
		bean.afterPropertiesSet();

		assertThat(getConfiguration(bean).getProtocolOptions().getSSLOptions(), is(sslOptions));
	}

	/**
	 * @see DATACASS-226
	 * @throws Exception
	 */
	@Test
	public void shouldDisableMetrics() throws Exception {

		CassandraCqlClusterFactoryBean bean = new CassandraCqlClusterFactoryBean();
		bean.setMetricsEnabled(false);
		bean.afterPropertiesSet();

		assertThat(getConfiguration(bean).getMetricsOptions(), is(nullValue()));
	}

	/**
	 * @see DATACASS-226
	 * @throws Exception
	 */
	@Test
	public void shouldDisableJmxReporting() throws Exception {

		CassandraCqlClusterFactoryBean bean = new CassandraCqlClusterFactoryBean();
		bean.setJmxReportingEnabled(false);
		bean.afterPropertiesSet();

		assertThat(getConfiguration(bean).getMetricsOptions().isJMXReportingEnabled(), is(false));
	}

	private Configuration getConfiguration(CassandraCqlClusterFactoryBean bean) throws Exception {
		return bean.getObject().getConfiguration();
	}
}
