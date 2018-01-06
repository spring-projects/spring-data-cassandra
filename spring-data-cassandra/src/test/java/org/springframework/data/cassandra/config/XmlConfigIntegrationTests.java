/*
 * Copyright 2017-2018 the original author or authors.
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

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.data.cassandra.support.KeyspaceTestUtils;
import org.springframework.data.cassandra.test.util.AbstractEmbeddedCassandraIntegrationTest;
import org.springframework.data.cassandra.test.util.KeyspaceRule;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.TimestampGenerator;
import com.datastax.driver.core.policies.AddressTranslator;
import com.datastax.driver.core.policies.SpeculativeExecutionPolicy;

/**
 * Test XML namespace configuration using the spring-cql XSD.
 *
 * @author Matthews T. Adams
 * @author Oliver Gierke
 * @author Mark Paluch
 * @author John Blum
 */
@SuppressWarnings("unused")
public class XmlConfigIntegrationTests extends AbstractEmbeddedCassandraIntegrationTest {

	public static final String KEYSPACE = "xmlconfigtest";

	@Rule public KeyspaceRule keyspaceRule = new KeyspaceRule(cassandraEnvironment, KEYSPACE);

	private ConfigurableApplicationContext applicationContext;

	private AddressTranslator addressTranslator;
	private Cluster cluster;
	private ClusterBuilderConfigurer clusterBuilderConfigurer;
	private Executor executor;
	private Session session;
	private SpeculativeExecutionPolicy speculativeExecutionPolicy;
	private TimestampGenerator timestampGenerator;

	@Before
	public void setUp() {
		this.applicationContext = new ClassPathXmlApplicationContext("XmlConfigIntegrationTests-context.xml", getClass());

		this.addressTranslator = applicationContext.getBean(AddressTranslator.class);
		this.cluster = applicationContext.getBean(Cluster.class);
		this.clusterBuilderConfigurer = applicationContext.getBean(ClusterBuilderConfigurer.class);
		this.executor = applicationContext.getBean(Executor.class);
		this.session = applicationContext.getBean(Session.class);
		this.speculativeExecutionPolicy = applicationContext.getBean(SpeculativeExecutionPolicy.class);
		this.timestampGenerator = applicationContext.getBean(TimestampGenerator.class);
	}

	@After
	public void tearDown() {
		if (this.applicationContext != null) {
			this.applicationContext.close();
		}
	}

	@Test
	public void keyspaceExists() {
		KeyspaceTestUtils.assertKeyspaceExists(KEYSPACE, session);
	}

	@Test
	public void clusterConfigurationIsCorrect() {

		assertThat(cluster.getConfiguration().getPolicies().getAddressTranslator()).isEqualTo(addressTranslator);
		assertThat(cluster.getClusterName()).isEqualTo("skynet");
		assertThat(cluster.getConfiguration().getProtocolOptions().getMaxSchemaAgreementWaitSeconds()).isEqualTo(2);
		assertThat(cluster.getConfiguration().getPolicies().getSpeculativeExecutionPolicy())
				.isEqualTo(speculativeExecutionPolicy);
		assertThat(cluster.getConfiguration().getPolicies().getTimestampGenerator()).isEqualTo(timestampGenerator);
	}

	@Test
	public void clusterBuilderConfigurerWasCalled() {

		assertThat(clusterBuilderConfigurer).isInstanceOf(TestClusterBuilderConfigurer.class);
		assertThat(((TestClusterBuilderConfigurer) clusterBuilderConfigurer).configureCalled.get()).isTrue();
	}

	@Test // DATACASS-298
	public void localAndRemotePoolingOptionsWereConfiguredProperly() {

		PoolingOptions poolingOptions = cluster.getConfiguration().getPoolingOptions();

		assertThat(poolingOptions).isNotNull();
		assertThat(poolingOptions.getHeartbeatIntervalSeconds()).isEqualTo(60);
		assertThat(poolingOptions.getIdleTimeoutSeconds()).isEqualTo(300);
		assertThat(poolingOptions.getInitializationExecutor()).isEqualTo(executor);
		assertThat(poolingOptions.getCoreConnectionsPerHost(HostDistance.LOCAL)).isEqualTo(2);
		assertThat(poolingOptions.getMaxConnectionsPerHost(HostDistance.LOCAL)).isEqualTo(8);
		assertThat(poolingOptions.getMaxRequestsPerConnection(HostDistance.LOCAL)).isEqualTo(100);
		assertThat(poolingOptions.getNewConnectionThreshold(HostDistance.LOCAL)).isEqualTo(25);
		assertThat(poolingOptions.getCoreConnectionsPerHost(HostDistance.REMOTE)).isEqualTo(1);
		assertThat(poolingOptions.getMaxConnectionsPerHost(HostDistance.REMOTE)).isEqualTo(2);
		assertThat(poolingOptions.getMaxRequestsPerConnection(HostDistance.REMOTE)).isEqualTo(100);
		assertThat(poolingOptions.getNewConnectionThreshold(HostDistance.REMOTE)).isEqualTo(25);
	}

	@Test // DATACASS-298
	public void socketOptionsWereConfiguredProperly() {

		SocketOptions socketOptions = cluster.getConfiguration().getSocketOptions();

		assertThat(socketOptions).isNotNull();
		assertThat(socketOptions.getConnectTimeoutMillis()).isEqualTo(5000);
		assertThat(socketOptions.getKeepAlive()).isTrue();
		assertThat(socketOptions.getReadTimeoutMillis()).isEqualTo(60000);
		assertThat(socketOptions.getReceiveBufferSize()).isEqualTo(65536);
		assertThat(socketOptions.getReuseAddress()).isTrue();
		assertThat(socketOptions.getSendBufferSize()).isEqualTo(65536);
		assertThat(socketOptions.getSoLinger()).isEqualTo(60);
		assertThat(socketOptions.getTcpNoDelay()).isTrue();
	}

	public static class TestClusterBuilderConfigurer implements ClusterBuilderConfigurer {

		AtomicBoolean configureCalled = new AtomicBoolean(false);

		@Override
		public Cluster.Builder configure(Cluster.Builder clusterBuilder) {
			configureCalled.set(true);
			return clusterBuilder;
		}
	}
}
