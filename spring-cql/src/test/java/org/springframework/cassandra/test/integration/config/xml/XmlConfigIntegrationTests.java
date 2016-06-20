/*
 * Copyright 2013-2016 the original author or authors.
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
package org.springframework.cassandra.test.integration.config.xml;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.concurrent.Executor;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.cassandra.test.integration.AbstractEmbeddedCassandraIntegrationTest;
import org.springframework.cassandra.test.integration.KeyspaceRule;
import org.springframework.cassandra.test.integration.config.IntegrationTestUtils;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;

/**
 * Test XML namespace configuration using the spring-cql-1.0.xsd.
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
	private Cluster cluster;
	private Executor executor;
	private Session session;

	@Before
	public void setUp() {
		this.applicationContext = new ClassPathXmlApplicationContext(
			"XmlConfigIntegrationTests-context.xml", getClass());

		this.cluster = applicationContext.getBean(Cluster.class);
		this.executor = applicationContext.getBean(Executor.class);
		this.session = applicationContext.getBean(Session.class);
	}

	@After
	public void tearDown() {
		if (this.applicationContext != null) {
			this.applicationContext.close();
		}
	}

	@Test
	public void keyspaceExists() {
		IntegrationTestUtils.assertKeyspaceExists(KEYSPACE, session);
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-298">DATACASS-298</a>
	 */
	@Test
	public void localAndRemotePoolingOptionsWereConfiguredProperly() {

		PoolingOptions poolingOptions = cluster.getConfiguration().getPoolingOptions();

		assertThat(poolingOptions, is(notNullValue(PoolingOptions.class)));
		assertThat(poolingOptions.getHeartbeatIntervalSeconds(), is(equalTo(60)));
		assertThat(poolingOptions.getIdleTimeoutSeconds(), is(equalTo(300)));
		assertThat(poolingOptions.getInitializationExecutor(), is(equalTo(executor)));
		assertThat(poolingOptions.getPoolTimeoutMillis(), is(equalTo(15000)));
		assertThat(poolingOptions.getCoreConnectionsPerHost(HostDistance.LOCAL), is(equalTo(2)));
		assertThat(poolingOptions.getMaxConnectionsPerHost(HostDistance.LOCAL), is(equalTo(8)));
		assertThat(poolingOptions.getMaxRequestsPerConnection(HostDistance.LOCAL), is(equalTo(100)));
		assertThat(poolingOptions.getNewConnectionThreshold(HostDistance.LOCAL), is(equalTo(25)));
		assertThat(poolingOptions.getCoreConnectionsPerHost(HostDistance.REMOTE), is(equalTo(1)));
		assertThat(poolingOptions.getMaxConnectionsPerHost(HostDistance.REMOTE), is(equalTo(2)));
		assertThat(poolingOptions.getMaxRequestsPerConnection(HostDistance.REMOTE), is(equalTo(100)));
		assertThat(poolingOptions.getNewConnectionThreshold(HostDistance.REMOTE), is(equalTo(25)));
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-298">DATACASS-298</a>
	 */
	@Test
	public void socketOptionsWereConfiguredProperly() {

		SocketOptions socketOptions = cluster.getConfiguration().getSocketOptions();

		assertThat(socketOptions, is(notNullValue(SocketOptions.class)));
		assertThat(socketOptions.getConnectTimeoutMillis(), is(equalTo(5000)));
		assertThat(socketOptions.getKeepAlive(), is(true));
		assertThat(socketOptions.getReadTimeoutMillis(), is(equalTo(60000)));
		assertThat(socketOptions.getReceiveBufferSize(), is(equalTo(65536)));
		assertThat(socketOptions.getReuseAddress(), is(true));
		assertThat(socketOptions.getSendBufferSize(), is(equalTo(65536)));
		assertThat(socketOptions.getSoLinger(), is(equalTo(60)));
		assertThat(socketOptions.getTcpNoDelay(), is(true));
	}
}
