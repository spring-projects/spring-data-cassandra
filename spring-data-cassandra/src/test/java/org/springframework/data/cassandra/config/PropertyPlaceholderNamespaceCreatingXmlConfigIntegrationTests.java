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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.core.cql.CqlOperations;
import org.springframework.data.cassandra.support.KeyspaceTestUtils;
import org.springframework.data.cassandra.test.util.AbstractEmbeddedCassandraIntegrationTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;

/**
 * Integration tests for XML-based configuration using property placeholders.
 *
 * @author Mark Paluch
 * @author John Blum
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
@SuppressWarnings("unused")
public class PropertyPlaceholderNamespaceCreatingXmlConfigIntegrationTests
		extends AbstractEmbeddedCassandraIntegrationTest {

	@Autowired private Cluster cassandraCluster;
	@Autowired private CqlOperations ops;
	@Autowired private Session session;

	@Test
	public void keyspaceExists() {

		KeyspaceTestUtils.assertKeyspaceExists("ppncxct", session);

		assertThat(ops).isNotNull();
	}

	@Test // DATACASS-298
	public void localAndRemotePoolingOptionsWereConfiguredProperly() {

		PoolingOptions poolingOptions = cassandraCluster.getConfiguration().getPoolingOptions();

		assertThat(poolingOptions).isNotNull();
		assertThat(poolingOptions.getHeartbeatIntervalSeconds()).isEqualTo(60);
		assertThat(poolingOptions.getIdleTimeoutSeconds()).isEqualTo(180);
		assertThat(poolingOptions.getCoreConnectionsPerHost(HostDistance.LOCAL)).isEqualTo(4);
		assertThat(poolingOptions.getMaxConnectionsPerHost(HostDistance.LOCAL)).isEqualTo(8);
		assertThat(poolingOptions.getMaxRequestsPerConnection(HostDistance.LOCAL)).isEqualTo(20);
		assertThat(poolingOptions.getNewConnectionThreshold(HostDistance.LOCAL)).isEqualTo(10);
		assertThat(poolingOptions.getCoreConnectionsPerHost(HostDistance.REMOTE)).isEqualTo(2);
		assertThat(poolingOptions.getMaxConnectionsPerHost(HostDistance.REMOTE)).isEqualTo(4);
		assertThat(poolingOptions.getMaxRequestsPerConnection(HostDistance.REMOTE)).isEqualTo(10);
		assertThat(poolingOptions.getNewConnectionThreshold(HostDistance.REMOTE)).isEqualTo(5);
	}

	@Test // DATACASS-298
	public void socketOptionsWereConfiguredProperly() {

		SocketOptions socketOptions = cassandraCluster.getConfiguration().getSocketOptions();

		assertThat(socketOptions).isNotNull();
		assertThat(socketOptions.getConnectTimeoutMillis()).isEqualTo(15000);
		assertThat(socketOptions.getKeepAlive()).isTrue();
		assertThat(socketOptions.getReadTimeoutMillis()).isEqualTo(60000);
		assertThat(socketOptions.getReceiveBufferSize()).isEqualTo(1024);
		assertThat(socketOptions.getReuseAddress()).isTrue();
		assertThat(socketOptions.getSendBufferSize()).isEqualTo(2048);
		assertThat(socketOptions.getSoLinger()).isEqualTo(5);
		assertThat(socketOptions.getTcpNoDelay()).isFalse();
	}
}
