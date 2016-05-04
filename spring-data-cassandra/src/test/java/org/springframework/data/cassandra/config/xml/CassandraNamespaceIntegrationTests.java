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
package org.springframework.data.cassandra.config.xml;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.Is.is;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.data.cassandra.test.integration.support.AbstractSpringDataEmbeddedCassandraIntegrationTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.SocketOptions;

/**
 * Integration tests for XML-based Cassandra configuration using the Cassandra namespace parsed with
 * {@link CassandraNamespaceHandler}.
 *
 * @author Mark Paluch
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class CassandraNamespaceIntegrationTests extends AbstractSpringDataEmbeddedCassandraIntegrationTest {

	@Autowired ApplicationContext applicationContext;

	/**
	 * @see DATACASS-271
	 */
	@Test
	public void clusterShouldHaveCompressionSet() {

		Cluster cluster = applicationContext.getBean(Cluster.class);
		Configuration configuration = cluster.getConfiguration();
		assertThat(configuration.getProtocolOptions().getCompression(), is(Compression.SNAPPY));
	}

	/**
	 * @see DATACASS-271
	 */
	@Test
	public void clusterShouldHavePoolingOptionsConfigured() {

		Cluster cluster = applicationContext.getBean(Cluster.class);
		PoolingOptions poolingOptions = cluster.getConfiguration().getPoolingOptions();

		assertThat(poolingOptions.getMaxRequestsPerConnection(HostDistance.LOCAL), is(101));
		assertThat(poolingOptions.getMaxRequestsPerConnection(HostDistance.REMOTE), is(100));

		assertThat(poolingOptions.getCoreConnectionsPerHost(HostDistance.LOCAL), is(3));
		assertThat(poolingOptions.getCoreConnectionsPerHost(HostDistance.REMOTE), is(1));

		assertThat(poolingOptions.getMaxConnectionsPerHost(HostDistance.LOCAL), is(9));
		assertThat(poolingOptions.getMaxConnectionsPerHost(HostDistance.REMOTE), is(2));
	}

	/**
	 * @see DATACASS-271
	 */
	@Test
	public void clusterShouldHaveSocketOptionsConfigured() {

		Cluster cluster = applicationContext.getBean(Cluster.class);
		SocketOptions socketOptions = cluster.getConfiguration().getSocketOptions();

		assertThat(socketOptions.getConnectTimeoutMillis(), is(5000));
		assertThat(socketOptions.getKeepAlive(), is(true));
		assertThat(socketOptions.getReuseAddress(), is(true));
		assertThat(socketOptions.getTcpNoDelay(), is(true));
		assertThat(socketOptions.getSoLinger(), is(equalTo(60)));
		assertThat(socketOptions.getReceiveBufferSize(), is(equalTo(65536)));
		assertThat(socketOptions.getSendBufferSize(), is(equalTo(65536)));
	}
}
