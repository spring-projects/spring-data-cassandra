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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.core.cql.CqlTemplate;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.SimpleUserTypeResolver;
import org.springframework.data.cassandra.repository.support.AbstractSpringDataEmbeddedCassandraIntegrationTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.util.ReflectionTestUtils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.SocketOptions;

/**
 * Integration tests for XML-based Cassandra configuration using the Cassandra namespace parsed with
 * {@link CqlNamespaceHandler}.
 *
 * @author Mark Paluch
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class CassandraNamespaceIntegrationTests extends AbstractSpringDataEmbeddedCassandraIntegrationTest {

	@Autowired ApplicationContext applicationContext;

	@Test // DATACASS-271
	public void clusterShouldHaveCompressionSet() {

		Cluster cluster = applicationContext.getBean(Cluster.class);
		Configuration configuration = cluster.getConfiguration();
		assertThat(configuration.getProtocolOptions().getCompression()).isEqualTo(Compression.SNAPPY);
	}

	@Test // DATACASS-271
	public void clusterShouldHavePoolingOptionsConfigured() {

		Cluster cluster = applicationContext.getBean(Cluster.class);
		PoolingOptions poolingOptions = cluster.getConfiguration().getPoolingOptions();

		assertThat(poolingOptions.getMaxRequestsPerConnection(HostDistance.LOCAL)).isEqualTo(101);
		assertThat(poolingOptions.getMaxRequestsPerConnection(HostDistance.REMOTE)).isEqualTo(100);

		assertThat(poolingOptions.getCoreConnectionsPerHost(HostDistance.LOCAL)).isEqualTo(3);
		assertThat(poolingOptions.getCoreConnectionsPerHost(HostDistance.REMOTE)).isEqualTo(1);

		assertThat(poolingOptions.getMaxConnectionsPerHost(HostDistance.LOCAL)).isEqualTo(9);
		assertThat(poolingOptions.getMaxConnectionsPerHost(HostDistance.REMOTE)).isEqualTo(2);
	}

	@Test // DATACASS-271
	public void clusterShouldHaveSocketOptionsConfigured() {

		Cluster cluster = applicationContext.getBean(Cluster.class);
		SocketOptions socketOptions = cluster.getConfiguration().getSocketOptions();

		assertThat(socketOptions.getConnectTimeoutMillis()).isEqualTo(5000);
		assertThat(socketOptions.getKeepAlive()).isTrue();
		assertThat(socketOptions.getReuseAddress()).isTrue();
		assertThat(socketOptions.getTcpNoDelay()).isTrue();
		assertThat(socketOptions.getSoLinger()).isEqualTo(60);
		assertThat(socketOptions.getReceiveBufferSize()).isEqualTo(65536);
		assertThat(socketOptions.getSendBufferSize()).isEqualTo(65536);
	}

	@Test // DATACASS-172
	public void mappingContextShouldHaveUserTypeResolverConfigured() {

		CassandraMappingContext mappingContext = applicationContext.getBean(CassandraMappingContext.class);

		SimpleUserTypeResolver userTypeResolver = (SimpleUserTypeResolver) ReflectionTestUtils.getField(mappingContext,
				"userTypeResolver");

		assertThat(userTypeResolver).isNotNull();
	}

	@Test // DATACASS-417
	public void mappingContextShouldCassandraTemplateConfigured() {

		CassandraTemplate cassandraTemplate = applicationContext.getBean(CassandraTemplate.class);
		CqlTemplate cqlTemplate = applicationContext.getBean(CqlTemplate.class);

		assertThat(cassandraTemplate.getCqlOperations()).isSameAs(cqlTemplate);
	}
}
