/*
 * Copyright 2016 the original author or authors.
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
package org.springframework.cassandra.test.unit.config;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.cassandra.config.CassandraCqlClusterFactoryBean;
import org.springframework.cassandra.test.integration.AbstractEmbeddedCassandraIntegrationTest;

import com.datastax.driver.core.ProtocolVersion;

/**
 * Integration tests for {@link CassandraCqlClusterFactoryBean}.
 *
 * @author Kirk Clemens
 */
public class CassandraCqlClusterFactoryBeanTests extends AbstractEmbeddedCassandraIntegrationTest {

	private CassandraCqlClusterFactoryBean cassandraCqlClusterFactoryBean;

	@Before
	public void setUp() throws Exception {
		cassandraCqlClusterFactoryBean = new CassandraCqlClusterFactoryBean();
	}

	@After
	public void tearDown() throws Exception {
		cassandraCqlClusterFactoryBean.destroy();
	}

	@Test
	public void configuredProtocolVersionShouldBeSet() throws Exception {

		cassandraCqlClusterFactoryBean.setProtocolVersion(ProtocolVersion.V2);
		cassandraCqlClusterFactoryBean.setPort(CASSANDRA_NATIVE_PORT);
		cassandraCqlClusterFactoryBean.afterPropertiesSet();

		assertEquals(ProtocolVersion.V2, getProtocolVersionEnum(cassandraCqlClusterFactoryBean));
	}

	@Test
	public void defaultProtocolVersionShouldBeSet() throws Exception {

		cassandraCqlClusterFactoryBean.setPort(CASSANDRA_NATIVE_PORT);
		cassandraCqlClusterFactoryBean.afterPropertiesSet();

		assertEquals(ProtocolVersion.NEWEST_SUPPORTED, getProtocolVersionEnum(cassandraCqlClusterFactoryBean));
	}

	private ProtocolVersion getProtocolVersionEnum(CassandraCqlClusterFactoryBean cassandraCqlClusterFactoryBean)
			throws Exception {

		// initialize connection factory
		cassandraCqlClusterFactoryBean.getObject().init();
		return cassandraCqlClusterFactoryBean.getObject().getConfiguration().getProtocolOptions().getProtocolVersionEnum();
	}
}
