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
package org.springframework.cassandra.test.integration.config;

import static org.assertj.core.api.Assertions.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.cassandra.config.CassandraCqlClusterFactoryBean;
import org.springframework.cassandra.test.integration.support.IntegrationTestNettyOptions;
import org.springframework.test.util.ReflectionTestUtils;

import com.datastax.driver.core.ProtocolVersion;

/**
 * Unit tests for {@link CassandraCqlClusterFactoryBean}.
 *
 * @author Kirk Clemens
 * @author Mark Paluch
 */
public class CassandraCqlClusterFactoryBeanIntegrationTests {

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

		cassandraCqlClusterFactoryBean.setNettyOptions(IntegrationTestNettyOptions.INSTANCE);
		cassandraCqlClusterFactoryBean.setProtocolVersion(ProtocolVersion.V2);
		cassandraCqlClusterFactoryBean.afterPropertiesSet();

		assertThat(getProtocolVersionEnum(cassandraCqlClusterFactoryBean)).isEqualTo(ProtocolVersion.V2);
	}

	@Test
	public void defaultProtocolVersionShouldBeSet() throws Exception {

		cassandraCqlClusterFactoryBean.afterPropertiesSet();

		assertThat(getProtocolVersionEnum(cassandraCqlClusterFactoryBean)).isNull();
	}

	private ProtocolVersion getProtocolVersionEnum(CassandraCqlClusterFactoryBean cassandraCqlClusterFactoryBean)
			throws Exception {

		// initialize connection factory
		return (ProtocolVersion) ReflectionTestUtils.getField(
				cassandraCqlClusterFactoryBean.getObject().getConfiguration().getProtocolOptions(), "initialProtocolVersion");
	}
}
