/*
 * Copyright 2017-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.config;

import static org.mockito.Mockito.*;

import java.net.InetSocketAddress;
import java.util.Arrays;

import org.assertj.core.api.Assertions;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.datastax.oss.driver.api.core.CqlSessionBuilder;

/**
 * @author Tomasz Lelek
 */
@ExtendWith(MockitoExtension.class)
class CqlSessionFactoryBeanUnitTests {

	@Mock CqlSessionBuilder cqlSessionBuilder;

	@Test
	public void constructCqlSessionBuilderWithDefaultHostAndPort() {
		new CqlSessionFactoryBean(() -> cqlSessionBuilder).buildBuilder();

		verify(cqlSessionBuilder, times(1)).addContactPoint(InetSocketAddress
				.createUnresolved(CqlSessionFactoryBean.DEFAULT_CONTACT_POINTS, CqlSessionFactoryBean.DEFAULT_PORT));
	}

	@Test
	public void constructCqlSessionBuilderWithDefaultHostAndNonDefaultPort() {
		CqlSessionFactoryBean cqlSessionFactoryBean = new CqlSessionFactoryBean(() -> cqlSessionBuilder);
		cqlSessionFactoryBean.setPort(1000);
		cqlSessionFactoryBean.buildBuilder();

		verify(cqlSessionBuilder, times(1))
				.addContactPoint(InetSocketAddress.createUnresolved(CqlSessionFactoryBean.DEFAULT_CONTACT_POINTS, 1000));
	}

	@Test
	public void constructCqlSessionBuilderWithMultipleContactPointsSamePort() {
		CqlSessionFactoryBean cqlSessionFactoryBean = new CqlSessionFactoryBean(() -> cqlSessionBuilder);
		cqlSessionFactoryBean.setPort(1000);
		cqlSessionFactoryBean.setContactPoints("a,b,c");
		cqlSessionFactoryBean.buildBuilder();
		verify(cqlSessionBuilder, times(1)).addContactPoint(InetSocketAddress.createUnresolved("a", 1000));
		verify(cqlSessionBuilder, times(1)).addContactPoint(InetSocketAddress.createUnresolved("b", 1000));
		verify(cqlSessionBuilder, times(1)).addContactPoint(InetSocketAddress.createUnresolved("c", 1000));
	}

	@Test
	public void constructCqlSessionBuilderWithMultipleContactPointsDifferentPorts() {
		CqlSessionFactoryBean cqlSessionFactoryBean = new CqlSessionFactoryBean(() -> cqlSessionBuilder);
		cqlSessionFactoryBean.setContactPoints("a:1000,b:1001,c:1002");
		cqlSessionFactoryBean.buildBuilder();
		verify(cqlSessionBuilder, times(1)).addContactPoint(InetSocketAddress.createUnresolved("a", 1000));
		verify(cqlSessionBuilder, times(1)).addContactPoint(InetSocketAddress.createUnresolved("b", 1001));
		verify(cqlSessionBuilder, times(1)).addContactPoint(InetSocketAddress.createUnresolved("c", 1002));
	}


	@Test
	public void throwWhenContactPointsWithPortHasWrongFormat() {
		CqlSessionFactoryBean cqlSessionFactoryBean = new CqlSessionFactoryBean(() -> cqlSessionBuilder);
		cqlSessionFactoryBean.setContactPoints("a:1000:100");
		Assertions.assertThatThrownBy(cqlSessionFactoryBean::buildBuilder).isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("The provided contact point: a:1000:100 has wrong format.");
	}

	@Test
	public void constructCqlSessionBuilderWithExplictPortsAndDefaultPort() {
		CqlSessionFactoryBean cqlSessionFactoryBean = new CqlSessionFactoryBean(() -> cqlSessionBuilder);
		cqlSessionFactoryBean.setContactPoints("a:1000,b:2000,c");
		cqlSessionFactoryBean.buildBuilder();
		verify(cqlSessionBuilder, times(1)).addContactPoint(InetSocketAddress.createUnresolved("a", 1000));
		verify(cqlSessionBuilder, times(1)).addContactPoint(InetSocketAddress.createUnresolved("b", 2000));
		verify(cqlSessionBuilder, times(1))
				.addContactPoint(InetSocketAddress.createUnresolved("c", CqlSessionFactoryBean.DEFAULT_PORT));
	}

	@Test
	public void constructCqlSessionBuilderWithIpv6ContactPoint() {
		CqlSessionFactoryBean cqlSessionFactoryBean = new CqlSessionFactoryBean(() -> cqlSessionBuilder);
		cqlSessionFactoryBean.setPort(1000);
		cqlSessionFactoryBean.setContactPoints("[2001:db8:85a3:8d3:1319:8a2e:370:7348]");
		cqlSessionFactoryBean.buildBuilder();
		verify(cqlSessionBuilder, times(1))
				.addContactPoint(InetSocketAddress.createUnresolved("[2001:db8:85a3:8d3:1319:8a2e:370:7348]", 1000));
	}

	@Test
	public void constructCqlSessionBuilderWithContactPointsProvidedAsInetSocketAddress() {
		CqlSessionFactoryBean cqlSessionFactoryBean = new CqlSessionFactoryBean(() -> cqlSessionBuilder);
		cqlSessionFactoryBean.setContactPoints(Arrays.asList(InetSocketAddress.createUnresolved("a", 1000)));
		cqlSessionFactoryBean.buildBuilder();
		verify(cqlSessionBuilder, times(1)).addContactPoint(InetSocketAddress.createUnresolved("a", 1000));
	}

	@Test
	public void constructCqlSessionBuilderLastSetContactPointsOverridePreviousInet() {
		CqlSessionFactoryBean cqlSessionFactoryBean = new CqlSessionFactoryBean(() -> cqlSessionBuilder);
		cqlSessionFactoryBean.setContactPoints(Arrays.asList(InetSocketAddress.createUnresolved("a", 1000)));
		cqlSessionFactoryBean.setContactPoints("b:1000");
		cqlSessionFactoryBean.buildBuilder();
		verify(cqlSessionBuilder, times(1)).addContactPoint(InetSocketAddress.createUnresolved("b", 1000));
	}

	@Test
	public void constructCqlSessionBuilderLastSetContactPointsOverridePreviousString() {
		CqlSessionFactoryBean cqlSessionFactoryBean = new CqlSessionFactoryBean(() -> cqlSessionBuilder);
		cqlSessionFactoryBean.setContactPoints("b:1000");
		cqlSessionFactoryBean.setContactPoints(Arrays.asList(InetSocketAddress.createUnresolved("a", 1000)));
		cqlSessionFactoryBean.buildBuilder();
		verify(cqlSessionBuilder, times(1)).addContactPoint(InetSocketAddress.createUnresolved("a", 1000));
	}
}
