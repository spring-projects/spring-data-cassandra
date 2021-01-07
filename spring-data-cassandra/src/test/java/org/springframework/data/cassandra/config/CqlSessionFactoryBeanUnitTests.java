/*
 * Copyright 2020-2021 the original author or authors.
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
import java.util.Collections;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.datastax.oss.driver.api.core.CqlSessionBuilder;

/**
 * Unit tests for {@link CqlSessionFactoryBean}.
 *
 * @author Tomasz Lelek
 * @author Mark Paluch
 */
@ExtendWith(MockitoExtension.class)
class CqlSessionFactoryBeanUnitTests {

	@Mock CqlSessionBuilder cqlSessionBuilder;

	@Test // DATACASS-766
	void shouldConstructCqlSessionBuilderWithDefaultHostAndPort() {

		new MyCqlSessionFactoryBean(cqlSessionBuilder).buildBuilder();

		verify(cqlSessionBuilder).addContactPoint(InetSocketAddress
				.createUnresolved(CqlSessionFactoryBean.DEFAULT_CONTACT_POINTS, CqlSessionFactoryBean.DEFAULT_PORT));
	}

	@Test // DATACASS-766
	void shouldConstructCqlSessionBuilderWithDefaultHostAndNonDefaultPort() {

		CqlSessionFactoryBean cqlSessionFactoryBean = new MyCqlSessionFactoryBean(cqlSessionBuilder);
		cqlSessionFactoryBean.setPort(1000);
		cqlSessionFactoryBean.buildBuilder();

		verify(cqlSessionBuilder)
				.addContactPoint(InetSocketAddress.createUnresolved(CqlSessionFactoryBean.DEFAULT_CONTACT_POINTS, 1000));
	}

	@Test // DATACASS-766
	void shouldConstructCqlSessionBuilderWithMultipleContactPointsSamePort() {

		CqlSessionFactoryBean cqlSessionFactoryBean = new MyCqlSessionFactoryBean(cqlSessionBuilder);
		cqlSessionFactoryBean.setPort(1000);
		cqlSessionFactoryBean.setContactPoints("a,b,c");
		cqlSessionFactoryBean.buildBuilder();

		verify(cqlSessionBuilder).addContactPoint(InetSocketAddress.createUnresolved("a", 1000));
		verify(cqlSessionBuilder).addContactPoint(InetSocketAddress.createUnresolved("b", 1000));
		verify(cqlSessionBuilder).addContactPoint(InetSocketAddress.createUnresolved("c", 1000));
	}

	@Test // DATACASS-766
	void constructCqlSessionBuilderWithMultipleContactPointsDifferentPorts() {

		CqlSessionFactoryBean cqlSessionFactoryBean = new MyCqlSessionFactoryBean(cqlSessionBuilder);
		cqlSessionFactoryBean.setContactPoints("a:1000,b:1001,c:1002");
		cqlSessionFactoryBean.buildBuilder();

		verify(cqlSessionBuilder).addContactPoint(InetSocketAddress.createUnresolved("a", 1000));
		verify(cqlSessionBuilder).addContactPoint(InetSocketAddress.createUnresolved("b", 1001));
		verify(cqlSessionBuilder).addContactPoint(InetSocketAddress.createUnresolved("c", 1002));
	}

	@Test // DATACASS-766
	void shouldConstructCqlSessionBuilderWithExplicitPortsAndDefaultPort() {

		CqlSessionFactoryBean cqlSessionFactoryBean = new MyCqlSessionFactoryBean(cqlSessionBuilder);
		cqlSessionFactoryBean.setContactPoints("a:1000,b:2000,c");
		cqlSessionFactoryBean.buildBuilder();

		verify(cqlSessionBuilder).addContactPoint(InetSocketAddress.createUnresolved("a", 1000));
		verify(cqlSessionBuilder).addContactPoint(InetSocketAddress.createUnresolved("b", 2000));
		verify(cqlSessionBuilder)
				.addContactPoint(InetSocketAddress.createUnresolved("c", CqlSessionFactoryBean.DEFAULT_PORT));
	}

	@Test // DATACASS-766
	void shouldConstructCqlSessionBuilderWithIpv6ContactPoint() {

		CqlSessionFactoryBean cqlSessionFactoryBean = new MyCqlSessionFactoryBean(cqlSessionBuilder);
		cqlSessionFactoryBean.setPort(1000);
		cqlSessionFactoryBean.setContactPoints("2001:db8:85a3:8d3:1319:8a2e:370:7348");
		cqlSessionFactoryBean.buildBuilder();

		verify(cqlSessionBuilder)
				.addContactPoint(InetSocketAddress.createUnresolved("2001:db8:85a3:8d3:1319:8a2e:370:7348", 1000));
	}

	@Test // DATACASS-766
	void shouldConstructCqlSessionBuilderWithIpv6ContactPointAndPort() {

		CqlSessionFactoryBean cqlSessionFactoryBean = new MyCqlSessionFactoryBean(cqlSessionBuilder);
		cqlSessionFactoryBean.setPort(1000);
		cqlSessionFactoryBean.setContactPoints("[2001:db8:85a3:8d3:1319:8a2e:370:7348]:1234");
		cqlSessionFactoryBean.buildBuilder();

		verify(cqlSessionBuilder)
				.addContactPoint(InetSocketAddress.createUnresolved("[2001:db8:85a3:8d3:1319:8a2e:370:7348]", 1234));
	}

	@Test // DATACASS-766
	void shouldConstructCqlSessionBuilderWithContactPointsProvidedAsInetSocketAddress() {

		CqlSessionFactoryBean cqlSessionFactoryBean = new MyCqlSessionFactoryBean(cqlSessionBuilder);
		cqlSessionFactoryBean.setContactPoints(Collections.singletonList(InetSocketAddress.createUnresolved("a", 1000)));
		cqlSessionFactoryBean.buildBuilder();

		verify(cqlSessionBuilder).addContactPoint(InetSocketAddress.createUnresolved("a", 1000));
	}

	@Test // DATACASS-766
	void shouldConstructCqlSessionBuilderLastSetContactPointsOverridePreviousInet() {

		CqlSessionFactoryBean cqlSessionFactoryBean = new MyCqlSessionFactoryBean(cqlSessionBuilder);
		cqlSessionFactoryBean.setContactPoints(Collections.singletonList(InetSocketAddress.createUnresolved("a", 1000)));
		cqlSessionFactoryBean.setContactPoints("b:1000");
		cqlSessionFactoryBean.buildBuilder();

		verify(cqlSessionBuilder).addContactPoint(InetSocketAddress.createUnresolved("b", 1000));
	}

	@Test // DATACASS-766
	void shouldConstructCqlSessionBuilderLastSetContactPointsOverridePreviousString() {

		CqlSessionFactoryBean cqlSessionFactoryBean = new MyCqlSessionFactoryBean(cqlSessionBuilder);
		cqlSessionFactoryBean.setContactPoints("b:1000");
		cqlSessionFactoryBean.setContactPoints(Collections.singletonList(InetSocketAddress.createUnresolved("a", 1000)));
		cqlSessionFactoryBean.buildBuilder();

		verify(cqlSessionBuilder).addContactPoint(InetSocketAddress.createUnresolved("a", 1000));
	}

	static class MyCqlSessionFactoryBean extends CqlSessionFactoryBean {

		final CqlSessionBuilder cqlSessionBuilder;

		MyCqlSessionFactoryBean(CqlSessionBuilder cqlSessionBuilder) {
			this.cqlSessionBuilder = cqlSessionBuilder;
		}

		@Override
		CqlSessionBuilder createBuilder() {
			return cqlSessionBuilder;
		}
	}
}
