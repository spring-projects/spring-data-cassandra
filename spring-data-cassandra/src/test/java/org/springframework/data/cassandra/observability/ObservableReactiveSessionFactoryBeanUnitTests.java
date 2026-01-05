/*
 * Copyright 2023-present the original author or authors.
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
package org.springframework.data.cassandra.observability;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.micrometer.observation.ObservationRegistry;

import org.junit.jupiter.api.Test;
import org.springframework.data.cassandra.ReactiveSession;
import org.springframework.data.cassandra.core.cql.session.DefaultBridgedReactiveSession;
import org.springframework.test.util.ReflectionTestUtils;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;

/**
 * Unit tests for {@link ObservableReactiveSessionFactoryBean}.
 *
 * @author Mark Paluch
 */
class ObservableReactiveSessionFactoryBeanUnitTests {

	@Test // GH-1366
	void sessionFactoryBeanUnwrapsObservationProxy() throws Exception {

		CqlSession sessionMock = mock(CqlSession.class);
		ObservationRegistry registry = ObservationRegistry.NOOP;

		CqlSession wrapped = ObservableCqlSessionFactory.wrap(sessionMock, registry);

		ObservableReactiveSessionFactoryBean bean = new ObservableReactiveSessionFactoryBean(wrapped, registry);
		bean.afterPropertiesSet();

		ReactiveSession object = bean.getObject();
		Object cqlSession = ReflectionTestUtils.getField(ReflectionTestUtils.getField(object, "delegate"), "session");

		assertThat(cqlSession).isSameAs(sessionMock);
	}

	@Test // GH-1366
	void reactiveSessionDelegateBeanUnwrapsObservationProxy() throws Exception {

		CqlSession sessionMock = mock(CqlSession.class);
		ObservationRegistry registry = ObservationRegistry.NOOP;

		CqlSession wrapped = ObservableCqlSessionFactory.wrap(sessionMock, registry);

		ReactiveSession object = new DefaultBridgedReactiveSession(wrapped);
		Object cqlSession = ReflectionTestUtils.getField(object, "session");

		assertThat(cqlSession).isSameAs(sessionMock);
	}

	@Test // GH-1366
	void closesCqlSessionViaBuilder() throws Exception {

		CqlSessionBuilder builderMock = mock(CqlSessionBuilder.class);
		CqlSession sessionMock = mock(CqlSession.class);

		doReturn(sessionMock).when(builderMock).build();

		ObservationRegistry registry = ObservationRegistry.NOOP;

		ObservableReactiveSessionFactoryBean bean = new ObservableReactiveSessionFactoryBean(builderMock, registry);
		bean.afterPropertiesSet();
		bean.destroy();

		verify(sessionMock).close();
	}

	@Test // GH-1366
	void doesNotCloseCqlSession() throws Exception {

		CqlSession sessionMock = mock(CqlSession.class);

		ObservationRegistry registry = ObservationRegistry.NOOP;

		ObservableReactiveSessionFactoryBean bean = new ObservableReactiveSessionFactoryBean(sessionMock, registry);
		bean.afterPropertiesSet();
		bean.destroy();

		verifyNoInteractions(sessionMock);
	}

	@Test // GH-1490
	void considersConvention() throws Exception {

		CqlSession sessionMock = mock(CqlSession.class);
		CassandraObservationConvention conventionMock = mock(CassandraObservationConvention.class);
		ObservationRegistry registry = ObservationRegistry.NOOP;

		CqlSession wrapped = ObservableCqlSessionFactory.wrap(sessionMock, registry);

		ObservableReactiveSessionFactoryBean bean = new ObservableReactiveSessionFactoryBean(wrapped, registry);
		bean.setConvention(conventionMock);
		bean.afterPropertiesSet();

		ReactiveSession object = bean.getObject();
		Object usedConvention = ReflectionTestUtils.getField(object, "convention");

		assertThat(usedConvention).isSameAs(conventionMock);
	}
}
