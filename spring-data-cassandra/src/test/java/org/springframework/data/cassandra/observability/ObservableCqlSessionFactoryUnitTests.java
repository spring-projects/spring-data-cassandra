/*
 * Copyright 2023-2025 the original author or authors.
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

import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.aop.support.AopUtils;

import com.datastax.oss.driver.api.core.CqlSession;

/**
 * Unit tests for {@link ObservableCqlSessionFactory}.
 *
 * @author Mark Paluch
 */
class ObservableCqlSessionFactoryUnitTests {

	@Test // GH-1426
	void sessionFactoryBeanUnwrapsObservationProxy() {

		CqlSession session = mock(CqlSession.class);
		ObservationRegistry registry = ObservationRegistry.NOOP;

		CqlSession object = ObservableCqlSessionFactory.wrap(session, registry);

		assertThat(AopUtils.getTargetClass(object)).isEqualTo(session.getClass());
		assertThat(AopProxyUtils.getSingletonTarget(object)).isEqualTo(session);
	}

}
