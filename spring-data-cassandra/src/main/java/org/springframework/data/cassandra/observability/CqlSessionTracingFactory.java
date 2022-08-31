/*
 * Copyright 2013-2022 the original author or authors.
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

import io.micrometer.observation.ObservationRegistry;

import org.springframework.aop.framework.ProxyFactory;

import com.datastax.oss.driver.api.core.CqlSession;

/**
 * Factory to wrap a {@link CqlSession} with a {@link CqlSessionTracingInterceptor}.
 *
 * @author Mark Paluch
 * @author Greg Turnquist
 * @since 4.0.0
 */
public final class CqlSessionTracingFactory {

	private CqlSessionTracingFactory() {
		throw new IllegalStateException("Can't instantiate a utility class");
	}

	/**
	 * Wrap the {@link CqlSession} with a {@link CqlSessionTracingInterceptor}.
	 *
	 * @param session
	 * @param observationRegistry
	 * @param observationConvention
	 * @return
	 */
	public static CqlSession wrap(CqlSession session, ObservationRegistry observationRegistry,
			CqlSessionObservationConvention observationConvention) {

		ProxyFactory proxyFactory = new ProxyFactory();

		proxyFactory.setTarget(session);
		proxyFactory.addAdvice(new CqlSessionTracingInterceptor(session, observationRegistry, observationConvention));
		proxyFactory.addInterface(CqlSession.class);

		return (CqlSession) proxyFactory.getProxy();
	}
}
