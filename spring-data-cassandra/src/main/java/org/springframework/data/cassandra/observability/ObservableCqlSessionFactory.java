/*
 * Copyright 2022-2025 the original author or authors.
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
import org.springframework.data.cassandra.observability.CqlSessionObservationInterceptor.ObservationDecoratedProxy;
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.CqlSession;

/**
 * Factory to wrap a {@link CqlSession} with a {@link CqlSessionObservationInterceptor}.
 *
 * @author Mark Paluch
 * @author Greg Turnquist
 * @since 4.0
 */
public final class ObservableCqlSessionFactory {

	private ObservableCqlSessionFactory() {
		throw new UnsupportedOperationException("Can't instantiate a utility class");
	}

	/**
	 * Wrap the {@link CqlSession} with a {@link CqlSessionObservationInterceptor}.
	 *
	 * @param session must not be {@literal null}.
	 * @param observationRegistry must not be {@literal null}.
	 * @return
	 */
	public static CqlSession wrap(CqlSession session, ObservationRegistry observationRegistry) {
		return wrap(session, "Cassandra", observationRegistry);
	}

	/**
	 * Wrap the {@link CqlSession} with a {@link CqlSessionObservationInterceptor}.
	 *
	 * @param session must not be {@literal null}.
	 * @param remoteServiceName must not be {@literal null}.
	 * @param observationRegistry must not be {@literal null}.
	 * @return
	 */
	public static CqlSession wrap(CqlSession session, String remoteServiceName, ObservationRegistry observationRegistry) {
		return wrap(session, remoteServiceName, DefaultCassandraObservationConvention.INSTANCE, observationRegistry);
	}

	/**
	 * Wrap the {@link CqlSession} with a {@link CqlSessionObservationInterceptor}.
	 *
	 * @param session must not be {@literal null}.
	 * @param remoteServiceName must not be {@literal null}.
	 * @param convention the observation convention.
	 * @param observationRegistry must not be {@literal null}.
	 * @return
	 * @since 4.3.4
	 */
	public static CqlSession wrap(CqlSession session, String remoteServiceName, CassandraObservationConvention convention,
			ObservationRegistry observationRegistry) {

		Assert.notNull(session, "CqlSession must not be null");
		Assert.notNull(remoteServiceName, "Remote service name must not be null");
		Assert.notNull(convention, "CassandraObservationConvention must not be null");
		Assert.notNull(observationRegistry, "ObservationRegistry must not be null");

		ProxyFactory proxyFactory = new ProxyFactory();

		proxyFactory.setTarget(session);
		proxyFactory
				.addAdvice(new CqlSessionObservationInterceptor(session, remoteServiceName, convention, observationRegistry));
		proxyFactory.addInterface(CqlSession.class);
		proxyFactory.addInterface(ObservationDecoratedProxy.class);

		return (CqlSession) proxyFactory.getProxy();
	}

}
