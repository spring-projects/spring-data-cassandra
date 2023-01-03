/*
 * Copyright 2022-2023 the original author or authors.
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

import org.springframework.beans.factory.config.AbstractFactoryBean;
import org.springframework.data.cassandra.ReactiveSession;
import org.springframework.data.cassandra.core.cql.session.DefaultBridgedReactiveSession;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import com.datastax.oss.driver.api.core.CqlSession;

/**
 * Factory bean to construct a {@link ReactiveSession} integrated with given {@link ObservationRegistry}. The required
 * {@link CqlSession} must be associated with {@link ObservationRequestTracker#INSTANCE
 * ObservationRequestTracker.INSTANCE} to ensure proper integration with all observability components. You can use
 * {@link ObservableCqlSessionFactoryBean} to obtain a properly configured {@link CqlSession}.
 *
 * @author Mark Paluch
 * @since 4.0
 * @see ObservationRequestTracker
 * @see ObservableReactiveSessionFactory
 */
public class ObservableReactiveSessionFactoryBean extends AbstractFactoryBean<ReactiveSession> {

	private final CqlSession cqlSession;

	private final ObservationRegistry observationRegistry;

	private @Nullable String remoteServiceName;

	/**
	 * Construct a new {@link ObservableReactiveSessionFactoryBean}.
	 *
	 * @param cqlSession must not be {@literal null}.
	 * @param observationRegistry must not be {@literal null}.
	 */
	public ObservableReactiveSessionFactoryBean(CqlSession cqlSession, ObservationRegistry observationRegistry) {

		Assert.notNull(cqlSession, "CqlSession must not be null");
		Assert.notNull(observationRegistry, "ObservationRegistry must not be null");

		this.cqlSession = cqlSession;
		this.observationRegistry = observationRegistry;
	}

	@Override
	protected ReactiveSession createInstance() {

		if (ObjectUtils.isEmpty(getRemoteServiceName())) {
			return ObservableReactiveSessionFactory.wrap(new DefaultBridgedReactiveSession(cqlSession), observationRegistry);
		}

		return ObservableReactiveSessionFactory.wrap(new DefaultBridgedReactiveSession(cqlSession), getRemoteServiceName(),
				observationRegistry);
	}

	@Override
	public Class<?> getObjectType() {
		return ReactiveSession.class;
	}

	@Nullable
	public String getRemoteServiceName() {
		return remoteServiceName;
	}

	/**
	 * Set the remote service name.
	 *
	 * @param remoteServiceName
	 */
	public void setRemoteServiceName(@Nullable String remoteServiceName) {
		this.remoteServiceName = remoteServiceName;
	}
}
