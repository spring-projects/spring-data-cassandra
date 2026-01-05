/*
 * Copyright 2022-present the original author or authors.
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

import org.springframework.aop.TargetSource;
import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.beans.factory.config.AbstractFactoryBean;
import org.springframework.data.cassandra.ReactiveSession;
import org.springframework.data.cassandra.core.cql.session.DefaultBridgedReactiveSession;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;

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

	private final boolean requiresDestroy;

	private final ObservationRegistry observationRegistry;

	private @Nullable String remoteServiceName;

	private CassandraObservationConvention convention = DefaultCassandraObservationConvention.INSTANCE;

	/**
	 * Construct a new {@link ObservableReactiveSessionFactoryBean}.
	 *
	 * @param cqlSessionBuilder must not be {@literal null}.
	 * @param observationRegistry must not be {@literal null}.
	 * @since 4.0.5
	 */
	public ObservableReactiveSessionFactoryBean(CqlSessionBuilder cqlSessionBuilder,
			ObservationRegistry observationRegistry) {

		Assert.notNull(cqlSessionBuilder, "CqlSessionBuilder must not be null");
		Assert.notNull(observationRegistry, "ObservationRegistry must not be null");

		this.cqlSession = cqlSessionBuilder.build();
		this.requiresDestroy = true;
		this.observationRegistry = observationRegistry;
	}

	/**
	 * Construct a new {@link ObservableReactiveSessionFactoryBean}.
	 *
	 * @param cqlSession must not be {@literal null}.
	 * @param observationRegistry must not be {@literal null}.
	 */
	public ObservableReactiveSessionFactoryBean(CqlSession cqlSession, ObservationRegistry observationRegistry) {

		Assert.notNull(cqlSession, "CqlSession must not be null");
		Assert.notNull(observationRegistry, "ObservationRegistry must not be null");

		this.cqlSession = cqlSession instanceof TargetSource c ? (CqlSession) AopProxyUtils.getSingletonTarget(c)
				: cqlSession;
		this.requiresDestroy = false;
		this.observationRegistry = observationRegistry;
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

	/**
	 * Set the observation convention.
	 *
	 * @param convention
	 * @since 4.3.4
	 */
	public void setConvention(CassandraObservationConvention convention) {
		this.convention = convention;
	}

	@Override
	protected ReactiveSession createInstance() {

		String remoteServiceName = ObjectUtils.isEmpty(getRemoteServiceName()) ? "Cassandra" : getRemoteServiceName();

		return ObservableReactiveSessionFactory.wrap(new DefaultBridgedReactiveSession(cqlSession), remoteServiceName,
				convention, observationRegistry);
	}

	@Override
	public Class<?> getObjectType() {
		return ReactiveSession.class;
	}

	@Override
	public void destroy() {

		if (requiresDestroy) {
			cqlSession.close();
		}
	}

}
