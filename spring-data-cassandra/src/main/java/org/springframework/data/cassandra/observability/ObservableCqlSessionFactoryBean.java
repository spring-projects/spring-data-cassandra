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

import org.springframework.beans.factory.config.AbstractFactoryBean;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;

import io.micrometer.observation.ObservationRegistry;

/**
 * Factory bean to construct a {@link CqlSession} integrated with given {@link ObservationRegistry}. This factory bean
 * registers also {@link ObservationRequestTracker#INSTANCE ObservationRequestTracker.INSTANCE} with the builder to
 * ensure full integration with the required infrastructure.
 *
 * @author Mark Paluch
 * @since 4.0
 * @see ObservationRequestTracker
 * @see ObservableCqlSessionFactory
 */
public class ObservableCqlSessionFactoryBean extends AbstractFactoryBean<CqlSession> {

	private final CqlSessionBuilder cqlSessionBuilder;

	private final ObservationRegistry observationRegistry;

	private @Nullable String remoteServiceName;

	/**
	 * Construct a new {@link ObservableCqlSessionFactoryBean}.
	 *
	 * @param cqlSessionBuilder must not be {@literal null}.
	 * @param observationRegistry must not be {@literal null}.
	 */
	public ObservableCqlSessionFactoryBean(CqlSessionBuilder cqlSessionBuilder, ObservationRegistry observationRegistry) {

		Assert.notNull(cqlSessionBuilder, "CqlSessionBuilder must not be null");
		Assert.notNull(observationRegistry, "ObservationRegistry must not be null");

		this.cqlSessionBuilder = cqlSessionBuilder;
		this.observationRegistry = observationRegistry;
	}

	@Override
	protected CqlSession createInstance() {

		cqlSessionBuilder.addRequestTracker(ObservationRequestTracker.INSTANCE);

		if (ObjectUtils.isEmpty(getRemoteServiceName())) {
			return ObservableCqlSessionFactory.wrap(cqlSessionBuilder.build(), observationRegistry);
		}

		return ObservableCqlSessionFactory.wrap(cqlSessionBuilder.build(), getRemoteServiceName(), observationRegistry);
	}

	@Override
	protected void destroyInstance(@Nullable CqlSession instance) {
		if (instance != null) {
			instance.close();
		}
	}

	@Override
	public Class<?> getObjectType() {
		return CqlSession.class;
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
