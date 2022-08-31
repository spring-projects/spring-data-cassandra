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

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;

import com.datastax.oss.driver.api.core.CqlSession;

/**
 * {@link BeanPostProcessor} to automatically wrap all {@link CqlSession}s with a {@link CqlSessionTracingInterceptor}.
 *
 * @author Marcin Grzejszczak
 * @author Mark Paluch
 * @author Greg Turnquist
 * @since 4.0.0
 */
public class CqlSessionTracingBeanPostProcessor implements BeanPostProcessor {

	private final ObservationRegistry observationRegistry;

	private final CqlSessionObservationConvention observationConvention;

	public CqlSessionTracingBeanPostProcessor(ObservationRegistry observationRegistry,
			CqlSessionObservationConvention observationConvention) {

		this.observationRegistry = observationRegistry;
		this.observationConvention = observationConvention;
	}

	@Override
	public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {

		if (bean instanceof CqlSession) {
			return CqlSessionTracingFactory.wrap((CqlSession) bean, this.observationRegistry, this.observationConvention);
		}

		return bean;
	}
}
