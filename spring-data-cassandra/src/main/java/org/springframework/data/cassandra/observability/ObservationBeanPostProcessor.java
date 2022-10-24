/*
 * Copyright 2022 the original author or authors.
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

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.data.cassandra.ReactiveSession;

import com.datastax.oss.driver.api.core.CqlSession;

import io.micrometer.observation.ObservationRegistry;

class ObservationBeanPostProcessor implements BeanPostProcessor {

	public final ObservationRegistry observationRegistry;

	public ObservationBeanPostProcessor(ObservationRegistry observationRegistry) {
		this.observationRegistry = observationRegistry;
	}

	@Override
	public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {

		if (bean instanceof CqlSession) {
			return ObservableCqlSessionFactory.wrap((CqlSession) bean, observationRegistry);
		}

		if (bean instanceof ReactiveSession) {
			return ObservableReactiveSessionFactory.wrap((ReactiveSession) bean, observationRegistry);
		}

		return BeanPostProcessor.super.postProcessAfterInitialization(bean, beanName);
	}
}
