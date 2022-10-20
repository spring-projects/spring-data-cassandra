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
import io.micrometer.tracing.Tracer;

import java.util.Optional;

import org.springframework.context.annotation.Bean;

/**
 * Set of beans enable observability with Spring Data Cassandra.
 *
 * @author Greg Turnquist
 * @since 4.0.0
 */
public class CassandraObservationConfiguration {

	@Bean
	CqlSessionObservationConvention observationConvention() {
		return new DefaultCassandraObservationConvention();
	}

	@Bean
	CqlSessionTracingObservationHandler cqlSessionTracingObservationHandler(
			Optional<ObservationRegistry> observationRegistry, Tracer tracer) {

		CqlSessionTracingObservationHandler observationHandler = new CqlSessionTracingObservationHandler(tracer);
		observationRegistry.ifPresent(registry -> registry.observationConfig().observationHandler(observationHandler));
		return observationHandler;
	}

	@Bean
	CqlSessionTracingBeanPostProcessor traceCqlSessionBeanPostProcessor(ObservationRegistry observationRegistry,
			CqlSessionObservationConvention observationConvention) {
		return new CqlSessionTracingBeanPostProcessor(observationRegistry, observationConvention);
	}
}
