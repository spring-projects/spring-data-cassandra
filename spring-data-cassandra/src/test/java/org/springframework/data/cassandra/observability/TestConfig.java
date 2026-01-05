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

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.observation.DefaultMeterObservationHandler;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.ObservationRegistry;

import org.jspecify.annotations.Nullable;

import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.data.cassandra.config.SessionBuilderConfigurer;
import org.springframework.data.cassandra.support.AbstractTestJavaConfig;

import com.datastax.oss.driver.api.core.CqlSession;

/**
 * @author Mark Paluch
 */
class TestConfig extends AbstractTestJavaConfig {

	static final MeterRegistry METER_REGISTRY = new SimpleMeterRegistry();
	static final ObservationRegistry OBSERVATION_REGISTRY = ObservationRegistry.create();

	static {
		OBSERVATION_REGISTRY.observationConfig().observationHandler(new DefaultMeterObservationHandler(METER_REGISTRY));
	}

	@Override
	protected String getKeyspaceName() {
		return "system";
	}

	@Bean
	ObservationRegistry registry() {
		return OBSERVATION_REGISTRY;
	}

	@Bean
	CassandraObservationConvention observationContention() {
		return new DefaultCassandraObservationConvention();
	}

	@Override
	protected @Nullable SessionBuilderConfigurer getSessionBuilderConfigurer() {
		return sessionBuilder -> sessionBuilder.addRequestTracker(ObservationRequestTracker.INSTANCE);
	}

	@Override
	protected @Nullable Resource getDriverConfigurationResource() {
		return new ClassPathResource("application.conf");
	}

	@Bean
	ObservableReactiveSessionFactoryBean reactiveSession(CqlSession session, ObservationRegistry observationRegistry) {
		return new ObservableReactiveSessionFactoryBean(session, observationRegistry);
	}
}
