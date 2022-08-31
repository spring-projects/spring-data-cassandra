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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import io.micrometer.observation.ObservationRegistry;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.aop.Advisor;
import org.springframework.aop.framework.Advised;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.datastax.oss.driver.api.core.CqlSession;

/**
 * Verify that {@link CqlSessionTracingBeanPostProcessor} properly wraps {@link CqlSession} beans registered in the app
 * context.
 *
 * @author Greg Turnquist
 * @since 4.0.0
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration
public class CqlSessionTracingBeanPostProcessorTests {

	@Autowired CqlSession session;

	@Test
	void injectedCqlSessionShouldBeWrapped() throws Exception {

		assertThat(session).isInstanceOf(CqlSession.class);
		assertThat(session).isInstanceOf(Advised.class);

		Advised advised = (Advised) session;

		assertThat(advised.getAdvisors()).extracting(Advisor::getAdvice)
				.satisfies(advice -> advice.getClass().equals(CqlSessionTracingInterceptor.class));
		assertThat(advised.getTargetSource().getTarget()).isEqualTo(TestConfig.originalSession);
	}

	@Configuration
	static class TestConfig {

		static CqlSession originalSession = mock(CqlSession.class);

		@Bean
		CqlSession originalSession() {
			return originalSession;
		}

		@Bean
		ObservationRegistry meterRegistry() {
			return ObservationRegistry.create();
		}

		@Bean
		CqlSessionObservationConvention observationConvention() {
			return new DefaultCassandraObservationContention();
		}

		@Bean
		CqlSessionTracingBeanPostProcessor traceCqlSessionBeanPostProcessor(ObservationRegistry observationRegistry,
				CqlSessionObservationConvention tagsProvider) {
			return new CqlSessionTracingBeanPostProcessor(observationRegistry, tagsProvider);
		}
	}
}
