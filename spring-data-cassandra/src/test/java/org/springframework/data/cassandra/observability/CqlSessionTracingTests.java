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

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.cassandra.observability.CassandraObservation.*;

import io.micrometer.common.Tag;
import io.micrometer.common.Tags;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.observation.TimerObservationHandler;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.core.tck.MeterRegistryAssert;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.test.simple.SimpleSpan;
import io.micrometer.tracing.test.simple.SimpleTracer;
import io.micrometer.tracing.test.simple.SpanAssert;

import java.util.Deque;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.config.AbstractSessionConfiguration;
import org.springframework.data.cassandra.test.util.CassandraExtension;
import org.springframework.data.cassandra.test.util.IntegrationTestsSupport;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Statement;

/**
 * Verify that {@link CqlSessionTracingInterceptor} properly wraps {@link Statement} object with tracing.
 *
 * @author Greg Turnquist
 * @since 4.0.0
 */
public class CqlSessionTracingTests extends IntegrationTestsSupport {

	private static final String CREATE_KEYSPACE = "CREATE KEYSPACE ConfigTest " + "WITH "
			+ "REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };";

	private ConfigurableApplicationContext context;

	private CqlSession session;

	@BeforeEach
	void setUp() {

		this.context = new AnnotationConfigApplicationContext(Config.class);
		this.session = context.getBean(CqlSession.class);
	}

	@AfterEach
	void tearDown() {
		this.context.close();
	}

	@Test
	void shouldNotCreateAnyMetricsWhenThereIsNoObservation() {

		MeterRegistry meterRegistry = new SimpleMeterRegistry();
		ObservationRegistry observationRegistry = ObservationRegistry.create();
		observationRegistry.observationConfig().observationHandler(new TimerObservationHandler(meterRegistry));

		MeterRegistryAssert.then(meterRegistry).hasNoMetrics();
	}

	@Test
	void tracingNoStatementsShouldProduceNoMetrics() {

		MeterRegistry meterRegistry = new SimpleMeterRegistry();
		ObservationRegistry observationRegistry = ObservationRegistry.create();
		observationRegistry.observationConfig().observationHandler(new TimerObservationHandler(meterRegistry));

		CqlSessionTagsProvider tagsProvider = new DefaultCassandraTagsProvider();

		SimpleTracer tracer = new SimpleTracer();
		observationRegistry.observationConfig().observationHandler(new CqlSessionTracingObservationHandler(tracer));

		CqlSessionTracingFactory.wrap(session, observationRegistry, tagsProvider);

		MeterRegistryAssert.then(meterRegistry).hasNoMetrics();
	}

	@Test
	void shouldCreateObservationForCqlSessionOperations() {

		MeterRegistry meterRegistry = new SimpleMeterRegistry();
		ObservationRegistry observationRegistry = ObservationRegistry.create();
		observationRegistry.observationConfig().observationHandler(new TimerObservationHandler(meterRegistry));

		CqlSessionTagsProvider tagsProvider = new DefaultCassandraTagsProvider();
		SimpleTracer tracer = new SimpleTracer();
		observationRegistry.observationConfig().observationHandler(new CqlSessionTracingObservationHandler(tracer));

		Observation.start("test", observationRegistry).scoped(() -> {

			CqlSession traceSession = CqlSessionTracingFactory.wrap(session, observationRegistry, tagsProvider);

			traceSession.execute(CREATE_KEYSPACE);
			traceSession.executeAsync(CREATE_KEYSPACE);
			traceSession.prepare(CREATE_KEYSPACE);
			traceSession.prepareAsync(CREATE_KEYSPACE);
		});

		MeterRegistryAssert.then(meterRegistry).hasTimerWithNameAndTags(CASSANDRA_QUERY_OBSERVATION.getName(), Tags.of( //
				LowCardinalityTags.SESSION_NAME.of("s5"), //
				LowCardinalityTags.KEYSPACE_NAME.of("unknown"), //
				Tag.of("error", "none") //
		));

		assertThat(tracer.getSpans()).hasSize(4);

		assertThat(findSpan(tracer.getSpans(), LowCardinalityTags.METHOD_NAME.getKey(), "execute")).isNotNull();
		assertThat(findSpan(tracer.getSpans(), LowCardinalityTags.METHOD_NAME.getKey(), "executeAsync")).isNotNull();
		assertThat(findSpan(tracer.getSpans(), LowCardinalityTags.METHOD_NAME.getKey(), "prepare")).isNotNull();
		assertThat(findSpan(tracer.getSpans(), LowCardinalityTags.METHOD_NAME.getKey(), "prepareAsync")).isNotNull();

		tracer.getSpans().forEach(simpleSpan -> SpanAssert.then(simpleSpan) //
				.hasRemoteServiceNameEqualTo("cassandra-s5") //
				.hasNameEqualTo(CASSANDRA_QUERY_OBSERVATION.getContextualName()) //
				.hasTag(LowCardinalityTags.SESSION_NAME.getKey(), "s5") //
				.hasTag(LowCardinalityTags.KEYSPACE_NAME.getKey(), "unknown") //
				.hasTag(HighCardinalityTags.CQL_TAG.getKey(), CREATE_KEYSPACE) //
				.hasIpThatIsBlank() //
				.hasPortEqualTo(0) //
				.hasKindEqualTo(Span.Kind.CLIENT));
	}

	/**
	 * Find a {@link Span} with a specific key and value.
	 *
	 * @param spans
	 * @param key
	 * @param value
	 * @return
	 */
	private SimpleSpan findSpan(Deque<SimpleSpan> spans, String key, String value) {

		return spans.stream() //
				.filter(simpleSpan -> {
					Map<String, String> tags = simpleSpan.getTags();
					return tags.containsKey(key) && tags.get(key).equals(value);
				}) //
				.findAny() //
				.orElse(null);

	}

	@Configuration
	static class Config extends AbstractSessionConfiguration {

		@Override
		protected String getKeyspaceName() {
			return "system";
		}

		@Override
		protected int getPort() {
			return CassandraExtension.getResources().getPort();
		}
	}
}
