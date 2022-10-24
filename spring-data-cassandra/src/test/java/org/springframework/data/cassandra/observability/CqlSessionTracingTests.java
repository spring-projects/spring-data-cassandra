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

import java.util.Deque;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.config.AbstractSessionConfiguration;
import org.springframework.data.cassandra.test.util.CassandraExtension;
import org.springframework.data.cassandra.test.util.IntegrationTestsSupport;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Statement;

import io.micrometer.common.KeyValue;
import io.micrometer.common.KeyValues;
import io.micrometer.common.docs.KeyName;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.observation.DefaultMeterObservationHandler;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.core.tck.MeterRegistryAssert;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.test.simple.SimpleSpan;
import io.micrometer.tracing.test.simple.SimpleTracer;
import io.micrometer.tracing.test.simple.SpanAssert;

/**
 * Verify that {@link CqlSessionObservationInterceptor} properly wraps {@link Statement} object with tracing.
 *
 * @author Greg Turnquist
 * @since 4.0.0
 */
@ExtendWith({ SpringExtension.class, CassandraExtension.class })
public class CqlSessionTracingTests extends IntegrationTestsSupport {

	private static final String CREATE_KEYSPACE = "CREATE KEYSPACE ConfigTest " + "WITH "
			+ "REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };";

	private final ObservationRegistry observationRegistry = ObservationRegistry.create();

	private final MeterRegistry meterRegistry = new SimpleMeterRegistry();

	private final SimpleTracer tracer = new SimpleTracer();

	private ConfigurableApplicationContext context;

	private CqlSession session;

	@BeforeEach
	void setUp() {

		this.context = new AnnotationConfigApplicationContext(Config.class);
		this.session = ObservableCqlSessionFactory.wrap(context.getBean(CqlSession.class), "my-cassandra",
				observationRegistry);
		this.observationRegistry.observationConfig().observationHandler(new DefaultMeterObservationHandler(meterRegistry));
	}

	@AfterEach
	void tearDown() {
		this.context.close();
	}

	@Test
	void shouldCreateObservationForCqlSessionOperations() {

		Observation.start("test", observationRegistry).scoped(() -> {

			session.execute(CREATE_KEYSPACE);
			session.executeAsync(CREATE_KEYSPACE);
			session.prepare(CREATE_KEYSPACE);
			session.prepareAsync(CREATE_KEYSPACE);
		});

		MeterRegistryAssert.then(meterRegistry).hasTimerWithNameAndTags(CASSANDRA_QUERY_OBSERVATION.getName(), KeyValues.of( //
				LowCardinalityKeyNames.SESSION_NAME.withValue("s5"), //
				LowCardinalityKeyNames.KEYSPACE_NAME.withValue("system"), //
				KeyValue.of("error", "none") //
		));

		assertThat(tracer.getSpans()).hasSize(4);

		assertThat(findSpan(tracer.getSpans(), LowCardinalityKeyNames.METHOD_NAME, "execute")).isNotNull();
		assertThat(findSpan(tracer.getSpans(), LowCardinalityKeyNames.METHOD_NAME, "executeAsync")).isNotNull();
		assertThat(findSpan(tracer.getSpans(), LowCardinalityKeyNames.METHOD_NAME, "prepare")).isNotNull();
		assertThat(findSpan(tracer.getSpans(), LowCardinalityKeyNames.METHOD_NAME, "prepareAsync")).isNotNull();

		tracer.getSpans().forEach(simpleSpan -> SpanAssert.then(simpleSpan) //
				.hasRemoteServiceNameEqualTo("cassandra-s5") //
				.hasNameEqualTo(CASSANDRA_QUERY_OBSERVATION.getContextualName()) //
				.hasTag(LowCardinalityKeyNames.SESSION_NAME, "s5") //
				.hasTag(LowCardinalityKeyNames.KEYSPACE_NAME, "system") //
				.hasTag(HighCardinalityKeyNames.CQL_TAG, CREATE_KEYSPACE) //
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
	private SimpleSpan findSpan(Deque<SimpleSpan> spans, KeyName key, String value) {

		return spans.stream() //
				.filter(simpleSpan -> {
					Map<String, String> tags = simpleSpan.getTags();
					return tags.containsKey(key.asString()) && tags.get(key.asString()).equals(value);
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
