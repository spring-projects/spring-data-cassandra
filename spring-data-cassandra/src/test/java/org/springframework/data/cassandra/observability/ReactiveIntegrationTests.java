/*
 * Copyright 2013-present the original author or authors.
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

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.contextpropagation.ObservationThreadLocalAccessor;
import io.micrometer.tracing.test.SampleTestRunner;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.context.Context;

import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.ReactiveResultSet;
import org.springframework.data.cassandra.ReactiveSession;
import org.springframework.data.cassandra.core.cql.ReactiveCqlTemplate;
import org.springframework.data.cassandra.test.util.CassandraExtension;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.datastax.oss.driver.api.core.cql.PreparedStatement;

/**
 * Collection of tests that log metrics and tracing.
 *
 * @author Mark Paluch
 */
@ExtendWith({ CassandraExtension.class, SpringExtension.class })
@ContextConfiguration(classes = TestConfig.class)
public class ReactiveIntegrationTests extends SampleTestRunner {

	@Autowired ReactiveSession observableSession;

	ReactiveIntegrationTests() {
		super(SampleRunnerConfig.builder().build());
	}

	@Override
	protected MeterRegistry createMeterRegistry() {
		return TestConfig.METER_REGISTRY;
	}

	@Override
	protected ObservationRegistry createObservationRegistry() {
		return TestConfig.OBSERVATION_REGISTRY;
	}

	@Override
	public SampleTestRunnerConsumer yourCode() {

		return (tracer, meterRegistry) -> {

			Observation intermediate = Observation.start("intermediate", createObservationRegistry());

			Mono<ReactiveResultSet> drop = observableSession.execute("DROP KEYSPACE IF EXISTS ObservationTest");
			Mono<ReactiveResultSet> create = observableSession.execute("CREATE KEYSPACE ObservationTest " + "WITH "
					+ "REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
			Mono<ReactiveResultSet> use = observableSession.execute("USE ObservationTest");
			Mono<ReactiveResultSet> createTable = observableSession
					.execute("CREATE TABLE IF NOT EXISTS person (id int, firstName text, lastName text, PRIMARY KEY(id));");

			ReactiveCqlTemplate template = new ReactiveCqlTemplate(observableSession);

			intermediate.observe(() -> {
				drop.then(create).then(use).then(createTable)
						.then(template.execute("INSERT INTO person (id,firstName,lastName) VALUES(?,?,?)", 1, "Walter", "White"))
						.contextWrite(Context.of(ObservationThreadLocalAccessor.KEY, intermediate)) //
						.as(StepVerifier::create) //
						.expectNextCount(1) //
						.verifyComplete();
			});

			PreparedStatement prepare1 = observableSession
					.prepare("INSERT INTO person (id, firstName, lastName) VALUES (?, ?, ?);").block();
			PreparedStatement prepare2 = observableSession
					.prepare("INSERT INTO person (id, firstName, lastName) VALUES (?, ?, ?);").block();

			assertThat(prepare1).isSameAs(prepare2);

			assertThat(tracer.getFinishedSpans()).hasSizeGreaterThanOrEqualTo(5);
		};
	}

}
