/*
 * Copyright 2013-2025 the original author or authors.
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
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.exporter.FinishedSpan;
import io.micrometer.tracing.test.SampleTestRunner;

import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.core.cql.CqlTemplate;
import org.springframework.data.cassandra.test.util.CassandraExtension;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;

/**
 * Collection of tests that log metrics and tracing.
 *
 * @author Greg Turnquist
 * @author Mark Paluch
 */
@ExtendWith({ CassandraExtension.class, SpringExtension.class })
@ContextConfiguration(classes = TestConfig.class)
public class ImperativeIntegrationTests extends SampleTestRunner {

	@Autowired CqlSession session;

	ImperativeIntegrationTests() {
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

			CqlSession observableSession = ObservableCqlSessionFactory.wrap(session, createObservationRegistry());

			observableSession.execute("DROP KEYSPACE IF EXISTS ObservationTest");
			observableSession.execute("CREATE KEYSPACE ObservationTest " + "WITH "
					+ "REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
			observableSession.execute("USE ObservationTest");
			observableSession
					.execute("CREATE TABLE IF NOT EXISTS person (id int, firstName text, lastName text, PRIMARY KEY(id));");

			CqlTemplate template = new CqlTemplate(observableSession);

			PreparedStatement prepare1 = observableSession
					.prepare("INSERT INTO person (id, firstName, lastName) VALUES (?, ?, ?);");
			PreparedStatement prepare2 = observableSession
					.prepare("INSERT INTO person (id, firstName, lastName) VALUES (?, ?, ?);");

			assertThat(prepare1).isSameAs(prepare2);

			template.execute("INSERT INTO person (id,firstName,lastName) VALUES(?,?,?)", 1, "Walter", "White");

			assertThat(tracer.getFinishedSpans()).hasSizeGreaterThanOrEqualTo(5);

			for (FinishedSpan finishedSpan : tracer.getFinishedSpans()) {

				assertThat(finishedSpan.getTags()).containsEntry("db.system", "cassandra");
				assertThat(finishedSpan.getTags()).containsKeys("db.operation", "db.statement",
						"spring.data.cassandra.methodName", "spring.data.cassandra.sessionName");
			}
		};
	}

}
