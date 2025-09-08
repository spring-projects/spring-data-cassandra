/*
 * Copyright 2025 the original author or authors.
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

import io.micrometer.observation.Observation;
import io.micrometer.observation.tck.TestObservationRegistry;

import org.junit.jupiter.api.Test;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;

/**
 * Unit test for {@link ObservationStatement}
 *
 * @author Mark Paluch
 */
class ObservationStatementUnitTests {

	@Test // GH-1601
	void equalsAndHashCodeShouldBeEqual() {

		TestObservationRegistry registry = TestObservationRegistry.create();

		Observation observation1 = Observation.start("foo", registry);
		Observation observation2 = Observation.start("bar", registry);

		SimpleStatement statement1 = ObservationStatement.createProxy(observation1,
				SimpleStatement.newInstance("SELECT * FROM foo"));
		SimpleStatement statement2 = ObservationStatement.createProxy(observation2,
				SimpleStatement.newInstance("SELECT * FROM foo"));
		SimpleStatement statement3 = ObservationStatement.createProxy(observation2,
				SimpleStatement.newInstance("SELECT * FROM bar"));

		assertThat(statement1).isEqualTo(statement2).hasSameHashCodeAs(statement2);
		assertThat(statement1).isNotEqualTo(statement3);
		assertThat(statement1.hashCode()).isNotEqualTo(statement3.hashCode());
	}
}
