/*
 * Copyright 2017-2021 the original author or authors.
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
package org.springframework.data.cassandra.core.cql.generator;

import org.junit.jupiter.api.Test;
import org.springframework.data.cassandra.core.cql.generator.CreateKeyspaceCqlGeneratorUnitTests.BasicTest;
import org.springframework.data.cassandra.core.cql.generator.CreateKeyspaceCqlGeneratorUnitTests.CreateKeyspaceTest;
import org.springframework.data.cassandra.core.cql.generator.CreateKeyspaceCqlGeneratorUnitTests.NetworkTopologyTest;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTests;

/**
 * Integration tests that reuse unit tests.
 *
 * @author John McPeek
 * @author Oliver Gierke
 * @author Mark Paluch
 */
class CreateKeyspaceCqlGeneratorIntegrationTests {

	/**
	 * Integration test base class that knows how to do everything except instantiate the concrete unit test type T.
	 *
	 * @param <T> The concrete unit test class to which this integration test corresponds.
	 */
	static abstract class Base<T extends CreateKeyspaceTest> extends AbstractKeyspaceCreatingIntegrationTests {
		private T unit;

		protected abstract T unit();

		@Test
		void test() {
			unit = unit();
			unit.prepare();

			session.execute(unit.cql);

			CqlKeyspaceSpecificationAssertions.assertKeyspace(unit.specification, unit.keyspace, session);

			dropKeyspace(unit.keyspace);
		}
	}

	static class BasicIntegrationTest extends Base<BasicTest> {

		@Override
		public BasicTest unit() {
			return new BasicTest();
		}
	}

	static class NetworkTopologyIntegrationTest extends Base<NetworkTopologyTest> {

		@Override
		public NetworkTopologyTest unit() {
			return new NetworkTopologyTest();
		}
	}
}
