/*
 * Copyright 2016-2021 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.springframework.data.cassandra.core.cql.keyspace.DropKeyspaceSpecification;
import org.springframework.data.cassandra.support.RandomKeyspaceName;

/**
 * Unit tests for {@link DropKeyspaceCqlGenerator}.
 *
 * @author John McPeek
 * @author Matthew T. Adams
 * @author David Webb
 */
class DropKeyspaceCqlGeneratorUnitTests {

	/**
	 * Asserts that the preamble is first & correctly formatted in the given CQL string.
	 */
	private static void assertStatement(String tableName, String cql) {
		assertThat(cql).isEqualTo("DROP KEYSPACE " + tableName + ";");
	}

	/**
	 * Convenient base class that other test classes can use so as not to repeat the generics declarations.
	 */
	static abstract class DropTableTest
			extends AbstractKeyspaceOperationCqlGeneratorTest<DropKeyspaceSpecification, DropKeyspaceCqlGenerator> {}

	static class BasicTest extends DropTableTest {

		private String name = RandomKeyspaceName.create();

		@Override
		public DropKeyspaceSpecification specification() {
			return DropKeyspaceSpecification.dropKeyspace(name);
		}

		@Override
		public DropKeyspaceCqlGenerator generator() {
			return new DropKeyspaceCqlGenerator(specification);
		}

		@Test
		void test() {
			prepare();

			assertStatement(name, cql);
		}
	}
}
