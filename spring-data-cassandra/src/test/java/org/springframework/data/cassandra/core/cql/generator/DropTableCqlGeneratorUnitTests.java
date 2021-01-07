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
import org.springframework.data.cassandra.core.cql.keyspace.DropTableSpecification;

/**
 * Unit tests for {@link DropTableCqlGenerator}.
 *
 * @author Matthew T. Adams
 * @author David Webb
 */
class DropTableCqlGeneratorUnitTests {

	/**
	 * Asserts that the preamble is first & correctly formatted in the given CQL string.
	 */
	private static void assertStatement(String tableName, boolean ifExists, String cql) {
		assertThat(cql).isEqualTo("DROP TABLE " + (ifExists ? "IF EXISTS " : "") + tableName + ";");
	}

	/**
	 * Convenient base class that other test classes can use so as not to repeat the generics declarations.
	 */
	static abstract class DropTableTest
			extends AbstractTableOperationCqlGeneratorTest<DropTableSpecification, DropTableCqlGenerator> {}

	static class BasicTest extends DropTableTest {

		private String name = "mytable";

		public DropTableSpecification specification() {
			return DropTableSpecification.dropTable(name);
		}

		public DropTableCqlGenerator generator() {
			return new DropTableCqlGenerator(specification);
		}

		@Test
		void test() {
			prepare();

			assertStatement(name, false, cql);
		}
	}
}
