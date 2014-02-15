/*
 * Copyright 2013-2014 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.cassandra.test.unit.core.cql.generator;

import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.springframework.cassandra.core.cql.generator.DropKeyspaceCqlGenerator;
import org.springframework.cassandra.core.keyspace.DropKeyspaceSpecification;
import org.springframework.cassandra.test.unit.support.Utils;

public class DropKeyspaceCqlGeneratorTests {

	/**
	 * Asserts that the preamble is first & correctly formatted in the given CQL string.
	 */
	public static void assertStatement(String tableName, String cql) {
		assertTrue(cql.equals("DROP KEYSPACE " + tableName + ";"));
	}

	/**
	 * Convenient base class that other test classes can use so as not to repeat the generics declarations.
	 */
	public static abstract class DropTableTest extends
			KeyspaceOperationCqlGeneratorTest<DropKeyspaceSpecification, DropKeyspaceCqlGenerator> {
	}

	public static class BasicTest extends DropTableTest {

		public String name = Utils.randomKeyspaceName();

		@Override
		public DropKeyspaceSpecification specification() {
			return DropKeyspaceSpecification.dropKeyspace().name(name);
		}

		@Override
		public DropKeyspaceCqlGenerator generator() {
			return new DropKeyspaceCqlGenerator(specification);
		}

		@Test
		public void test() {
			prepare();

			assertStatement(name, cql);
		}
	}
}
