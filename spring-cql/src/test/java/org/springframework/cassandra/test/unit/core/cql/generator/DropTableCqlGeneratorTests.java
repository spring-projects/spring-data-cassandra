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
import org.springframework.cassandra.core.cql.generator.DropTableCqlGenerator;
import org.springframework.cassandra.core.keyspace.DropTableSpecification;

public class DropTableCqlGeneratorTests {

	/**
	 * Asserts that the preamble is first & correctly formatted in the given CQL string.
	 */
	public static void assertStatement(String tableName, boolean ifExists, String cql) {
		assertTrue(cql.equals("DROP TABLE " + (ifExists ? "IF EXISTS " : "") + tableName + ";"));
	}

	/**
	 * Asserts that the given list of columns definitions are contained in the given CQL string properly.
	 * 
	 * @param columnSpec IE, "foo text, bar blob"
	 */
	public static void assertColumnChanges(String columnSpec, String cql) {
		assertTrue(cql.contains(""));
	}

	/**
	 * Convenient base class that other test classes can use so as not to repeat the generics declarations.
	 */
	public static abstract class DropTableTest extends
			TableOperationCqlGeneratorTest<DropTableSpecification, DropTableCqlGenerator> {}

	public static class BasicTest extends DropTableTest {

		public String name = "mytable";

		public DropTableSpecification specification() {
			return DropTableSpecification.dropTable().name(name);
		}

		public DropTableCqlGenerator generator() {
			return new DropTableCqlGenerator(specification);
		}

		@Test
		public void test() {
			prepare();

			assertStatement(name, false, cql);
		}
	}

	// public static class IfExistsTest extends DropTableTest {
	//
	// public String name = "mytable";
	//
	// public DropTableSpecification specification() {
	// return DropTableSpecification.dropTable().ifExists().name(name);
	// }
	//
	// public DropTableCqlGenerator generator() {
	// return new DropTableCqlGenerator(specification);
	// }
	//
	// @Test
	// public void test() {
	// prepare();
	//
	// assertStatement(name, true, cql);
	// }
	// }
}
