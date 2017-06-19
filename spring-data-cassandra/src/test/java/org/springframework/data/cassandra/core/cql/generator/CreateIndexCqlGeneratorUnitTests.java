/*
 * Copyright 2016-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core.cql.generator;

import static org.assertj.core.api.Assertions.*;

import org.junit.Test;
import org.springframework.data.cassandra.core.cql.keyspace.CreateIndexSpecification;

/**
 * Unit tests for {@link CreateIndexCqlGenerator}.
 *
 * @author Matthew T. Adams
 * @author David Webb
 */
public class CreateIndexCqlGeneratorUnitTests {

	/**
	 * Asserts that the preamble is first & correctly formatted in the given CQL string.
	 */
	public static void assertPreamble(String indexName, String tableName, String cql) {
		assertThat(cql.startsWith("CREATE INDEX " + indexName + " ON " + tableName)).isTrue();
	}

	/**
	 * Asserts that the given list of columns definitions are contained in the given CQL string properly.
	 *
	 * @param columnName IE, "(foo)"
	 */
	public static void assertColumn(String columnName, String cql) {
		assertThat(cql.contains("(" + columnName + ")")).isTrue();
	}

	/**
	 * Convenient base class that other test classes can use so as not to repeat the generics declarations or
	 * {@link #generator()} method.
	 */
	public static abstract class CreateIndexTest
			extends AbstractIndexOperationCqlGeneratorTest<CreateIndexSpecification, CreateIndexCqlGenerator> {

		public CreateIndexCqlGenerator generator() {
			return new CreateIndexCqlGenerator(specification);
		}
	}

	public static class BasicTest extends CreateIndexTest {

		public String name = "myindex";
		public String tableName = "mytable";
		public String column1 = "column1";

		public CreateIndexSpecification specification() {
			return CreateIndexSpecification.createIndex().name(name).tableName(tableName).columnName(column1);
		}

		@Test
		public void test() {
			prepare();

			assertPreamble(name, tableName, cql);
			assertColumn(column1, cql);
		}
	}

}
