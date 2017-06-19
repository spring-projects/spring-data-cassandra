/*
 * Copyright 2017 the original author or authors.
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

import org.junit.Test;
import org.springframework.data.cassandra.core.cql.generator.CreateTableCqlGeneratorUnitTests.BasicTest;
import org.springframework.data.cassandra.core.cql.generator.CreateTableCqlGeneratorUnitTests.CompositePartitionKeyTest;
import org.springframework.data.cassandra.core.cql.generator.CreateTableCqlGeneratorUnitTests.CreateTableTest;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTest;

/**
 * Integration tests that reuse unit tests.
 *
 * @author Matthew T. Adams
 * @author Oliver Gierke
 */
public class CreateTableCqlGeneratorIntegrationTests {

	/**
	 * Integration test base class that knows how to do everything except instantiate the concrete unit test type T.
	 *
	 * @author Matthew T. Adams
	 * @param <T> The concrete unit test class to which this integration test corresponds.
	 */
	public static abstract class Base<T extends CreateTableTest> extends AbstractKeyspaceCreatingIntegrationTest {

		T unit;

		public abstract T unit();

		@Test
		public void test() {

			unit = unit();
			unit.prepare();

			session.execute(unit.cql);

			CqlTableSpecificationAssertions.assertTable(unit.specification, keyspace, session);
		}
	}

	public static class BasicIntegrationTest extends Base<BasicTest> {

		@Override
		public BasicTest unit() {
			return new BasicTest();
		}
	}

	public static class CompositePartitionKeyIntegrationTest extends Base<CompositePartitionKeyTest> {

		@Override
		public CompositePartitionKeyTest unit() {
			return new CompositePartitionKeyTest();
		}
	}
}
