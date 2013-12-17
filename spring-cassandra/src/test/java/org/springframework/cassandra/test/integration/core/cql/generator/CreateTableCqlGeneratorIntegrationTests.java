package org.springframework.cassandra.test.integration.core.cql.generator;

import static org.springframework.cassandra.test.integration.core.cql.generator.CqlTableSpecificationAssertions.assertTable;

import org.junit.Test;
import org.springframework.cassandra.test.integration.AbstractEmbeddedCassandraIntegrationTest;
import org.springframework.cassandra.test.unit.core.cql.generator.CreateTableCqlGeneratorTests;
import org.springframework.cassandra.test.unit.core.cql.generator.CreateTableCqlGeneratorTests.BasicTest;
import org.springframework.cassandra.test.unit.core.cql.generator.CreateTableCqlGeneratorTests.CompositePartitionKeyTest;
import org.springframework.cassandra.test.unit.core.cql.generator.CreateTableCqlGeneratorTests.CreateTableTest;

/**
 * Integration tests that reuse unit tests.
 * 
 * @author Matthew T. Adams
 */
public class CreateTableCqlGeneratorIntegrationTests {

	/**
	 * Integration test base class that knows how to do everything except instantiate the concrete unit test type T.
	 * 
	 * @author Matthew T. Adams
	 * 
	 * @param <T> The concrete unit test class to which this integration test corresponds.
	 */
	public static abstract class Base<T extends CreateTableTest> extends AbstractEmbeddedCassandraIntegrationTest {
		T unit;

		public abstract T unit();

		@Test
		public void test() {
			unit = unit();
			unit.prepare();

			session.execute(unit.cql);

			assertTable(unit.specification, keyspace, session);
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

	public static class TableOptionsIntegrationTest extends AbstractEmbeddedCassandraIntegrationTest {

		@Test
		public void test() {

			CreateTableCqlGeneratorTests.MultipleOptionsTest optionsTest = new CreateTableCqlGeneratorTests.MultipleOptionsTest();

			optionsTest.prepare();

			session.execute(optionsTest.cql);

			// assertTable(optionsTest.specification, keyspace, session);
		}
	}
}