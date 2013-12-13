package org.springframework.cassandra.test.integration.core.cql.generator;

import static org.springframework.cassandra.test.integration.core.cql.generator.CqlIndexSpecificationAssertions.assertIndex;

import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.cassandra.test.integration.AbstractEmbeddedCassandraIntegrationTest;
import org.springframework.cassandra.test.unit.core.cql.generator.CreateIndexCqlGeneratorTests.BasicTest;
import org.springframework.cassandra.test.unit.core.cql.generator.CreateIndexCqlGeneratorTests.CreateIndexTest;

/**
 * Integration tests that reuse unit tests.
 * 
 * @author Matthew T. Adams
 */
public class CreateIndexCqlGeneratorIntegrationTests {

	/**
	 * Integration test base class that knows how to do everything except instantiate the concrete unit test type T.
	 * 
	 * @author Matthew T. Adams
	 * 
	 * @param <T> The concrete unit test class to which this integration test corresponds.
	 */
	public static abstract class Base<T extends CreateIndexTest> extends AbstractEmbeddedCassandraIntegrationTest {
		T unit;

		public abstract T unit();

		@Test
		public void test() {
			unit = unit();
			unit.prepare();

			session.execute(unit.cql);

			assertIndex(unit.specification, keyspace, session);
		}
	}

	public static class BasicIntegrationTest extends Base<BasicTest> {

		/**
		 * This loads any test specific Cassandra objects
		 */
		@Rule
		public CassandraCQLUnit cassandraCQLUnit = new CassandraCQLUnit(new ClassPathCQLDataSet(
				"integration/cql/generator/CreateIndexCqlGeneratorIntegrationTests-BasicTest.cql", this.keyspace),
				CASSANDRA_CONFIG, CASSANDRA_HOST, CASSANDRA_NATIVE_PORT);

		@Override
		public BasicTest unit() {
			return new BasicTest();
		}

	}

}