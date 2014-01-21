package org.springframework.cassandra.test.integration.core.cql.generator;

import static org.springframework.cassandra.test.integration.core.cql.generator.CqlKeyspaceSpecificationAssertions.assertKeyspace;

import org.junit.Test;
import org.springframework.cassandra.test.integration.AbstractEmbeddedCassandraIntegrationTest;
import org.springframework.cassandra.test.unit.core.cql.generator.CreateKeyspaceCqlGeneratorTests.BasicTest;
import org.springframework.cassandra.test.unit.core.cql.generator.CreateKeyspaceCqlGeneratorTests.CreateKeyspaceTest;
import org.springframework.cassandra.test.unit.core.cql.generator.CreateKeyspaceCqlGeneratorTests.NetworkTopologyTest;

/**
 * Integration tests that reuse unit tests.
 * 
 * @author John McPeek
 */
public class CreateKeyspaceCqlGeneratorIntegrationTests {

	/**
	 * Integration test base class that knows how to do everything except instantiate the concrete unit test type T.
	 * 
	 * @param <T> The concrete unit test class to which this integration test corresponds.
	 */
	public static abstract class Base<T extends CreateKeyspaceTest> extends AbstractEmbeddedCassandraIntegrationTest {
		T unit;

		public abstract T unit();

		@Test
		public void test() {
			unit = unit();
			unit.prepare();

			SYSTEM.execute(unit.cql);

			assertKeyspace(unit.specification, unit.keyspace, SYSTEM);
		}
	}

	public static class BasicIntegrationTest extends Base<BasicTest> {

		@Override
		public BasicTest unit() {
			return new BasicTest();
		}
	}

	public static class NetworkTopologyIntegrationTest extends Base<NetworkTopologyTest> {

		@Override
		public NetworkTopologyTest unit() {
			return new NetworkTopologyTest();
		}
	}
}