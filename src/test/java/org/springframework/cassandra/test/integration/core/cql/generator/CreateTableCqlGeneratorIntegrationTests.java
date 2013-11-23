package org.springframework.cassandra.test.integration.core.cql.generator;

import static org.springframework.cassandra.test.integration.core.cql.generator.CqlTableSpecificationAssertions.assertTable;

import org.junit.Test;
import org.springframework.cassandra.test.unit.core.cql.generator.CreateTableCqlGeneratorTests.BasicTest;

public class CreateTableCqlGeneratorIntegrationTests {

	public static class BasicIntegrationTest extends AbstractEmbeddedCassandraIntegrationTest {
		BasicTest unit = new BasicTest();

		@Test
		public void test() {
			unit.prepare();

			session.execute(unit.cql);

			assertTable(unit.specification, keyspace, session);
		}
	}
}