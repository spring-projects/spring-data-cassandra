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
