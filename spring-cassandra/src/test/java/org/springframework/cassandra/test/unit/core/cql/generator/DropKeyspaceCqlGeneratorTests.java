package org.springframework.cassandra.test.unit.core.cql.generator;

import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.springframework.cassandra.core.cql.generator.DropKeyspaceCqlGenerator;
import org.springframework.cassandra.core.cql.generator.DropTableCqlGenerator;
import org.springframework.cassandra.core.keyspace.DropKeyspaceSpecification;
import org.springframework.cassandra.core.keyspace.DropTableSpecification;

public class DropKeyspaceCqlGeneratorTests {

	/**
	 * Asserts that the preamble is first & correctly formatted in the given CQL string.
	 */
	public static void assertStatement(String tableName, String cql) {
		assertTrue(cql.equals("DROP TABLE " + tableName + ";"));
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
			KeyspaceOperationCqlGeneratorTest<DropKeyspaceSpecification, DropKeyspaceCqlGenerator> {
	}

	public static class BasicTest extends DropTableTest {

		public String name = "mytable";

		public DropKeyspaceSpecification specification() {
			return DropKeyspaceSpecification.dropTable().name(name);
		}

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
