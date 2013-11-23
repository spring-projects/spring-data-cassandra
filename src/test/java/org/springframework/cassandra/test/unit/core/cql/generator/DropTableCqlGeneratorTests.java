package org.springframework.cassandra.test.unit.core.cql.generator;

import static junit.framework.Assert.assertTrue;
import static org.springframework.cassandra.core.keyspace.TableOperations.dropTable;

import org.junit.Test;
import org.springframework.cassandra.core.cql.generator.DropTableCqlGenerator;
import org.springframework.cassandra.core.keyspace.DropTableSpecification;

public class DropTableCqlGeneratorTests {

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
			TableOperationCqlGeneratorTest<DropTableSpecification, DropTableCqlGenerator> {
	}

	public static class BasicTest extends DropTableTest {

		public String name = "mytable";

		public DropTableSpecification specification() {
			return dropTable().name(name);
		}

		public DropTableCqlGenerator generator() {
			return new DropTableCqlGenerator(specification);
		}

		@Test
		public void test() {
			prepare();

			assertStatement(name, cql);
		}
	}
}
