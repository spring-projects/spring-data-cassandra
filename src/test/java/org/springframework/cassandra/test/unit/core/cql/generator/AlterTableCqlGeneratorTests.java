package org.springframework.cassandra.test.unit.core.cql.generator;

import static junit.framework.Assert.assertTrue;
import static org.springframework.cassandra.core.keyspace.TableOperations.alterTable;

import org.junit.Test;
import org.springframework.cassandra.core.cql.generator.AlterTableCqlGenerator;
import org.springframework.cassandra.core.keyspace.AlterTableSpecification;

import com.datastax.driver.core.DataType;

public class AlterTableCqlGeneratorTests {

	/**
	 * Asserts that the preamble is first & correctly formatted in the given CQL string.
	 */
	public static void assertPreamble(String tableName, String cql) {
		assertTrue(cql.startsWith("ALTER TABLE " + tableName + " "));
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
	public static abstract class AlterTableTest extends
			TableOperationCqlGeneratorTest<AlterTableSpecification, AlterTableCqlGenerator> {
	}

	public static class BasicTest extends AlterTableTest {

		public String name = "mytable";
		public DataType alteredType = DataType.text();
		public String altered = "altered";

		public DataType addedType = DataType.text();
		public String added = "added";

		public String dropped = "dropped";

		public AlterTableSpecification specification() {
			return alterTable().name(name).alter(altered, alteredType).add(added, addedType).drop(dropped);
		}

		public AlterTableCqlGenerator generator() {
			return new AlterTableCqlGenerator(specification);
		}

		@Test
		public void test() {
			prepare();

			assertPreamble(name, cql);
			assertColumnChanges(
					String.format("ALTER %s TYPE %s, ADD %s %s, DROP %s", altered, alteredType, added, addedType, dropped), cql);
		}
	}
}
