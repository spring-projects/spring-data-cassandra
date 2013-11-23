package org.springframework.cassandra.test.unit.core.cql.generator;

import static junit.framework.Assert.assertTrue;
import static org.springframework.cassandra.core.keyspace.TableOperations.createTable;

import org.junit.Test;
import org.springframework.cassandra.core.cql.generator.CreateTableCqlGenerator;
import org.springframework.cassandra.core.keyspace.CreateTableSpecification;

import com.datastax.driver.core.DataType;

public class CreateTableCqlGeneratorTests {

	/**
	 * Asserts that the preamble is first & correctly formatted in the given CQL string.
	 */
	public static void assertPreamble(String tableName, String cql) {
		assertTrue(cql.startsWith("CREATE TABLE " + tableName + " "));
	}

	/**
	 * Asserts that the given primary key definition is contained in the given CQL string properly.
	 * 
	 * @param primaryKeyString IE, "foo", "foo, bar, baz", "(foo, bar), baz", etc
	 */
	public static void assertPrimaryKey(String primaryKeyString, String cql) {
		assertTrue(cql.contains(", PRIMARY KEY (" + primaryKeyString + "))"));
	}

	/**
	 * Asserts that the given list of columns definitions are contained in the given CQL string properly.
	 * 
	 * @param columnSpec IE, "foo text, bar blob"
	 */
	public static void assertColumns(String columnSpec, String cql) {
		assertTrue(cql.contains("(" + columnSpec + ","));
	}

	/**
	 * Convenient base class that other test classes can use so as not to repeat the generics declarations.
	 */
	public static abstract class CreateTableTest extends
			TableOperationCqlGeneratorTest<CreateTableSpecification, CreateTableCqlGenerator> {
	}

	public static class BasicTest extends CreateTableTest {

		public String name = "mytable";
		public DataType partitionKeyType0 = DataType.text();
		public String partitionKey0 = "partitionKey0";
		public DataType columnType1 = DataType.text();
		public String column1 = "column1";

		public CreateTableSpecification specification() {
			return createTable().name(name).partitionKeyColumn(partitionKey0, partitionKeyType0).column(column1, columnType1);
		}

		public CreateTableCqlGenerator generator() {
			return new CreateTableCqlGenerator(specification);
		}

		@Test
		public void test() {
			prepare();

			assertPreamble(name, cql);
			assertColumns(partitionKey0 + " " + partitionKeyType0 + ", " + column1 + " " + columnType1, cql);
			assertPrimaryKey(partitionKey0, cql);
		}
	}
}
