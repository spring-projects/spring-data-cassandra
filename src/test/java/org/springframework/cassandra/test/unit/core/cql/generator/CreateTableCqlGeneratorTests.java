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
	 * Convenient base class that other test classes can use so as not to repeat the generics declarations or
	 * {@link #generator()} method.
	 */
	public static abstract class CreateTableTest extends
			TableOperationCqlGeneratorTest<CreateTableSpecification, CreateTableCqlGenerator> {

		public CreateTableCqlGenerator generator() {
			return new CreateTableCqlGenerator(specification);
		}
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

		@Test
		public void test() {
			prepare();

			assertPreamble(name, cql);
			assertColumns(String.format("%s %s, %s %s", partitionKey0, partitionKeyType0, column1, columnType1), cql);
			assertPrimaryKey(partitionKey0, cql);
		}
	}

	public static class CompositePartitionKeyTest extends CreateTableTest {

		public String name = "composite_partition_key_table";
		public DataType partKeyType0 = DataType.text();
		public String partKey0 = "partKey0";
		public DataType partKeyType1 = DataType.text();
		public String partKey1 = "partKey1";
		public String column0 = "column0";
		public DataType columnType0 = DataType.text();

		@Override
		public CreateTableSpecification specification() {
			return createTable().name(name).partitionKeyColumn(partKey0, partKeyType0)
					.partitionKeyColumn(partKey1, partKeyType1).column(column0, columnType0);
		}

		@Test
		public void test() {
			prepare();

			assertPreamble(name, cql);
			assertColumns(
					String.format("%s %s, %s %s, %s %s", partKey0, partKeyType0, partKey1, partKeyType1, column0, columnType0),
					cql);
			assertPrimaryKey(String.format("(%s, %s)", partKey0, partKey1), cql);
		}
	}
}
