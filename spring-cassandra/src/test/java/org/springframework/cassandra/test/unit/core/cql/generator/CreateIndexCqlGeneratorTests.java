package org.springframework.cassandra.test.unit.core.cql.generator;

import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.springframework.cassandra.core.cql.generator.CreateIndexCqlGenerator;
import org.springframework.cassandra.core.keyspace.CreateIndexSpecification;

public class CreateIndexCqlGeneratorTests {

	/**
	 * Asserts that the preamble is first & correctly formatted in the given CQL string.
	 */
	public static void assertPreamble(String indexName, String tableName, String cql) {
		assertTrue(cql.startsWith("CREATE INDEX " + indexName + " ON " + tableName));
	}

	/**
	 * Asserts that the given list of columns definitions are contained in the given CQL string properly.
	 * 
	 * @param columnSpec IE, "(foo)"
	 */
	public static void assertColumn(String columnName, String cql) {
		assertTrue(cql.contains("(" + columnName + ")"));
	}

	/**
	 * Convenient base class that other test classes can use so as not to repeat the generics declarations or
	 * {@link #generator()} method.
	 */
	public static abstract class CreateIndexTest extends
			IndexOperationCqlGeneratorTest<CreateIndexSpecification, CreateIndexCqlGenerator> {

		public CreateIndexCqlGenerator generator() {
			return new CreateIndexCqlGenerator(specification);
		}
	}

	public static class BasicTest extends CreateIndexTest {

		public String name = "myindex";
		public String tableName = "mytable";
		public String column1 = "column1";

		public CreateIndexSpecification specification() {
			return CreateIndexSpecification.createIndex().name(name).tableName(tableName).columnName(column1);
		}

		@Test
		public void test() {
			prepare();

			assertPreamble(name, tableName, cql);
			assertColumn(column1, cql);

		}
	}

}
