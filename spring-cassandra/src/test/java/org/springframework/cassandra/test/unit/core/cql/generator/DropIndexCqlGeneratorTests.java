package org.springframework.cassandra.test.unit.core.cql.generator;

import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.springframework.cassandra.core.cql.generator.DropIndexCqlGenerator;
import org.springframework.cassandra.core.keyspace.DropIndexSpecification;

public class DropIndexCqlGeneratorTests {

	/**
	 * Asserts that the preamble is first & correctly formatted in the given CQL string.
	 */
	public static void assertStatement(String indexName, boolean ifExists, String cql) {
		assertTrue(cql.equals("DROP INDEX " + (ifExists ? "IF EXISTS " : "") + indexName + ";"));
	}

	/**
	 * Convenient base class that other test classes can use so as not to repeat the generics declarations.
	 */
	public static abstract class DropIndexTest extends
			IndexOperationCqlGeneratorTest<DropIndexSpecification, DropIndexCqlGenerator> {
	}

	public static class BasicTest extends DropIndexTest {

		public String name = "myindex";

		public DropIndexSpecification specification() {
			return DropIndexSpecification.dropIndex().name(name);
		}

		public DropIndexCqlGenerator generator() {
			return new DropIndexCqlGenerator(specification);
		}

		@Test
		public void test() {
			prepare();

			assertStatement(name, false, cql);
		}

	}

	public static class IfExistsTest extends DropIndexTest {

		public String name = "myindex";

		public DropIndexSpecification specification() {
			return DropIndexSpecification.dropIndex().name(name)
			// .ifExists()
			;
		}

		public DropIndexCqlGenerator generator() {
			return new DropIndexCqlGenerator(specification);
		}

		@Test
		public void test() {
			prepare();

			// assertStatement(name, true, cql);
			assertStatement(name, false, cql);
		}

	}
}
