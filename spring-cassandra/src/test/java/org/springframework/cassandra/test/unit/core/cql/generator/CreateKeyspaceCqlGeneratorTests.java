package org.springframework.cassandra.test.unit.core.cql.generator;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.springframework.cassandra.core.cql.generator.CreateKeyspaceCqlGenerator;
import org.springframework.cassandra.core.keyspace.CreateKeyspaceSpecification;
import org.springframework.cassandra.core.keyspace.DefaultOption;
import org.springframework.cassandra.core.keyspace.KeyspaceOption;
import org.springframework.cassandra.core.keyspace.Option;

public class CreateKeyspaceCqlGeneratorTests {

	/**
	 * Asserts that the preamble is first & correctly formatted in the given CQL string.
	 */
	public static void assertPreamble(String keyspaceName, String cql) {
		assertTrue(cql.startsWith("CREATE KEYSPACE " + keyspaceName + " "));
	}

	/**
	 * Convenient base class that other test classes can use so as not to repeat the generics declarations or
	 * {@link #generator()} method.
	 */
	public static abstract class CreateKeyspaceTest extends
			KeyspaceOperationCqlGeneratorTest<CreateKeyspaceSpecification, CreateKeyspaceCqlGenerator> {

		public CreateKeyspaceCqlGenerator generator() {
			return new CreateKeyspaceCqlGenerator(specification);
		}
	}

	public static class BasicTest extends CreateKeyspaceTest {

		public String name = "mytable";
		public Boolean durableWrites = true;
		
		public Map<Option, Object> replicationMap = new HashMap<Option, Object>();

		@Override
		public CreateKeyspaceSpecification specification() {
			replicationMap.put( new DefaultOption( "class", String.class, false, false, true ), "SimpleStrategy" );
			replicationMap.put( new DefaultOption( "replication_factor", Long.class, false, false, true ), 1 );
			replicationMap.put( new DefaultOption( "dc1", Long.class, false, false, true ), 2 );
			replicationMap.put( new DefaultOption( "dc2", Long.class, false, false, true ), 3 );
			
			return (CreateKeyspaceSpecification) CreateKeyspaceSpecification.createKeyspace()
						.name(name)
						.with(KeyspaceOption.REPLICATION, replicationMap)
						.with(KeyspaceOption.DURABLE_WRITES, durableWrites);
		}

		@Test
		public void test() {
			prepare();

			assertPreamble(name, cql);
		}
	}
}
