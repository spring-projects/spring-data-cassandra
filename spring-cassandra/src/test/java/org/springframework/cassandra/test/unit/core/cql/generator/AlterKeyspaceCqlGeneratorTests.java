package org.springframework.cassandra.test.unit.core.cql.generator;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.springframework.cassandra.core.cql.generator.AlterKeyspaceCqlGenerator;
import org.springframework.cassandra.core.cql.generator.AlterTableCqlGenerator;
import org.springframework.cassandra.core.keyspace.AlterKeyspaceSpecification;
import org.springframework.cassandra.core.keyspace.AlterTableSpecification;
import org.springframework.cassandra.core.keyspace.DefaultOption;
import org.springframework.cassandra.core.keyspace.KeyspaceOption;
import org.springframework.cassandra.core.keyspace.KeyspaceSpecification;
import org.springframework.cassandra.core.keyspace.Option;

import com.datastax.driver.core.DataType;

public class AlterKeyspaceCqlGeneratorTests {

	/**
	 * Asserts that the preamble is first & correctly formatted in the given CQL string.
	 */
	public static void assertPreamble(String tableName, String cql) {
		assertTrue(cql.startsWith("ALTER KEYSPACE " + tableName + " "));
	}

	/**
	 * Convenient base class that other test classes can use so as not to repeat the generics declarations.
	 */
	public static abstract class AlterTableTest extends
	KeyspaceOperationCqlGeneratorTest<AlterKeyspaceSpecification, AlterKeyspaceCqlGenerator> {
	}

	public static class BasicTest extends AlterTableTest {

		public String name = "mytable";
		public Boolean durableWrites = true;
		
		public Map<Option, Object> replicationMap = new HashMap<Option, Object>();

		public AlterKeyspaceSpecification specification() {
			replicationMap.put( new DefaultOption( "class", String.class, false, false, true ), "SimpleStrategy" );
			replicationMap.put( new DefaultOption( "replication_factor", Long.class, false, false, true ), 1 );
			replicationMap.put( new DefaultOption( "dc1", Long.class, false, false, true ), 2 );
			replicationMap.put( new DefaultOption( "dc2", Long.class, false, false, true ), 3 );
			
			return AlterKeyspaceSpecification.alterKeyspace()
					.name(name)
					.with(KeyspaceOption.REPLICATION, replicationMap)
					.with(KeyspaceOption.DURABLE_WRITES, durableWrites);
		}

		public AlterKeyspaceCqlGenerator generator() {
			return new AlterKeyspaceCqlGenerator(specification);
		}

		@Test
		public void test() {
			prepare();

			assertPreamble(name, cql);
		}
	}
}
