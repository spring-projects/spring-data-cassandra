package org.springframework.cassandra.test.unit.core.cql.generator;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.springframework.cassandra.config.KeyspaceAttributes;
import org.springframework.cassandra.core.cql.generator.CreateKeyspaceCqlGenerator;
import org.springframework.cassandra.core.keyspace.CreateKeyspaceSpecification;
import org.springframework.cassandra.core.keyspace.DefaultOption;
import org.springframework.cassandra.core.keyspace.KeyspaceOption;
import org.springframework.cassandra.core.keyspace.Option;
import org.springframework.cassandra.test.unit.support.Utils;

public class CreateKeyspaceCqlGeneratorTests {

	/**
	 * Asserts that the preamble is first & correctly formatted in the given CQL string.
	 */
	public static void assertPreamble(String keyspaceName, String cql) {
		assertTrue(cql.startsWith("CREATE KEYSPACE " + keyspaceName + " "));
	}

	private static void assertReplicationMap(Map<Option, Object> replicationMap, String cql) {
		assertTrue(cql.contains(" WITH replication = { "));

		for (Map.Entry<Option, Object> entry : replicationMap.entrySet()) {
			String keyValuePair = "'" + entry.getKey().getName() + "' : " + (entry.getKey().quotesValue() ? "'" : "")
					+ entry.getValue().toString() + (entry.getKey().quotesValue() ? "'" : "");
			assertTrue(cql.contains(keyValuePair));
		}
	}

	public static void assertDurableWrites(Boolean durableWrites, String cql) {
		assertTrue(cql.contains(" AND durable_writes = " + durableWrites));
	}

	/**
	 * Convenient base class that other test classes can use so as not to repeat the generics declarations or
	 * {@link #generator()} method.
	 */
	public static abstract class CreateKeyspaceTest extends
			KeyspaceOperationCqlGeneratorTest<CreateKeyspaceSpecification, CreateKeyspaceCqlGenerator> {

		@Override
		public CreateKeyspaceCqlGenerator generator() {
			return new CreateKeyspaceCqlGenerator(specification);
		}
	}

	public static class BasicTest extends CreateKeyspaceTest {

		public String name = Utils.randomKeyspaceName();
		public Boolean durableWrites = true;

		public Map<Option, Object> replicationMap = KeyspaceAttributes.newSimpleReplication();

		@Override
		public CreateKeyspaceSpecification specification() {
			keyspace = name;

			return CreateKeyspaceSpecification.createKeyspace().name(keyspace)
					.with(KeyspaceOption.REPLICATION, replicationMap).with(KeyspaceOption.DURABLE_WRITES, durableWrites);
		}

		@Test
		public void test() {
			prepare();

			assertPreamble(keyspace, cql);
			assertReplicationMap(replicationMap, cql);
			assertDurableWrites(durableWrites, cql);
		}
	}

	public static class NoOptionsBasicTest extends CreateKeyspaceTest {

		public String name = Utils.randomKeyspaceName();
		public Boolean durableWrites = true;

		public Map<Option, Object> replicationMap = KeyspaceAttributes.newSimpleReplication();

		@Override
		public CreateKeyspaceSpecification specification() {
			keyspace = name;

			return CreateKeyspaceSpecification.createKeyspace().name(keyspace);
		}

		@Test
		public void test() {
			prepare();

			assertPreamble(keyspace, cql);
			assertReplicationMap(replicationMap, cql);
			assertDurableWrites(durableWrites, cql);
		}
	}

	public static class NetworkTopologyTest extends CreateKeyspaceTest {

		public String name = Utils.randomKeyspaceName();
		public Boolean durableWrites = false;

		public Map<Option, Object> replicationMap = new HashMap<Option, Object>();

		@Override
		public CreateKeyspaceSpecification specification() {
			keyspace = name;

			replicationMap.put(new DefaultOption("class", String.class, false, false, true), "NetworkTopologyStrategy");
			replicationMap.put(new DefaultOption("dc1", Long.class, false, false, true), 2);
			replicationMap.put(new DefaultOption("dc2", Long.class, false, false, true), 3);

			return CreateKeyspaceSpecification.createKeyspace().name(keyspace)
					.with(KeyspaceOption.REPLICATION, replicationMap).with(KeyspaceOption.DURABLE_WRITES, durableWrites);
		}

		@Test
		public void test() {
			prepare();

			assertPreamble(keyspace, cql);
			assertReplicationMap(replicationMap, cql);
			assertDurableWrites(durableWrites, cql);
		}
	}
}
