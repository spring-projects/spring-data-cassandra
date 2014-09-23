/*
 * Copyright 2013-2014 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.cassandra.test.unit.core.cql.generator;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.springframework.cassandra.core.cql.generator.AlterKeyspaceCqlGenerator;
import org.springframework.cassandra.core.keyspace.AlterKeyspaceSpecification;
import org.springframework.cassandra.core.keyspace.DefaultOption;
import org.springframework.cassandra.core.keyspace.KeyspaceOption;
import org.springframework.cassandra.core.keyspace.Option;
import org.springframework.cassandra.test.unit.support.Utils;

public class AlterKeyspaceCqlGeneratorTests {

	/**
	 * Asserts that the preamble is first & correctly formatted in the given CQL string.
	 */
	public static void assertPreamble(String tableName, String cql) {
		assertTrue(cql.startsWith("ALTER KEYSPACE " + tableName + " "));
	}

	private static void assertReplicationMap(Map<Option, Object> replicationMap, String cql) {
		assertTrue(cql.contains(" WITH replication = { "));

		for (Map.Entry<Option, Object> entry : replicationMap.entrySet()) {
			String keyValuePair = "'" + entry.getKey().getName() + "' : '" + entry.getValue().toString() + "'";
			assertTrue(cql.contains(keyValuePair));
		}
	}

	public static void assertDurableWrites(Boolean durableWrites, String cql) {
		assertTrue(cql.contains(" AND durable_writes = " + durableWrites));
	}

	/**
	 * Convenient base class that other test classes can use so as not to repeat the generics declarations.
	 */
	public static abstract class AlterKeyspaceTest extends
			KeyspaceOperationCqlGeneratorTest<AlterKeyspaceSpecification, AlterKeyspaceCqlGenerator> {}

	public static class CompleteTest extends AlterKeyspaceTest {

		public String name = Utils.randomKeyspaceName();
		public Boolean durableWrites = true;

		public Map<Option, Object> replicationMap = new HashMap<Option, Object>();

		@Override
		public AlterKeyspaceSpecification specification() {
			replicationMap.put(new DefaultOption("class", String.class, false, false, true), "SimpleStrategy");
			replicationMap.put(new DefaultOption("replication_factor", Long.class, false, false, true), 1);
			replicationMap.put(new DefaultOption("dc1", Long.class, false, false, true), 2);
			replicationMap.put(new DefaultOption("dc2", Long.class, false, false, true), 3);

			return AlterKeyspaceSpecification.alterKeyspace().name(name).with(KeyspaceOption.REPLICATION, replicationMap)
					.with(KeyspaceOption.DURABLE_WRITES, durableWrites);
		}

		@Override
		public AlterKeyspaceCqlGenerator generator() {
			return new AlterKeyspaceCqlGenerator(specification);
		}

		@Test
		public void test() {
			prepare();

			assertPreamble(name, cql);
			assertReplicationMap(replicationMap, cql);
			assertDurableWrites(durableWrites, cql);
		}
	}

	public static class ReplicationMapOnlyTest extends AlterKeyspaceTest {

		public String name = "mytable";
		public Boolean durableWrites = true;

		public Map<Option, Object> replicationMap = new HashMap<Option, Object>();

		@Override
		public AlterKeyspaceSpecification specification() {
			replicationMap.put(new DefaultOption("class", String.class, false, false, true), "SimpleStrategy");
			replicationMap.put(new DefaultOption("replication_factor", Long.class, false, false, true), 1);
			replicationMap.put(new DefaultOption("dc1", Long.class, false, false, true), 2);
			replicationMap.put(new DefaultOption("dc2", Long.class, false, false, true), 3);

			return AlterKeyspaceSpecification.alterKeyspace().name(name).with(KeyspaceOption.REPLICATION, replicationMap);
		}

		@Override
		public AlterKeyspaceCqlGenerator generator() {
			return new AlterKeyspaceCqlGenerator(specification);
		}

		@Test
		public void test() {
			prepare();

			assertPreamble(name, cql);
			assertReplicationMap(replicationMap, cql);
		}
	}
}
