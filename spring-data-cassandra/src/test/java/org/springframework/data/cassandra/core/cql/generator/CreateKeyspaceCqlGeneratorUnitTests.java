/*
 * Copyright 2016-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core.cql.generator;

import static org.assertj.core.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.data.cassandra.core.cql.keyspace.CreateKeyspaceSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.DefaultOption;
import org.springframework.data.cassandra.core.cql.keyspace.KeyspaceAttributes;
import org.springframework.data.cassandra.core.cql.keyspace.KeyspaceOption;
import org.springframework.data.cassandra.core.cql.keyspace.Option;
import org.springframework.data.cassandra.support.RandomKeyspaceName;

/**
 * Unit tests for {@link CreateKeyspaceCqlGenerator}.
 *
 * @author John McPeek
 * @author Matthew T. Adams
 */
public class CreateKeyspaceCqlGeneratorUnitTests {

	/**
	 * Asserts that the preamble is first & correctly formatted in the given CQL string.
	 */
	private static void assertPreamble(String keyspaceName, String cql) {
		assertThat(cql).startsWith("CREATE KEYSPACE " + keyspaceName + " ");
	}

	private static void assertReplicationMap(Map<Option, Object> replicationMap, String cql) {
		assertThat(cql).contains(" WITH replication = { ");

		for (Map.Entry<Option, Object> entry : replicationMap.entrySet()) {
			String keyValuePair = "'" + entry.getKey().getName() + "' : " + (entry.getKey().quotesValue() ? "'" : "")
					+ entry.getValue().toString() + (entry.getKey().quotesValue() ? "'" : "");
			assertThat(cql).contains(keyValuePair);
		}
	}

	private static void assertDurableWrites(Boolean durableWrites, String cql) {
		assertThat(cql).contains(" AND durable_writes = " + durableWrites);
	}

	/**
	 * Convenient base class that other test classes can use so as not to repeat the generics declarations or
	 * {@link #generator()} method.
	 */
	public static abstract class CreateKeyspaceTest
			extends AbstractKeyspaceOperationCqlGeneratorTest<CreateKeyspaceSpecification, CreateKeyspaceCqlGenerator> {

		@Override
		public CreateKeyspaceCqlGenerator generator() {
			return new CreateKeyspaceCqlGenerator(specification);
		}
	}

	static class BasicTest extends CreateKeyspaceTest {

		private String name = RandomKeyspaceName.create();
		private Boolean durableWrites = true;

		private Map<Option, Object> replicationMap = KeyspaceAttributes.newSimpleReplication();

		@Override
		public CreateKeyspaceSpecification specification() {
			keyspace = name;

			return CreateKeyspaceSpecification.createKeyspace(keyspace)
					.with(KeyspaceOption.REPLICATION, replicationMap).with(KeyspaceOption.DURABLE_WRITES, durableWrites);
		}

		@Test
		void test() {
			prepare();

			assertPreamble(keyspace, cql);
			assertReplicationMap(replicationMap, cql);
			assertDurableWrites(durableWrites, cql);
		}
	}

	static class NoOptionsBasicTest extends CreateKeyspaceTest {

		private String name = RandomKeyspaceName.create();
		private Boolean durableWrites = true;

		private Map<Option, Object> replicationMap = KeyspaceAttributes.newSimpleReplication();

		@Override
		public CreateKeyspaceSpecification specification() {
			keyspace = name;

			return CreateKeyspaceSpecification.createKeyspace(keyspace);
		}

		@Test
		void test() {
			prepare();

			assertPreamble(keyspace, cql);
			assertReplicationMap(replicationMap, cql);
			assertDurableWrites(durableWrites, cql);
		}
	}

	static class NetworkTopologyTest extends CreateKeyspaceTest {

		private String name = RandomKeyspaceName.create();
		private Boolean durableWrites = false;

		private Map<Option, Object> replicationMap = new HashMap<>();

		@Override
		public CreateKeyspaceSpecification specification() {
			keyspace = name;

			replicationMap.put(new DefaultOption("class", String.class, false, false, true), "NetworkTopologyStrategy");
			replicationMap.put(new DefaultOption("dc1", Long.class, false, false, true), 2);
			replicationMap.put(new DefaultOption("dc2", Long.class, false, false, true), 3);

			return CreateKeyspaceSpecification.createKeyspace(keyspace)
					.with(KeyspaceOption.REPLICATION, replicationMap).with(KeyspaceOption.DURABLE_WRITES, durableWrites);
		}

		@Test
		void test() {
			prepare();

			assertPreamble(keyspace, cql);
			assertReplicationMap(replicationMap, cql);
			assertDurableWrites(durableWrites, cql);
		}
	}
}
