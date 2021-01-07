/*
 * Copyright 2017-2021 the original author or authors.
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
package org.springframework.data.cassandra.config;

import static org.assertj.core.api.Assertions.*;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import org.springframework.data.cassandra.core.cql.keyspace.AlterKeyspaceSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.CreateKeyspaceSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.DropKeyspaceSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.KeyspaceActionSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.KeyspaceOption.ReplicationStrategy;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * Unit tests for {@link KeyspaceActionSpecificationFactoryBean}.
 *
 * @author Mark Paluch
 */
class KeyspaceActionSpecificationFactoryBeanUnitTests {

	private KeyspaceActionSpecificationFactoryBean bean = new KeyspaceActionSpecificationFactoryBean();

	@Test // DATACASS-502
	void shouldCreateKeyspace() {

		bean.setAction(KeyspaceAction.CREATE);
		bean.setName("my_keyspace");
		bean.setReplicationStrategy(ReplicationStrategy.SIMPLE_STRATEGY);
		bean.setReplicationFactor(1);

		bean.afterPropertiesSet();

		List<KeyspaceActionSpecification> actions = bean.getObject().getActions();

		assertThat(actions).hasSize(1).hasAtLeastOneElementOfType(CreateKeyspaceSpecification.class);

		CreateKeyspaceSpecification create = (CreateKeyspaceSpecification) actions.get(0);

		assertThat(create.getName()).isEqualTo(CqlIdentifier.fromCql("my_keyspace"));
		assertThat(create.getOptions()).containsKeys("durable_writes", "replication");
	}

	@Test // DATACASS-502
	void shouldCreateAndDropKeyspace() {

		bean.setAction(KeyspaceAction.CREATE_DROP);
		bean.setName("my_keyspace");

		bean.afterPropertiesSet();

		List<KeyspaceActionSpecification> actions = bean.getObject().getActions();

		assertThat(actions).hasSize(2).hasAtLeastOneElementOfType(CreateKeyspaceSpecification.class)
				.hasAtLeastOneElementOfType(DropKeyspaceSpecification.class);

		DropKeyspaceSpecification drop = (DropKeyspaceSpecification) actions.get(1);

		assertThat(drop.getName()).isEqualTo(CqlIdentifier.fromCql("my_keyspace"));
	}

	@Test // DATACASS-502
	void shouldAlterKeyspace() {

		bean.setAction(KeyspaceAction.ALTER);
		bean.setDurableWrites(true);
		bean.setName("my_keyspace");
		bean.setNetworkTopologyDataCenters(Arrays.asList("foo", "bar"));
		bean.setNetworkTopologyReplicationFactors(Arrays.asList("1", "2"));
		bean.setReplicationStrategy(ReplicationStrategy.NETWORK_TOPOLOGY_STRATEGY);

		bean.afterPropertiesSet();

		List<KeyspaceActionSpecification> actions = bean.getObject().getActions();

		assertThat(actions).hasSize(1).hasAtLeastOneElementOfType(AlterKeyspaceSpecification.class);

		AlterKeyspaceSpecification alter = (AlterKeyspaceSpecification) actions.get(0);

		assertThat(alter.getName()).isEqualTo(CqlIdentifier.fromCql("my_keyspace"));
		assertThat(alter.getOptions()).containsKeys("durable_writes", "replication");
	}

	@Test // DATACASS-502
	void shouldAlterKeyspaceWithSimpleReplication() {

		bean.setAction(KeyspaceAction.ALTER);
		bean.setDurableWrites(true);
		bean.setName("my_keyspace");
		bean.setReplicationFactor(5);

		bean.afterPropertiesSet();

		List<KeyspaceActionSpecification> actions = bean.getObject().getActions();

		assertThat(actions).hasSize(1).hasAtLeastOneElementOfType(AlterKeyspaceSpecification.class);

		AlterKeyspaceSpecification alter = (AlterKeyspaceSpecification) actions.get(0);

		assertThat(alter.getName()).isEqualTo(CqlIdentifier.fromCql("my_keyspace"));
		assertThat(alter.getOptions()).containsKeys("durable_writes", "replication");
	}

	@Test // DATACASS-502
	void shouldAlterKeyspaceWithoutReplication() {

		bean.setAction(KeyspaceAction.ALTER);
		bean.setDurableWrites(true);
		bean.setName("my_keyspace");
		bean.afterPropertiesSet();

		List<KeyspaceActionSpecification> actions = bean.getObject().getActions();

		assertThat(actions).hasSize(1).hasAtLeastOneElementOfType(AlterKeyspaceSpecification.class);

		AlterKeyspaceSpecification alter = (AlterKeyspaceSpecification) actions.get(0);

		assertThat(alter.getName()).isEqualTo(CqlIdentifier.fromCql("my_keyspace"));
		assertThat(alter.getOptions()).doesNotContainKeys("replication");
	}
}
