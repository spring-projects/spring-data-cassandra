/*
 * Copyright 2019-present the original author or authors.
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
package org.springframework.data.cassandra.repository.support;

import static org.assertj.core.api.Assertions.*;

import java.util.Arrays;
import java.util.Collections;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.core.mapping.BasicMapId;
import org.springframework.data.cassandra.core.mapping.MapId;
import org.springframework.data.cassandra.domain.CompositeKey;
import org.springframework.data.cassandra.domain.TypeWithKeyClass;
import org.springframework.data.cassandra.domain.TypeWithMapId;
import org.springframework.data.cassandra.domain.User;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTests;

/**
 * Integration tests for {@link SimpleCassandraRepository} using MapId and primary key classes.
 *
 * @author Mark Paluch
 */
class SimpleCassandraRepositoryCompositeIdIntegrationTests extends AbstractKeyspaceCreatingIntegrationTests {

	private SimpleCassandraRepository<User, MapId> simple;

	private SimpleCassandraRepository<TypeWithMapId, MapId> mapId;

	private SimpleCassandraRepository<TypeWithKeyClass, CompositeKey> primaryKeyClass;

	@BeforeEach
	@SuppressWarnings("unchecked")
	void setUp() {

		CassandraTemplate template = new CassandraTemplate(this.session);
		SchemaTestUtils.potentiallyCreateTableFor(User.class, template);
		SchemaTestUtils.potentiallyCreateTableFor(TypeWithMapId.class, template);
		SchemaTestUtils.potentiallyCreateTableFor(TypeWithKeyClass.class, template);

		SchemaTestUtils.truncate(TypeWithMapId.class, template);
		SchemaTestUtils.truncate(TypeWithKeyClass.class, template);

		simple = new SimpleCassandraRepository<>(new MappingCassandraEntityInformation(
				template.getConverter().getMappingContext().getRequiredPersistentEntity(User.class), template.getConverter()),
				template);

		mapId = new SimpleCassandraRepository<>(new MappingCassandraEntityInformation(
				template.getConverter().getMappingContext().getRequiredPersistentEntity(TypeWithMapId.class),
				template.getConverter()), template);

		primaryKeyClass = new SimpleCassandraRepository<>(new MappingCassandraEntityInformation(
				template.getConverter().getMappingContext().getRequiredPersistentEntity(TypeWithKeyClass.class),
				template.getConverter()), template);
	}

	@Test // DATACASS-661
	void shouldFindByIdWithSimpleKey() {

		User user = new User();
		user.setId("heisenberg");
		user.setFirstname("Walter");
		user.setLastname("White");

		simple.save(user);

		assertThat(simple.findAllById(Collections.singletonList(BasicMapId.id("id", user.getId())))).hasSize(1);
	}

	@Test // DATACASS-661
	void shouldFindByIdWithCompositeKey() {

		TypeWithMapId withMapId = new TypeWithMapId();
		withMapId.setFirstname("Walter");
		withMapId.setLastname("White");

		mapId.save(withMapId);

		assertThatThrownBy(() -> mapId.findAllById(Collections.singletonList(withMapId.getMapId())))
				.isInstanceOf(InvalidDataAccessApiUsageException.class);
	}

	@Test // GH-1298
	void shouldDeleteAllByMapId() {

		TypeWithMapId withMapId1 = new TypeWithMapId();
		withMapId1.setFirstname("Walter");
		withMapId1.setLastname("White");

		TypeWithMapId withMapId2 = new TypeWithMapId();
		withMapId2.setFirstname("Skyler");
		withMapId2.setLastname("White");

		mapId.saveAll(Arrays.asList(withMapId1, withMapId2));

		mapId.deleteAllById(Arrays.asList(withMapId1.getMapId(), withMapId2.getMapId()));

		assertThat(mapId.findAll()).isEmpty();
	}

	@Test // GH-1298
	void shouldDeleteAllByCompositeId() {

		TypeWithKeyClass composite1 = new TypeWithKeyClass();
		composite1.setKey(new CompositeKey("Walter", "White"));

		TypeWithKeyClass composite2 = new TypeWithKeyClass();
		composite2.setKey(new CompositeKey("Skyler", "White"));

		primaryKeyClass.saveAll(Arrays.asList(composite1, composite2));

		primaryKeyClass.deleteAllById(Arrays.asList(composite1.getKey(), composite2.getKey()));

		assertThat(primaryKeyClass.findAll()).isEmpty();
	}
}
