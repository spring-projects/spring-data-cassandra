/*
 * Copyright 2019-2021 the original author or authors.
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

import java.util.Collections;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.core.mapping.BasicMapId;
import org.springframework.data.cassandra.core.mapping.MapId;
import org.springframework.data.cassandra.domain.TypeWithMapId;
import org.springframework.data.cassandra.domain.User;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTests;

/**
 * Integration tests for {@link SimpleCassandraRepository} using MapId.
 *
 * @author Mark Paluch
 */
class SimpleCassandraRepositoryMapIdIntegrationTests extends AbstractKeyspaceCreatingIntegrationTests {

	private SimpleCassandraRepository<User, MapId> simple;

	private SimpleCassandraRepository<TypeWithMapId, MapId> composite;

	@BeforeEach
	@SuppressWarnings("unchecked")
	void setUp() {

		CassandraTemplate template = new CassandraTemplate(this.session);
		SchemaTestUtils.potentiallyCreateTableFor(User.class, template);
		SchemaTestUtils.potentiallyCreateTableFor(TypeWithMapId.class, template);

		SchemaTestUtils.truncate(TypeWithMapId.class, template);
		SchemaTestUtils.truncate(TypeWithMapId.class, template);

		simple = new SimpleCassandraRepository<>(new MappingCassandraEntityInformation(
				template.getConverter().getMappingContext().getRequiredPersistentEntity(User.class), template.getConverter()),
				template);

		composite = new SimpleCassandraRepository<>(new MappingCassandraEntityInformation(
				template.getConverter().getMappingContext().getRequiredPersistentEntity(TypeWithMapId.class),
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

		composite.save(withMapId);

		assertThatThrownBy(() -> composite.findAllById(Collections.singletonList(withMapId.getMapId())))
				.isInstanceOf(InvalidDataAccessApiUsageException.class);
	}
}
