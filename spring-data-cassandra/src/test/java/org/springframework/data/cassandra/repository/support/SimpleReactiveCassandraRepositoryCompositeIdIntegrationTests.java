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

import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.Collections;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.core.ReactiveCassandraTemplate;
import org.springframework.data.cassandra.core.cql.session.DefaultBridgedReactiveSession;
import org.springframework.data.cassandra.core.mapping.BasicMapId;
import org.springframework.data.cassandra.core.mapping.MapId;
import org.springframework.data.cassandra.domain.CompositeKey;
import org.springframework.data.cassandra.domain.TypeWithKeyClass;
import org.springframework.data.cassandra.domain.TypeWithMapId;
import org.springframework.data.cassandra.domain.User;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTests;

/**
 * Integration tests for {@link SimpleReactiveCassandraRepository} using MapId and primary key classes.
 *
 * @author Mark Paluch
 */
class SimpleReactiveCassandraRepositoryCompositeIdIntegrationTests extends AbstractKeyspaceCreatingIntegrationTests {

	private SimpleReactiveCassandraRepository<User, MapId> simple;

	private SimpleReactiveCassandraRepository<TypeWithMapId, MapId> mapId;

	private SimpleReactiveCassandraRepository<TypeWithKeyClass, CompositeKey> primaryKeyClass;

	@BeforeEach
	@SuppressWarnings("unchecked")
	void setUp() {

		CassandraTemplate template = new CassandraTemplate(this.session);
		SchemaTestUtils.potentiallyCreateTableFor(User.class, template);
		SchemaTestUtils.potentiallyCreateTableFor(TypeWithMapId.class, template);
		SchemaTestUtils.potentiallyCreateTableFor(TypeWithKeyClass.class, template);

		SchemaTestUtils.truncate(TypeWithMapId.class, template);
		SchemaTestUtils.truncate(TypeWithKeyClass.class, template);

		ReactiveCassandraTemplate reactiveTemplate = new ReactiveCassandraTemplate(
				new DefaultBridgedReactiveSession(this.session));

		simple = new SimpleReactiveCassandraRepository<>(new MappingCassandraEntityInformation(
				template.getConverter().getMappingContext().getRequiredPersistentEntity(User.class), template.getConverter()),
				reactiveTemplate);

		mapId = new SimpleReactiveCassandraRepository<>(new MappingCassandraEntityInformation(
				template.getConverter().getMappingContext().getRequiredPersistentEntity(TypeWithMapId.class),
				template.getConverter()), reactiveTemplate);

		primaryKeyClass = new SimpleReactiveCassandraRepository<>(new MappingCassandraEntityInformation(
				template.getConverter().getMappingContext().getRequiredPersistentEntity(TypeWithKeyClass.class),
				template.getConverter()), reactiveTemplate);
	}

	@Test // DATACASS-661
	void shouldFindByIdWithSimpleKey() {

		User user = new User();
		user.setId("heisenberg");
		user.setFirstname("Walter");
		user.setLastname("White");

		simple.save(user) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		simple.findAllById(Collections.singletonList(BasicMapId.id("id", user.getId()))) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();
	}

	@Test // DATACASS-661
	void shouldFindByIdWithCompositeKey() {

		TypeWithMapId withMapId = new TypeWithMapId();
		withMapId.setFirstname("Walter");
		withMapId.setLastname("White");

		mapId.save(withMapId) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		mapId.findAllById(Collections.singletonList(withMapId.getMapId())) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();
	}

	@Test // GH-1298
	void shouldDeleteAllByMapId() {

		TypeWithMapId withMapId1 = new TypeWithMapId();
		withMapId1.setFirstname("Walter");
		withMapId1.setLastname("White");

		TypeWithMapId withMapId2 = new TypeWithMapId();
		withMapId2.setFirstname("Skyler");
		withMapId2.setLastname("White");

		mapId.saveAll(Arrays.asList(withMapId1, withMapId2)).then() //
				.as(StepVerifier::create) //
				.verifyComplete();

		mapId.deleteAllById(Arrays.asList(withMapId1.getMapId(), withMapId2.getMapId())) //
				.as(StepVerifier::create) //
				.verifyComplete();

		mapId.findAll() //
				.as(StepVerifier::create) //
				.verifyComplete();
	}

	@Test // GH-1298
	void shouldDeleteAllByCompositeId() {

		TypeWithKeyClass composite1 = new TypeWithKeyClass();
		composite1.setKey(new CompositeKey("Walter", "White"));

		TypeWithKeyClass composite2 = new TypeWithKeyClass();
		composite2.setKey(new CompositeKey("Skyler", "White"));

		primaryKeyClass.saveAll(Arrays.asList(composite1, composite2)).then() //
				.as(StepVerifier::create) //
				.verifyComplete();

		primaryKeyClass.deleteAllById(Arrays.asList(composite1.getKey(), composite2.getKey())) //
				.as(StepVerifier::create) //
				.verifyComplete();

		primaryKeyClass.findAll() //
				.as(StepVerifier::create) //
				.verifyComplete();
	}
}
