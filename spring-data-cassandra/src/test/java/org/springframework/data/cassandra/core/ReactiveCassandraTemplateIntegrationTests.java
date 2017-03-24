/*
 * Copyright 2016-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import org.junit.Before;
import org.junit.Test;
import org.springframework.cassandra.core.ReactiveCqlTemplate;
import org.springframework.cassandra.core.session.DefaultBridgedReactiveSession;
import org.springframework.cassandra.test.integration.AbstractKeyspaceCreatingIntegrationTest;
import org.springframework.data.cassandra.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.domain.Person;
import org.springframework.data.cassandra.test.integration.support.SchemaTestUtils;

/**
 * Integration tests for {@link ReactiveCassandraTemplate}.
 *
 * @author Mark Paluch
 */
public class ReactiveCassandraTemplateIntegrationTests extends AbstractKeyspaceCreatingIntegrationTest {

	ReactiveCassandraTemplate template;

	@Before
	public void setUp() throws Exception {

		MappingCassandraConverter converter = new MappingCassandraConverter();
		CassandraTemplate cassandraTemplate = new CassandraTemplate(this.session, converter);
		DefaultBridgedReactiveSession session = new DefaultBridgedReactiveSession(this.session, Schedulers.elastic());

		template = new ReactiveCassandraTemplate(new ReactiveCqlTemplate(session), converter);

		SchemaTestUtils.potentiallyCreateTableFor(Person.class, cassandraTemplate);
		SchemaTestUtils.truncate(Person.class, cassandraTemplate);
	}

	@Test // DATACASS-335
	public void insertShouldInsertEntity() {

		Person person = new Person("heisenberg", "Walter", "White");

		Mono<Person> insert = template.insert(person);
		StepVerifier.create(template.selectOneById(person.getId(), Person.class)).verifyComplete();

		StepVerifier.create(insert).expectNext(person).verifyComplete();

		StepVerifier.create(template.selectOneById(person.getId(), Person.class)).expectNext(person).verifyComplete();
	}

	@Test // DATACASS-335
	public void shouldInsertAndCountEntities() {

		Person person = new Person("heisenberg", "Walter", "White");

		StepVerifier.create(template.insert(person)).expectNextCount(1).verifyComplete();

		StepVerifier.create(template.count(Person.class)).expectNext(1L).verifyComplete();
	}

	@Test // DATACASS-335
	public void updateShouldUpdateEntity() {

		Person person = new Person("heisenberg", "Walter", "White");

		StepVerifier.create(template.insert(person)).expectNextCount(1).verifyComplete();

		person.setFirstname("Walter Hartwell");

		StepVerifier.create(template.insert(person)).expectNextCount(1).verifyComplete();

		StepVerifier.create(template.selectOneById(person.getId(), Person.class)).expectNext(person).verifyComplete();
	}

	@Test // DATACASS-335
	public void deleteShouldRemoveEntity() {

		Person person = new Person("heisenberg", "Walter", "White");

		StepVerifier.create(template.insert(person)).expectNextCount(1).verifyComplete();

		StepVerifier.create(template.delete(person)).expectNext(person).verifyComplete();

		StepVerifier.create(template.selectOneById(person.getId(), Person.class)).verifyComplete();
	}

	@Test // DATACASS-335
	public void deleteByIdShouldRemoveEntity() {

		Person person = new Person("heisenberg", "Walter", "White");

		StepVerifier.create(template.insert(person)).expectNextCount(1).verifyComplete();

		StepVerifier.create(template.deleteById(person.getId(), Person.class)).expectNext(true).verifyComplete();

		StepVerifier.create(template.selectOneById(person.getId(), Person.class)).verifyComplete();
	}
}
