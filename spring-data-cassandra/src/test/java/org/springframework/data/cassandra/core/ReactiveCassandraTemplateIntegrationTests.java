/*
 * Copyright 2016 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;

import org.junit.Before;
import org.junit.Test;
import org.springframework.cassandra.core.DefaultBridgedReactiveSession;
import org.springframework.cassandra.core.ReactiveCqlTemplate;
import org.springframework.cassandra.test.integration.AbstractKeyspaceCreatingIntegrationTest;
import org.springframework.data.cassandra.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.domain.Person;
import org.springframework.data.cassandra.test.integration.support.SchemaTestUtils;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * Integration tests for {@link ReactiveCassandraTemplate}.
 * 
 * @author Mark Paluch
 */
public class ReactiveCassandraTemplateIntegrationTests extends AbstractKeyspaceCreatingIntegrationTest {

	private ReactiveCassandraTemplate template;

	@Before
	public void setUp() throws Exception {

		MappingCassandraConverter converter = new MappingCassandraConverter();
		CassandraTemplate cassandraTemplate = new CassandraTemplate(session, converter);

		DefaultBridgedReactiveSession session = new DefaultBridgedReactiveSession(this.session, Schedulers.elastic());
		template = new ReactiveCassandraTemplate(new ReactiveCqlTemplate(session), converter);

		SchemaTestUtils.potentiallyCreateTableFor(Person.class, cassandraTemplate);
		SchemaTestUtils.truncate(Person.class, cassandraTemplate);
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void insertShouldInsertEntity() {

		Person person = new Person("heisenberg", "Walter", "White");

		Mono<Person> insert = template.insert(person);

		Mono<Person> oneById = template.selectOneById(person.getId(), Person.class);
		assertThat(oneById.hasElement().block()).isFalse();

		Person saved = insert.block();
		assertThat(saved).isNotNull().isEqualTo(person);
		assertThat(oneById.block()).isNotNull().isEqualTo(saved);
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void shouldInsertAndCountEntities() {

		Person person = new Person("heisenberg", "Walter", "White");

		template.insert(person).block();

		Mono<Long> count = template.count(Person.class);
		assertThat(count.block()).isEqualTo(1L);
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void updateShouldUpdateEntity() {

		Person person = new Person("heisenberg", "Walter", "White");
		template.insert(person).block();

		person.setFirstname("Walter Hartwell");
		Person updated = template.update(person).block();
		assertThat(updated).isNotNull();

		Mono<Person> oneById = template.selectOneById(person.getId(), Person.class);
		assertThat(oneById.block()).isEqualTo(person);
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void deleteShouldRemoveEntity() {

		Person person = new Person("heisenberg", "Walter", "White");
		template.insert(person).block();

		Person deleted = template.delete(person).block();
		assertThat(deleted).isNotNull();

		Mono<Person> oneById = template.selectOneById(person.getId(), Person.class);
		assertThat(oneById.block()).isNull();
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void deleteByIdShouldRemoveEntity() {

		Person person = new Person("heisenberg", "Walter", "White");
		template.insert(person).block();

		Boolean deleted = template.deleteById(person.getId(), Person.class).block();
		assertThat(deleted).isTrue();

		Mono<Person> oneById = template.selectOneById(person.getId(), Person.class);
		assertThat(oneById.block()).isNull();
	}
}
