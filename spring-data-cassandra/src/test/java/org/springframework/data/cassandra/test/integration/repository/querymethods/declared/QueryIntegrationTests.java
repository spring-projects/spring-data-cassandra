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
package org.springframework.data.cassandra.test.integration.repository.querymethods.declared;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.config.SchemaAction;
import org.springframework.data.cassandra.test.integration.repository.querymethods.declared.base.PersonRepository;
import org.springframework.data.cassandra.test.integration.support.AbstractSpringDataEmbeddedCassandraIntegrationTest;
import org.springframework.data.cassandra.test.integration.support.IntegrationTestConfig;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
@RunWith(SpringJUnit4ClassRunner.class)
public abstract class QueryIntegrationTests extends AbstractSpringDataEmbeddedCassandraIntegrationTest {

	public static class Config extends IntegrationTestConfig {

		@Override
		public String[] getEntityBasePackages() {
			return new String[] { Person.class.getPackage().getName() };
		}

		@Override
		public SchemaAction getSchemaAction() {
			return SchemaAction.RECREATE_DROP_UNUSED;
		}
	}

	@Autowired PersonRepository personRepository;

	@Before
	public void before() {
		deleteAllEntities();
	}

	@Test
	public void testListMethodSingleResult() {

		Person saved = new Person();
		saved.setFirstname(uuid());
		saved.setLastname(uuid());

		saved = personRepository.save(saved);

		List<Person> results = personRepository.findFolksWithLastnameAsList(saved.getLastname());

		assertNotNull(results);
		assertTrue(results.size() == 1);
		Person found = results.iterator().next();
		assertNotNull(found);
		assertEquals(found.getLastname(), saved.getLastname());
		assertEquals(found.getFirstname(), saved.getFirstname());
	}

	@Test
	public void testListMethodMultipleResults() {

		Person saved = new Person();
		saved.setFirstname("a");
		saved.setLastname(uuid());

		saved = personRepository.save(saved);

		Person saved2 = new Person();
		saved2.setFirstname("b");
		saved2.setLastname(saved.getLastname());

		saved2 = personRepository.save(saved2);

		List<Person> results = personRepository.findFolksWithLastnameAsList(saved.getLastname());

		assertNotNull(results);
		assertTrue(results.size() == 2);
		boolean first = true;
		for (Person person : results) {
			assertNotNull(person);
			assertEquals(saved.getLastname(), person.getLastname());
			assertEquals(first ? saved.getFirstname() : saved2.getFirstname(), person.getFirstname());
			first = false;
		}
	}

	@Test
	public void testListOfMapOfStringToObjectMethodSingleResult() {

		Person saved = new Person();
		saved.setFirstname(uuid());
		saved.setLastname(uuid());

		saved = personRepository.save(saved);

		List<Map<String, Object>> results = personRepository
				.findFolksWithLastnameAsListOfMapOfStringToObject(saved.getLastname());

		assertNotNull(results);
		assertTrue(results.size() == 1);
		Map<String, Object> found = results.iterator().next();
		assertNotNull(found);
		assertEquals(found.get("lastname"), saved.getLastname());
		assertEquals(found.get("firstname"), saved.getFirstname());
	}

	@Test
	public void testEntityMethodResult() {

		Person saved = new Person();
		saved.setFirstname(uuid());
		saved.setLastname(uuid());

		saved = personRepository.save(saved);

		Person found = personRepository.findSingle(saved.getLastname(), saved.getFirstname());

		assertNotNull(found);
		assertEquals(found.getLastname(), saved.getLastname());
		assertEquals(found.getFirstname(), saved.getFirstname());
	}

	@Test
	public void testListOfMapOfStringToObjectMethodMultipleResults() {

		Person saved = new Person();
		saved.setFirstname("a");
		saved.setLastname(uuid());

		saved = personRepository.save(saved);

		Person saved2 = new Person();
		saved2.setFirstname("b");
		saved2.setLastname(saved.getLastname());

		saved2 = personRepository.save(saved2);

		Collection<Person> results = personRepository.findFolksWithLastnameAsList(saved.getLastname());

		assertNotNull(results);
		assertTrue(results.size() == 2);
		boolean first = true;
		for (Person person : results) {
			assertNotNull(person);
			assertEquals(saved.getLastname(), person.getLastname());
			assertEquals(first ? saved.getFirstname() : saved2.getFirstname(), person.getFirstname());
			first = false;
		}
	}

	@Test
	public void testStringMethodResult() {

		Person saved = new Person();
		saved.setFirstname(uuid());
		saved.setLastname(uuid());
		saved.setNickname(uuid());

		saved = personRepository.save(saved);

		String nickname = personRepository.findSingleNickname(saved.getLastname(), saved.getFirstname());

		assertNotNull(nickname);
		assertEquals(saved.getNickname(), nickname);
	}

	@Test
	public void testBooleanMethodResult() {

		Person saved = new Person();
		saved.setFirstname(uuid());
		saved.setLastname(uuid());
		saved.setCool(true);

		saved = personRepository.save(saved);

		boolean value = personRepository.findSingleCool(saved.getLastname(), saved.getFirstname());

		assertEquals(saved.isCool(), value);
	}

	@Test
	public void testDateMethodResult() {

		Person saved = new Person();
		saved.setFirstname(uuid());
		saved.setLastname(uuid());
		saved.setBirthDate(new Date());

		saved = personRepository.save(saved);

		Date value = personRepository.findSingleBirthdate(saved.getLastname(), saved.getFirstname());

		assertEquals(saved.getBirthDate(), value);
	}

	@Test
	public void testIntMethodResult() {

		Person saved = new Person();
		saved.setFirstname(uuid());
		saved.setLastname(uuid());
		saved.setNumberOfChildren(1);

		saved = personRepository.save(saved);

		int value = personRepository.findSingleNumberOfChildren(saved.getLastname(), saved.getFirstname());

		assertEquals(saved.getNumberOfChildren(), value);
	}

	@Test
	public void testArrayMethodSingleResult() {

		Person saved = new Person();
		saved.setFirstname(uuid());
		saved.setLastname(uuid());

		saved = personRepository.save(saved);

		Person[] results = personRepository.findFolksWithLastnameAsArray(saved.getLastname());

		assertNotNull(results);
		assertTrue(results.length == 1);
		Person found = results[0];
		assertNotNull(found);
		assertEquals(found.getLastname(), saved.getLastname());
		assertEquals(found.getFirstname(), saved.getFirstname());
	}

	@Test
	public void testEscapeSingleQuoteInQueryParameterValue() {

		Person saved = new Person();
		saved.setFirstname("Bri'an" + uuid());
		String lastname = "O'Brian" + uuid();
		saved.setLastname(lastname);

		saved = personRepository.save(saved);

		List<Person> results = personRepository.findFolksWithLastnameAsList(lastname);

		assertNotNull(results);
		assertTrue(results.size() == 1);
		for (Person person : results) {
			assertNotNull(person);
			assertEquals(saved.getLastname(), person.getLastname());
			assertEquals(saved.getFirstname(), person.getFirstname());
		}
	}

	@Test
	public void findOptionalShouldReturnTargetType() {

		Person personToSave = new Person();

		personToSave.setFirstname(uuid());
		personToSave.setLastname(uuid());
		personToSave.setNumberOfChildren(1);

		personToSave = personRepository.save(personToSave);

		Optional<Person> savedPerson = personRepository.findOptionalWithLastnameAndFirstname(
			personToSave.getLastname(), personToSave.getFirstname());

		assertThat(savedPerson, is(notNullValue(Optional.class)));
		assertThat(savedPerson.isPresent(), is(true));
		assertThat(savedPerson.get(), is(notNullValue(Person.class)));
	}

	@Test
	public void findOptionalShouldAbsentOptional() {

		Optional<Person> optional = personRepository.findOptionalWithLastnameAndFirstname("not", "existent");

		assertThat(optional.isPresent(), is(false));
	}

	/**
	 * @see <a href="DATACASS-297">https://jira.spring.io/browse/DATACASS-297</a>
	 */
	@Test
	public void streamShouldReturnEntities() {

		for (int i = 0; i < 100; i++) {
			Person person = new Person();

			person.setFirstname(uuid());
			person.setLastname(uuid());
			person.setNumberOfChildren(i);

			personRepository.save(person);
		}

		Stream<Person> allPeople = personRepository.findAllPeople();

		long count = allPeople.peek(new Consumer<Person>() {
			@Override
			public void accept(Person person) {
				assertThat(person, is(instanceOf(Person.class)));
			}
		}).count();

		assertThat(count, is(equalTo(100L)));
	}
}
