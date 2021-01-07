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
package org.springframework.data.cassandra.repository;

import static org.assertj.core.api.Assertions.*;

import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.data.cassandra.config.SchemaAction;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.domain.Person;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;
import org.springframework.data.cassandra.repository.support.AbstractSpringDataEmbeddedCassandraIntegrationTest;
import org.springframework.data.cassandra.repository.support.IntegrationTestConfig;
import org.springframework.data.cassandra.repository.support.SchemaTestUtils;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import com.datastax.oss.driver.api.core.cql.ResultSet;

/**
 * Integration tests for use with {@link PersonRepositoryWithNamedQueries}.
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
 * @soundtrack Mary Jane Kelly - Volbeat
 */
@SpringJUnitConfig
class NamedQueryIntegrationTests extends AbstractSpringDataEmbeddedCassandraIntegrationTest {

	@Configuration
	@EnableCassandraRepositories(basePackageClasses = PersonRepositoryWithNamedQueries.class,
			namedQueriesLocation = "classpath:META-INF/PersonRepositoryWithNamedQueries.properties",
			considerNestedRepositories = true,
			includeFilters = @Filter(pattern = ".*PersonRepositoryWithNamedQueries", type = FilterType.REGEX))
	public static class Config extends IntegrationTestConfig {

		@Override
		protected Set<Class<?>> getInitialEntitySet() {
			return Collections.singleton(Person.class);
		}

		@Override
		public SchemaAction getSchemaAction() {
			return SchemaAction.RECREATE_DROP_UNUSED;
		}
	}

	@Autowired PersonRepositoryWithNamedQueries personRepository;
	@Autowired CassandraOperations cassandraOperations;

	@BeforeEach
	void before() {
		SchemaTestUtils.truncate(Person.class, cassandraOperations);
	}

	@Test
	void testListMethodSingleResult() {

		Person saved = new Person();
		saved.setFirstname(uuid());
		saved.setLastname(uuid());

		saved = personRepository.save(saved);

		List<Person> results = personRepository.findFolksWithLastnameAsList(saved.getLastname());

		assertThat(results).isNotNull();
		assertThat(results.size() == 1).isTrue();
		Person found = results.iterator().next();
		assertThat(found).isNotNull();
		assertThat(saved.getLastname()).isEqualTo(found.getLastname());
		assertThat(saved.getFirstname()).isEqualTo(found.getFirstname());
	}

	@Test
	void testListMethodMultipleResults() {

		Person saved = new Person();
		saved.setFirstname("a");
		saved.setLastname(uuid());

		saved = personRepository.save(saved);

		Person saved2 = new Person();
		saved2.setFirstname("b");
		saved2.setLastname(saved.getLastname());

		saved2 = personRepository.save(saved2);

		List<Person> results = personRepository.findFolksWithLastnameAsList(saved.getLastname());

		assertThat(results).isNotNull();
		assertThat(results.size() == 2).isTrue();
		boolean first = true;
		for (Person person : results) {
			assertThat(person).isNotNull();
			assertThat(person.getLastname()).isEqualTo(saved.getLastname());
			assertThat(person.getFirstname()).isEqualTo(first ? saved.getFirstname() : saved2.getFirstname());
			first = false;
		}
	}

	@Test
	void testListOfMapOfStringToObjectMethodSingleResult() {

		Person saved = new Person();
		saved.setFirstname(uuid());
		saved.setLastname(uuid());

		saved = personRepository.save(saved);

		List<Map<String, Object>> results = personRepository
				.findFolksWithLastnameAsListOfMapOfStringToObject(saved.getLastname());

		assertThat(results).isNotNull();
		assertThat(results.size() == 1).isTrue();
		Map<String, Object> found = results.iterator().next();
		assertThat(found).isNotNull();
		assertThat(saved.getLastname()).isEqualTo(found.get("lastname"));
		assertThat(saved.getFirstname()).isEqualTo(found.get("firstname"));
	}

	@Test
	void testEntityMethodResult() {

		Person saved = new Person();
		saved.setFirstname(uuid());
		saved.setLastname(uuid());

		saved = personRepository.save(saved);

		Person found = personRepository.findSingle(saved.getLastname(), saved.getFirstname());

		assertThat(found).isNotNull();
		assertThat(saved.getLastname()).isEqualTo(found.getLastname());
		assertThat(saved.getFirstname()).isEqualTo(found.getFirstname());
	}

	@Test
	void testListOfMapOfStringToObjectMethodMultipleResults() {

		Person saved = new Person();
		saved.setFirstname("a");
		saved.setLastname(uuid());

		saved = personRepository.save(saved);

		Person saved2 = new Person();
		saved2.setFirstname("b");
		saved2.setLastname(saved.getLastname());

		saved2 = personRepository.save(saved2);

		Collection<Person> results = personRepository.findFolksWithLastnameAsList(saved.getLastname());

		assertThat(results).isNotNull();
		assertThat(results.size() == 2).isTrue();
		boolean first = true;
		for (Person person : results) {
			assertThat(person).isNotNull();
			assertThat(person.getLastname()).isEqualTo(saved.getLastname());
			assertThat(person.getFirstname()).isEqualTo(first ? saved.getFirstname() : saved2.getFirstname());
			first = false;
		}
	}

	@Test
	void testStringMethodResult() {

		Person saved = new Person();
		saved.setFirstname(uuid());
		saved.setLastname(uuid());
		saved.setNickname(uuid());

		saved = personRepository.save(saved);

		String nickname = personRepository.findSingleNickname(saved.getLastname(), saved.getFirstname());

		assertThat(nickname).isNotNull();
		assertThat(nickname).isEqualTo(saved.getNickname());
	}

	@Test
	void testBooleanMethodResult() {

		Person saved = new Person();
		saved.setFirstname(uuid());
		saved.setLastname(uuid());
		saved.setCool(true);

		saved = personRepository.save(saved);

		boolean value = personRepository.findSingleCool(saved.getLastname(), saved.getFirstname());

		assertThat(value).isEqualTo(saved.isCool());
	}

	@Test
	void testDateMethodResult() {

		Person saved = new Person();
		saved.setFirstname(uuid());
		saved.setLastname(uuid());
		saved.setBirthDate(new Date());

		saved = personRepository.save(saved);

		Date value = personRepository.findSingleBirthdate(saved.getLastname(), saved.getFirstname());

		assertThat(value).isEqualTo(saved.getBirthDate());
	}

	@Test
	void testIntMethodResult() {

		Person saved = new Person();
		saved.setFirstname(uuid());
		saved.setLastname(uuid());
		saved.setNumberOfChildren(1);

		saved = personRepository.save(saved);

		int value = personRepository.findSingleNumberOfChildren(saved.getLastname(), saved.getFirstname());

		assertThat(value).isEqualTo(saved.getNumberOfChildren());
	}

	@Test
	void testArrayMethodSingleResult() {

		Person saved = new Person();
		saved.setFirstname(uuid());
		saved.setLastname(uuid());

		saved = personRepository.save(saved);

		Person[] results = personRepository.findFolksWithLastnameAsArray(saved.getLastname());

		assertThat(results).isNotNull();
		assertThat(results.length == 1).isTrue();
		Person found = results[0];
		assertThat(found).isNotNull();
		assertThat(saved.getLastname()).isEqualTo(found.getLastname());
		assertThat(saved.getFirstname()).isEqualTo(found.getFirstname());
	}

	@Test
	void testEscapeSingleQuoteInQueryParameterValue() {

		Person saved = new Person();
		saved.setFirstname("Bri'an" + uuid());
		String lastname = "O'Brian" + uuid();
		saved.setLastname(lastname);

		saved = personRepository.save(saved);

		List<Person> results = personRepository.findFolksWithLastnameAsList(lastname);

		assertThat(results).isNotNull();
		assertThat(results.size() == 1).isTrue();
		for (Person person : results) {
			assertThat(person).isNotNull();
			assertThat(person.getLastname()).isEqualTo(saved.getLastname());
			assertThat(person.getFirstname()).isEqualTo(saved.getFirstname());
		}
	}

	@Test
	void findOptionalShouldReturnTargetType() {

		Person personToSave = new Person();

		personToSave.setFirstname(uuid());
		personToSave.setLastname(uuid());
		personToSave.setNumberOfChildren(1);

		personToSave = personRepository.save(personToSave);

		Optional<Person> savedPerson = personRepository.findOptionalWithLastnameAndFirstname(personToSave.getLastname(),
				personToSave.getFirstname());

		assertThat(savedPerson.isPresent()).isTrue();
	}

	@Test
	void findOptionalShouldAbsentOptional() {

		Optional<Person> optional = personRepository.findOptionalWithLastnameAndFirstname("not", "existent");

		assertThat(optional.isPresent()).isFalse();
	}

	@Test // DATACASS-297
	void streamShouldReturnEntities() {

		long before = personRepository.count();

		for (int i = 0; i < 100; i++) {
			Person person = new Person();

			person.setFirstname(uuid());
			person.setLastname(uuid());
			person.setNumberOfChildren(i);

			personRepository.save(person);
		}

		Stream<Person> allPeople = personRepository.findPeopleBy();

		long count = allPeople.peek(person -> assertThat(person).isInstanceOf(Person.class)).count();

		assertThat(count).isEqualTo(before + 100L);
	}

	public interface PersonRepositoryWithNamedQueries extends MapIdCassandraRepository<Person> {

		List<Person> findFolksWithLastnameAsList(String lastname);

		ResultSet findFolksWithLastnameAsResultSet(String last);

		Person[] findFolksWithLastnameAsArray(String lastname);

		Person findSingle(String last, String first);

		List<Map<String, Object>> findFolksWithLastnameAsListOfMapOfStringToObject(String last);

		String findSingleNickname(String last, String first);

		Date findSingleBirthdate(String last, String first);

		boolean findSingleCool(String last, String first);

		int findSingleNumberOfChildren(String last, String first);

		Optional<Person> findOptionalWithLastnameAndFirstname(String last, String first);

		Stream<Person> findPeopleBy();
	}
}
