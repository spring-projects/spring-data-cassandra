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

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;
import org.springframework.cassandra.core.CqlTemplate;
import org.springframework.cassandra.test.integration.AbstractKeyspaceCreatingIntegrationTest;
import org.springframework.data.cassandra.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.domain.Person;
import org.springframework.data.cassandra.domain.UserToken;
import org.springframework.data.cassandra.repository.support.BasicMapId;
import org.springframework.data.cassandra.test.integration.simpletons.BookReference;
import org.springframework.data.cassandra.test.integration.support.SchemaTestUtils;

import com.datastax.driver.core.utils.UUIDs;

/**
 * Integration tests for {@link CassandraTemplate}.
 * 
 * @author Mark Paluch
 */
public class CassandraTemplateIntegrationTests extends AbstractKeyspaceCreatingIntegrationTest {

	private CassandraTemplate template;

	@Before
	public void setUp() {

		MappingCassandraConverter converter = new MappingCassandraConverter();
		converter.afterPropertiesSet();

		template = new CassandraTemplate(new CqlTemplate(session), converter);

		SchemaTestUtils.potentiallyCreateTableFor(Person.class, template);
		SchemaTestUtils.potentiallyCreateTableFor(UserToken.class, template);
		SchemaTestUtils.potentiallyCreateTableFor(BookReference.class, template);
		SchemaTestUtils.truncate(Person.class, template);
		SchemaTestUtils.truncate(UserToken.class, template);
		SchemaTestUtils.truncate(BookReference.class, template);
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void insertShouldInsertEntity() {

		Person person = new Person("heisenberg", "Walter", "White");

		assertThat(template.selectOneById(person.getId(), Person.class)).isNull();

		Person inserted = template.insert(person);

		assertThat(inserted).isNotNull().isEqualTo(person);
		assertThat(template.selectOneById(person.getId(), Person.class)).isEqualTo(person);
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void shouldInsertAndCountEntities() {

		Person person = new Person("heisenberg", "Walter", "White");

		template.insert(person);

		long count = template.count(Person.class);
		assertThat(count).isEqualTo(1L);
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void updateShouldUpdateEntity() {

		Person person = new Person("heisenberg", "Walter", "White");
		template.insert(person);

		person.setFirstname("Walter Hartwell");
		Person updated = template.update(person);
		assertThat(updated).isNotNull();

		assertThat(template.selectOneById(person.getId(), Person.class)).isEqualTo(person);
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void deleteShouldRemoveEntity() {

		Person person = new Person("heisenberg", "Walter", "White");
		template.insert(person);

		Person deleted = template.delete(person);
		assertThat(deleted).isNotNull();

		assertThat(template.selectOneById(person.getId(), Person.class)).isNull();
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void deleteByIdShouldRemoveEntity() {

		Person person = new Person("heisenberg", "Walter", "White");
		template.insert(person);

		Boolean deleted = template.deleteById(person.getId(), Person.class);
		assertThat(deleted).isTrue();

		assertThat(template.selectOneById(person.getId(), Person.class)).isNull();
	}

	/**
	 * @see DATACASS-182
	 */
	@Test
	public void stream() {

		Person person = new Person("heisenberg", "Walter", "White");
		template.insert(person);

		Stream<Person> stream = template.stream("SELECT * FROM person", Person.class);

		assertThat(stream.collect(Collectors.toList())).hasSize(1).contains(person);
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-182">DATACASS-182</a>
	 */
	@Test
	public void updateShouldRemoveFields() {

		Person person = new Person("heisenberg", "Walter", "White");

		template.insert(person);

		person.setFirstname(null);
		template.update(person);

		Person loaded = template.selectOneById(person.getId(), Person.class);

		assertThat(loaded.getFirstname()).isNull();
		assertThat(loaded.getId()).isEqualTo("heisenberg");
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-182">DATACASS-182</a>
	 */
	@Test
	public void insertShouldRemoveFields() {

		Person person = new Person("heisenberg", "Walter", "White");

		template.insert(person);

		person.setFirstname(null);
		template.insert(person);

		Person loaded = template.selectOneById(person.getId(), Person.class);

		assertThat(loaded.getFirstname()).isNull();
		assertThat(loaded.getId()).isEqualTo("heisenberg");
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-182">DATACASS-182</a>
	 */
	@Test
	public void insertAndUpdateToEmptyCollection() {

		BookReference bookReference = new BookReference();

		bookReference.setIsbn("isbn");
		bookReference.setBookmarks(Arrays.asList(1, 2, 3, 4));

		template.insert(bookReference);

		bookReference.setBookmarks(Collections.<Integer> emptyList());

		template.update(bookReference);

		BookReference loaded = template.selectOneById(bookReference.getIsbn(), BookReference.class);

		assertThat(loaded.getTitle()).isNull();
		assertThat(loaded.getBookmarks()).isNull();
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-206">DATACASS-206</a>
	 */
	@Test
	public void shouldUseSpecifiedColumnNamesForSingleEntityModifyingOperations() {

		UserToken userToken = new UserToken();
		userToken.setToken(UUIDs.startOf(System.currentTimeMillis()));
		userToken.setUserId(UUIDs.endOf(System.currentTimeMillis()));

		template.insert(userToken);

		userToken.setUserComment("comment");
		template.update(userToken);

		UserToken loaded = template.selectOneById(
				BasicMapId.id("userId", userToken.getUserId()).with("token", userToken.getToken()), UserToken.class);

		assertThat(loaded).isNotNull();
		assertThat(loaded.getUserComment()).isEqualTo("comment");

		template.delete(userToken);

		UserToken loadAfterDelete = template.selectOneById(
				BasicMapId.id("userId", userToken.getUserId()).with("token", userToken.getToken()), UserToken.class);

		assertThat(loadAfterDelete).isNull();
	}
}
