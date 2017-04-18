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

import static org.assertj.core.api.Assertions.*;
import static org.junit.Assume.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;
import org.springframework.cassandra.core.CqlTemplate;
import org.springframework.cassandra.test.integration.AbstractKeyspaceCreatingIntegrationTest;
import org.springframework.cassandra.test.integration.support.CassandraVersion;
import org.springframework.data.cassandra.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.query.Columns;
import org.springframework.data.cassandra.core.query.Criteria;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.cassandra.core.query.Update;
import org.springframework.data.cassandra.domain.Person;
import org.springframework.data.cassandra.domain.UserToken;
import org.springframework.data.cassandra.repository.support.BasicMapId;
import org.springframework.data.cassandra.test.integration.simpletons.BookReference;
import org.springframework.data.cassandra.test.integration.support.SchemaTestUtils;
import org.springframework.data.domain.Sort;
import org.springframework.data.util.Version;

import com.datastax.driver.core.utils.UUIDs;

/**
 * Integration tests for {@link CassandraTemplate}.
 *
 * @author Mark Paluch
 */
public class CassandraTemplateIntegrationTests extends AbstractKeyspaceCreatingIntegrationTest {

	final static Version CASSANDRA_3 = Version.parse("3.0");

	Version cassandraVersion;

	CassandraTemplate template;

	@Before
	public void setUp() {

		MappingCassandraConverter converter = new MappingCassandraConverter();
		converter.afterPropertiesSet();

		cassandraVersion = CassandraVersion.get(session);

		template = new CassandraTemplate(new CqlTemplate(session), converter);

		SchemaTestUtils.potentiallyCreateTableFor(Person.class, template);
		SchemaTestUtils.potentiallyCreateTableFor(UserToken.class, template);
		SchemaTestUtils.potentiallyCreateTableFor(BookReference.class, template);
		SchemaTestUtils.truncate(Person.class, template);
		SchemaTestUtils.truncate(UserToken.class, template);
		SchemaTestUtils.truncate(BookReference.class, template);
	}

	@Test // DATACASS-343
	public void shouldSelectByQueryWithAllowFiltering() {

		assumeTrue(cassandraVersion.isGreaterThanOrEqualTo(CASSANDRA_3));

		UserToken userToken = new UserToken();
		userToken.setUserId(UUIDs.endOf(System.currentTimeMillis()));
		userToken.setToken(UUIDs.startOf(System.currentTimeMillis()));
		userToken.setUserComment("cook");

		template.insert(userToken);

		Query query = Query.query(Criteria.where("userId").is(userToken.getUserId()))
				.and(Criteria.where("userComment").is("cook")).withAllowFiltering();
		UserToken loaded = template.selectOne(query, UserToken.class);

		assertThat(loaded).isNotNull();
		assertThat(loaded.getUserComment()).isEqualTo("cook");
	}

	@Test // DATACASS-343
	public void shouldSelectByQueryWithSorting() {

		UserToken token1 = new UserToken();
		token1.setUserId(UUIDs.endOf(System.currentTimeMillis()));
		token1.setToken(UUIDs.startOf(System.currentTimeMillis()));
		token1.setUserComment("foo");

		UserToken token2 = new UserToken();
		token2.setUserId(token1.getUserId());
		token2.setToken(UUIDs.endOf(System.currentTimeMillis() + 100));
		token2.setUserComment("bar");

		template.insert(token1);
		template.insert(token2);

		Query query = Query.query(Criteria.where("userId").is(token1.getUserId())).sort(Sort.by("token"));
		List<UserToken> loaded = template.select(query, UserToken.class);

		assertThat(loaded).containsSequence(token1, token2);
	}

	@Test // DATACASS-343
	public void shouldSelectOneByQuery() {

		UserToken token1 = new UserToken();
		token1.setUserId(UUIDs.endOf(System.currentTimeMillis()));
		token1.setToken(UUIDs.startOf(System.currentTimeMillis()));
		token1.setUserComment("foo");

		template.insert(token1);

		Query query = Query.query(Criteria.where("userId").is(token1.getUserId()));
		UserToken loaded = template.selectOne(query, UserToken.class);

		assertThat(loaded).isEqualTo(token1);
	}

	@Test // DATACASS-292
	public void insertShouldInsertEntity() {

		Person person = new Person("heisenberg", "Walter", "White");

		assertThat(template.selectOneById(person.getId(), Person.class)).isNull();

		Person inserted = template.insert(person);

		assertThat(inserted).isEqualTo(person);
		assertThat(template.selectOneById(person.getId(), Person.class)).isEqualTo(person);
	}

	@Test // DATACASS-292
	public void shouldInsertAndCountEntities() {

		Person person = new Person("heisenberg", "Walter", "White");

		template.insert(person);

		long count = template.count(Person.class);
		assertThat(count).isEqualTo(1L);
	}

	@Test // DATACASS-292
	public void updateShouldUpdateEntity() {

		Person person = new Person("heisenberg", "Walter", "White");
		template.insert(person);

		person.setFirstname("Walter Hartwell");

		Person updated = template.update(person);

		assertThat(updated).isNotNull();
		assertThat(template.selectOneById(person.getId(), Person.class)).isEqualTo(person);
	}

	@Test // DATACASS-343
	public void updateShouldUpdateEntityByQuery() {

		Person person = new Person("heisenberg", "Walter", "White");
		template.insert(person);

		Query query = Query.query(Criteria.where("id").is("heisenberg"));
		boolean result = template.update(query, Update.empty().set("firstname", "Walter Hartwell"), Person.class);
		assertThat(result).isTrue();

		assertThat(template.selectOneById(person.getId(), Person.class).getFirstname()).isEqualTo("Walter Hartwell");
	}

	@Test // DATACASS-343
	public void deleteByQueryShouldRemoveEntity() {

		Person person = new Person("heisenberg", "Walter", "White");
		template.insert(person);

		Query query = Query.query(Criteria.where("id").is("heisenberg"));
		assertThat(template.delete(query, Person.class)).isTrue();

		assertThat(template.selectOneById(person.getId(), Person.class)).isNull();
	}

	@Test // DATACASS-343
	public void deleteColumnsByQueryShouldRemoveColumn() {

		Person person = new Person("heisenberg", "Walter", "White");
		template.insert(person);

		Query query = Query.query(Criteria.where("id").is("heisenberg")).columns(Columns.from("lastname"));

		assertThat(template.delete(query, Person.class)).isTrue();

		Person loaded = template.selectOneById(person.getId(), Person.class);
		assertThat(loaded.getFirstname()).isEqualTo("Walter");
		assertThat(loaded.getLastname()).isNull();
	}

	@Test // DATACASS-292
	public void deleteShouldRemoveEntity() {

		Person person = new Person("heisenberg", "Walter", "White");
		template.insert(person);

		Person deleted = template.delete(person);

		assertThat(deleted).isNotNull();
		assertThat(template.selectOneById(person.getId(), Person.class)).isNull();
	}

	@Test // DATACASS-292
	public void deleteByIdShouldRemoveEntity() {

		Person person = new Person("heisenberg", "Walter", "White");
		template.insert(person);

		Boolean deleted = template.deleteById(person.getId(), Person.class);
		assertThat(deleted).isTrue();

		assertThat(template.selectOneById(person.getId(), Person.class)).isNull();
	}

	@Test // DATACASS-182
	public void stream() {

		Person person = new Person("heisenberg", "Walter", "White");
		template.insert(person);

		Stream<Person> stream = template.stream("SELECT * FROM person", Person.class);

		assertThat(stream.collect(Collectors.toList())).hasSize(1).contains(person);
	}

	@Test // DATACASS-343
	public void streamByQuery() {

		Person person = new Person("heisenberg", "Walter", "White");
		template.insert(person);

		Query query = Query.query(Criteria.where("id").is("heisenberg"));

		Stream<Person> stream = template.stream(query, Person.class);

		assertThat(stream.collect(Collectors.toList())).hasSize(1).contains(person);
	}

	@Test // DATACASS-182
	public void updateShouldRemoveFields() {

		Person person = new Person("heisenberg", "Walter", "White");

		template.insert(person);

		person.setFirstname(null);
		template.update(person);

		Person loaded = template.selectOneById(person.getId(), Person.class);

		assertThat(loaded.getFirstname()).isNull();
		assertThat(loaded.getId()).isEqualTo("heisenberg");
	}

	@Test // DATACASS-182, DATACASS-420
	public void insertShouldNotRemoveFields() {

		Person person = new Person("heisenberg", "Walter", "White");

		template.insert(person);

		person.setFirstname(null);
		template.insert(person);

		Person loaded = template.selectOneById(person.getId(), Person.class);

		assertThat(loaded.getFirstname()).isEqualTo("Walter");
		assertThat(loaded.getId()).isEqualTo("heisenberg");
	}

	@Test // DATACASS-182
	public void insertAndUpdateToEmptyCollection() {

		BookReference bookReference = new BookReference();

		bookReference.setIsbn("isbn");
		bookReference.setBookmarks(Arrays.asList(1, 2, 3, 4));

		template.insert(bookReference);

		bookReference.setBookmarks(Collections.emptyList());

		template.update(bookReference);

		BookReference loaded = template.selectOneById(bookReference.getIsbn(), BookReference.class);

		assertThat(loaded.getTitle()).isNull();
		assertThat(loaded.getBookmarks()).isNull();
	}

	@Test // DATACASS-206
	public void shouldUseSpecifiedColumnNamesForSingleEntityModifyingOperations() {

		UserToken userToken = new UserToken();
		userToken.setToken(UUIDs.startOf(System.currentTimeMillis()));
		userToken.setUserId(UUIDs.endOf(System.currentTimeMillis()));

		template.insert(userToken);

		userToken.setUserComment("comment");
		template.update(userToken);

		UserToken loaded = template.selectOneById(
				BasicMapId.id("userId", userToken.getUserId()).with("token", userToken.getToken()), UserToken.class);

		assertThat(loaded.getUserComment()).isEqualTo("comment");

		template.delete(userToken);

		UserToken loadAfterDelete = template.selectOneById(
				BasicMapId.id("userId", userToken.getUserId()).with("token", userToken.getToken()), UserToken.class);

		assertThat(loadAfterDelete).isNull();
	}
}
