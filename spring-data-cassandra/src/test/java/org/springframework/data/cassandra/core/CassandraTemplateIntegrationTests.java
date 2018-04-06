/*
 * Copyright 2016-2018 the original author or authors.
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
import static org.springframework.data.cassandra.core.query.Criteria.*;

import lombok.Data;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.cql.CqlTemplate;
import org.springframework.data.cassandra.core.mapping.BasicMapId;
import org.springframework.data.cassandra.core.query.CassandraPageRequest;
import org.springframework.data.cassandra.core.query.Columns;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.cassandra.core.query.Update;
import org.springframework.data.cassandra.domain.BookReference;
import org.springframework.data.cassandra.domain.User;
import org.springframework.data.cassandra.domain.UserToken;
import org.springframework.data.cassandra.repository.support.SchemaTestUtils;
import org.springframework.data.cassandra.support.CassandraVersion;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTest;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;
import org.springframework.data.util.Version;

import com.datastax.driver.core.utils.UUIDs;

/**
 * Integration tests for {@link CassandraTemplate}.
 *
 * @author Mark Paluch
 */
public class CassandraTemplateIntegrationTests extends AbstractKeyspaceCreatingIntegrationTest {

	static final Version CASSANDRA_3 = Version.parse("3.0");

	Version cassandraVersion;

	CassandraTemplate template;

	@Before
	public void setUp() {

		MappingCassandraConverter converter = new MappingCassandraConverter();
		converter.afterPropertiesSet();

		cassandraVersion = CassandraVersion.get(session);

		template = new CassandraTemplate(new CqlTemplate(session), converter);

		SchemaTestUtils.potentiallyCreateTableFor(User.class, template);
		SchemaTestUtils.potentiallyCreateTableFor(UserToken.class, template);
		SchemaTestUtils.potentiallyCreateTableFor(BookReference.class, template);
		SchemaTestUtils.potentiallyCreateTableFor(TimeClass.class, template);
		SchemaTestUtils.truncate(User.class, template);
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

		Query query = Query.query(where("userId").is(userToken.getUserId())).and(where("userComment").is("cook"))
				.withAllowFiltering();
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

		Query query = Query.query(where("userId").is(token1.getUserId())).sort(Sort.by("token"));
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

		Query query = Query.query(where("userId").is(token1.getUserId()));
		UserToken loaded = template.selectOne(query, UserToken.class);

		assertThat(loaded).isEqualTo(token1);
	}

	@Test // DATACASS-292
	public void insertShouldInsertEntity() {

		User user = new User("heisenberg", "Walter", "White");

		assertThat(template.selectOneById(user.getId(), User.class)).isNull();

		template.insert(user);

		assertThat(template.selectOneById(user.getId(), User.class)).isEqualTo(user);
	}

	@Test // DATACASS-250
	public void insertShouldCreateEntityWithLwt() {

		InsertOptions lwtOptions = InsertOptions.builder().withIfNotExists().build();

		User user = new User("heisenberg", "Walter", "White");

		template.insert(user, lwtOptions);

		assertThat(template.selectOneById(user.getId(), User.class)).isNotNull();
	}

	@Test // DATACASS-250
	public void insertShouldNotUpdateEntityWithLwt() {

		InsertOptions lwtOptions = InsertOptions.builder().withIfNotExists().build();

		User user = new User("heisenberg", "Walter", "White");

		template.insert(user, lwtOptions);

		user.setFirstname("Walter Hartwell");

		WriteResult lwt = template.insert(user, lwtOptions);

		assertThat(lwt.wasApplied()).isFalse();
		assertThat(template.selectOneById(user.getId(), User.class).getFirstname()).isEqualTo("Walter");
	}

	@Test // DATACASS-292
	public void shouldInsertAndCountEntities() {

		User user = new User("heisenberg", "Walter", "White");

		template.insert(user);

		long count = template.count(User.class);
		assertThat(count).isEqualTo(1L);
	}

	@Test // DATACASS-155
	public void shouldNotOverrideLaterMutation() {

		Instant now = LocalDateTime.now().atZone( ZoneId.systemDefault() ).toInstant();
		User user = new User("heisenberg", "Walter", "White");
		template.insert(user);

		// more recent mutation
		user.setFirstname("John");
		template.update(user);

		// previous mutation
		user.setFirstname("Greg");
		template.update(user, UpdateOptions.builder().timestamp(now.minusSeconds(120)).build());

		User loaded = template.selectOneById(user.getId(), User.class);
		assertThat(loaded.getFirstname()).isEqualTo("John");
	}

	@Test // DATACASS-512
	public void shouldInsertEntityAndCountByQuery() {

		User user = new User("heisenberg", "Walter", "White");

		template.insert(user);

		assertThat(template.count(Query.query(where("id").is("heisenberg")), User.class)).isOne();
		assertThat(template.count(Query.query(where("id").is("foo")), User.class)).isZero();
	}

	@Test // DATACASS-512
	public void shouldInsertEntityAndExistsByQuery() {

		User user = new User("heisenberg", "Walter", "White");

		template.insert(user);

		assertThat(template.exists(Query.query(where("id").is("heisenberg")), User.class)).isTrue();
		assertThat(template.exists(Query.query(where("id").is("foo")), User.class)).isFalse();
	}

	@Test // DATACASS-292
	public void updateShouldUpdateEntity() {

		User user = new User("heisenberg", "Walter", "White");
		template.insert(user);

		user.setFirstname("Walter Hartwell");

		template.update(user);

		assertThat(template.selectOneById(user.getId(), User.class)).isEqualTo(user);
	}

	@Test // DATACASS-292
	public void updateShouldNotCreateEntityWithLwt() {

		UpdateOptions lwtOptions = UpdateOptions.builder().withIfExists().build();

		User user = new User("heisenberg", "Walter", "White");

		WriteResult lwt = template.update(user, lwtOptions);

		assertThat(lwt.wasApplied()).isFalse();
		assertThat(template.selectOneById(user.getId(), User.class)).isNull();
	}

	@Test // DATACASS-292
	public void updateShouldUpdateEntityWithLwt() {

		UpdateOptions lwtOptions = UpdateOptions.builder().withIfExists().build();

		User user = new User("heisenberg", "Walter", "White");

		template.insert(user);

		user.setFirstname("Walter Hartwell");

		WriteResult lwt = template.update(user, lwtOptions);

		assertThat(lwt.wasApplied()).isTrue();
		assertThat(template.selectOneById(user.getId(), User.class).getFirstname()).isEqualTo("Walter Hartwell");
	}

	@Test // DATACASS-343
	public void updateShouldUpdateEntityByQuery() {

		User person = new User("heisenberg", "Walter", "White");
		template.insert(person);

		Query query = Query.query(where("id").is("heisenberg"));
		boolean result = template.update(query, Update.empty().set("firstname", "Walter Hartwell"), User.class);
		assertThat(result).isTrue();

		assertThat(template.selectOneById(person.getId(), User.class).getFirstname()).isEqualTo("Walter Hartwell");
	}

	@Test // DATACASS-343
	public void deleteByQueryShouldRemoveEntity() {

		User user = new User("heisenberg", "Walter", "White");
		template.insert(user);

		Query query = Query.query(where("id").is("heisenberg"));
		assertThat(template.delete(query, User.class)).isTrue();

		assertThat(template.selectOneById(user.getId(), User.class)).isNull();
	}

	@Test // DATACASS-343
	public void deleteColumnsByQueryShouldRemoveColumn() {

		User user = new User("heisenberg", "Walter", "White");
		template.insert(user);

		Query query = Query.query(where("id").is("heisenberg")).columns(Columns.from("lastname"));

		assertThat(template.delete(query, User.class)).isTrue();

		User loaded = template.selectOneById(user.getId(), User.class);
		assertThat(loaded.getFirstname()).isEqualTo("Walter");
		assertThat(loaded.getLastname()).isNull();
	}

	@Test // DATACASS-292
	public void deleteShouldRemoveEntity() {

		User user = new User("heisenberg", "Walter", "White");
		template.insert(user);

		template.delete(user);

		assertThat(template.selectOneById(user.getId(), User.class)).isNull();
	}

	@Test // DATACASS-292
	public void deleteByIdShouldRemoveEntity() {

		User user = new User("heisenberg", "Walter", "White");
		template.insert(user);

		Boolean deleted = template.deleteById(user.getId(), User.class);
		assertThat(deleted).isTrue();

		assertThat(template.selectOneById(user.getId(), User.class)).isNull();
	}

	@Test // DATACASS-182
	public void stream() {

		User user = new User("heisenberg", "Walter", "White");
		template.insert(user);

		Stream<User> stream = template.stream("SELECT * FROM users", User.class);

		assertThat(stream.collect(Collectors.toList())).hasSize(1).contains(user);
	}

	@Test // DATACASS-343
	public void streamByQuery() {

		User person = new User("heisenberg", "Walter", "White");
		template.insert(person);

		Query query = Query.query(where("id").is("heisenberg"));

		Stream<User> stream = template.stream(query, User.class);

		assertThat(stream.collect(Collectors.toList())).hasSize(1).contains(person);
	}

	@Test // DATACASS-182
	public void updateShouldRemoveFields() {

		User user = new User("heisenberg", "Walter", "White");

		template.insert(user);

		user.setFirstname(null);
		template.update(user);

		User loaded = template.selectOneById(user.getId(), User.class);

		assertThat(loaded.getFirstname()).isNull();
		assertThat(loaded.getId()).isEqualTo("heisenberg");
	}

	@Test // DATACASS-182, DATACASS-420
	public void insertShouldNotRemoveFields() {

		User user = new User("heisenberg", "Walter", "White");

		template.insert(user);

		user.setFirstname(null);
		template.insert(user);

		User loaded = template.selectOneById(user.getId(), User.class);

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

	@Test // DATACASS-56
	public void shouldPageRequests() {

		Set<String> expectedIds = new LinkedHashSet<>();

		for (int count = 0; count < 100; count++) {
			User user = new User("heisenberg" + count, "Walter", "White");
			expectedIds.add(user.getId());
			template.insert(user);
		}

		Set<String> ids = new HashSet<>();

		Query query = Query.empty().pageRequest(CassandraPageRequest.first(10));

		Slice<User> slice = template.slice(query, User.class);

		int iterations = 0;

		do {

			iterations++;

			assertThat(slice).hasSize(10);

			slice.stream().map(User::getId).forEach(ids::add);

			if (slice.hasNext()) {
				slice = template.slice(query.pageRequest(slice.nextPageable()), User.class);
			} else {
				break;
			}
		} while (!slice.getContent().isEmpty());

		assertThat(ids).containsAll(expectedIds);
		assertThat(iterations).isEqualTo(10);
	}

	@Data
	static class TimeClass {

		@Id LocalTime id;
		LocalTime bar;
	}
}
