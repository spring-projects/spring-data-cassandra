/*
 * Copyright 2016-2021 the original author or authors.
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
package org.springframework.data.cassandra.core;

import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.junit.Assume.*;
import static org.springframework.data.cassandra.core.query.Criteria.*;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.CassandraInvalidQueryException;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.cql.CqlTemplate;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.cql.QueryOptions;
import org.springframework.data.cassandra.core.mapping.BasicMapId;
import org.springframework.data.cassandra.core.mapping.Embedded;
import org.springframework.data.cassandra.core.mapping.PrimaryKey;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyClass;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.core.mapping.SimpleUserTypeResolver;
import org.springframework.data.cassandra.core.mapping.Table;
import org.springframework.data.cassandra.core.mapping.UserDefinedType;
import org.springframework.data.cassandra.core.query.CassandraPageRequest;
import org.springframework.data.cassandra.core.query.Columns;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.cassandra.core.query.Update;
import org.springframework.data.cassandra.domain.BookReference;
import org.springframework.data.cassandra.domain.User;
import org.springframework.data.cassandra.domain.UserToken;
import org.springframework.data.cassandra.repository.support.SchemaTestUtils;
import org.springframework.data.cassandra.support.CassandraVersion;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTests;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;
import org.springframework.data.util.Version;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.uuid.Uuids;

/**
 * Integration tests for {@link CassandraTemplate}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Tomasz Lelek
 */
class CassandraTemplateIntegrationTests extends AbstractKeyspaceCreatingIntegrationTests {

	private static final Version CASSANDRA_3 = Version.parse("3.0");
	private static final Version CASSANDRA_4 = Version.parse("4.0");

	private Version cassandraVersion;

	private CassandraTemplate template;

	@BeforeEach
	void setUp() {

		MappingCassandraConverter converter = new MappingCassandraConverter();
		converter.setUserTypeResolver(new SimpleUserTypeResolver(session, CqlIdentifier.fromCql(keyspace)));
		converter.afterPropertiesSet();

		cassandraVersion = CassandraVersion.get(session);

		template = new CassandraTemplate(new CqlTemplate(session), converter);

		prepareTemplate(template);

		SchemaTestUtils.potentiallyCreateTableFor(User.class, template);
		SchemaTestUtils.potentiallyCreateTableFor(UserToken.class, template);
		SchemaTestUtils.potentiallyCreateTableFor(BookReference.class, template);
		SchemaTestUtils.potentiallyCreateTableFor(TimeClass.class, template);
		SchemaTestUtils.potentiallyCreateTableFor(TypeWithCompositeKey.class, template);
		SchemaTestUtils.potentiallyCreateTableFor(WithNullableEmbeddedType.class, template);
		SchemaTestUtils.potentiallyCreateTableFor(WithEmptyEmbeddedType.class, template);
		SchemaTestUtils.potentiallyCreateTableFor(WithPrefixedNullableEmbeddedType.class, template);
		SchemaTestUtils.createTableAndTypes(OuterWithNullableEmbeddedType.class, template);
		SchemaTestUtils.createTableAndTypes(OuterWithPrefixedNullableEmbeddedType.class, template);
		SchemaTestUtils.createTableAndTypes(WithMappedUdtList.class, template);
		SchemaTestUtils.truncate(User.class, template);
		SchemaTestUtils.truncate(UserToken.class, template);
		SchemaTestUtils.truncate(BookReference.class, template);
		SchemaTestUtils.truncate(TypeWithCompositeKey.class, template);
		SchemaTestUtils.truncate(WithNullableEmbeddedType.class, template);
		SchemaTestUtils.truncate(WithEmptyEmbeddedType.class, template);
		SchemaTestUtils.truncate(WithPrefixedNullableEmbeddedType.class, template);
		SchemaTestUtils.truncate(OuterWithNullableEmbeddedType.class, template);
		SchemaTestUtils.truncate(OuterWithPrefixedNullableEmbeddedType.class, template);
		SchemaTestUtils.truncate(WithMappedUdtList.class, template);
	}

	/**
	 * Post-process the {@link CassandraTemplate} before running the tests.
	 *
	 * @param template
	 */
	void prepareTemplate(CassandraTemplate template) {

	}

	@Test // DATACASS-343
	void shouldSelectByQueryWithAllowFiltering() {

		assumeTrue(cassandraVersion.isGreaterThanOrEqualTo(CASSANDRA_3));

		UserToken userToken = new UserToken();
		userToken.setUserId(Uuids.endOf(System.currentTimeMillis()));
		userToken.setToken(Uuids.startOf(System.currentTimeMillis()));
		userToken.setUserComment("cook");

		template.insert(userToken);

		Query query = Query.query(where("userId").is(userToken.getUserId())).and(where("userComment").is("cook"))
				.withAllowFiltering();
		UserToken loaded = template.selectOne(query, UserToken.class);

		assertThat(loaded).isNotNull();
		assertThat(loaded.getUserComment()).isEqualTo("cook");
	}

	@Test // DATACASS-343
	void shouldSelectByQueryWithSorting() {

		UserToken token1 = new UserToken();
		token1.setUserId(Uuids.endOf(System.currentTimeMillis()));
		token1.setToken(Uuids.startOf(System.currentTimeMillis()));
		token1.setUserComment("foo");

		UserToken token2 = new UserToken();
		token2.setUserId(token1.getUserId());
		token2.setToken(Uuids.endOf(System.currentTimeMillis() + 100));
		token2.setUserComment("bar");

		template.insert(token1);
		template.insert(token2);

		Query query = Query.query(where("userId").is(token1.getUserId())).sort(Sort.by("token"));
		List<UserToken> loaded = template.select(query, UserToken.class);

		assertThat(loaded).containsSequence(token1, token2);
	}

	@Test // DATACASS-638
	void shouldSelectProjection() {

		User user = new User("heisenberg", "Walter", "White");

		template.insert(user);

		Query query = Query.query(where("id").is(user.getId())).columns(Columns.empty().include("id").include("firstname"));
		List<User> loaded = template.select(query, User.class);

		assertThat(loaded).hasSize(1);

		User loadedUser = loaded.get(0);

		assertThat(loadedUser.getFirstname()).isNotNull();
		assertThat(loadedUser.getLastname()).isNull();
	}

	@Test // DATACASS-638
	void shouldSelectProjectionWithCompositeKey() {

		CompositeKey key = new CompositeKey("Walter", "White");
		TypeWithCompositeKey user = new TypeWithCompositeKey(key, "comment");

		template.insert(user);

		Query query = Query.empty().columns(Columns.empty().include("firstname").include("comment"));
		List<TypeWithCompositeKey> loaded = template.select(query, TypeWithCompositeKey.class);

		assertThat(loaded).hasSize(1);

		TypeWithCompositeKey loadedEntity = loaded.get(0);

		assertThat(loadedEntity.getKey()).isNotNull();
		assertThat(loadedEntity.getKey().getFirstname()).isNotNull();
		assertThat(loadedEntity.getKey().getLastname()).isNull();
		assertThat(loadedEntity.getComment()).isNotNull();
	}

	@Test // DATACASS-828
	void shouldSelectClosedProjectionWithCompositeKey() {

		CompositeKey key = new CompositeKey("Walter", "White");
		TypeWithCompositeKey user = new TypeWithCompositeKey(key, "comment");

		template.insert(user);

		WithCompositeKeyProjection loaded = template.query(TypeWithCompositeKey.class).as(WithCompositeKeyProjection.class)
				.firstValue();
		assertThat(loaded).isNotNull();

		assertThat(loaded.getKey()).isEqualTo(key);
	}

	@Test // DATACASS-343
	void shouldSelectOneByQuery() {

		UserToken token1 = new UserToken();
		token1.setUserId(Uuids.endOf(System.currentTimeMillis()));
		token1.setToken(Uuids.startOf(System.currentTimeMillis()));
		token1.setUserComment("foo");

		template.insert(token1);

		Query query = Query.query(where("userId").is(token1.getUserId()));
		UserToken loaded = template.selectOne(query, UserToken.class);

		assertThat(loaded).isEqualTo(token1);
	}

	@Test // DATACASS-292, DATACASS-573
	void insertShouldInsertEntity() {

		User user = new User("heisenberg", "Walter", "White");

		assertThat(template.selectOneById(user.getId(), User.class)).isNull();

		User result = template.insert(user);

		assertThat(result).isSameAs(user);
		assertThat(template.selectOneById(user.getId(), User.class)).isEqualTo(user);
	}

	@Test // DATACASS-250, DATACASS-573
	void insertShouldCreateEntityWithLwt() {

		InsertOptions lwtOptions = InsertOptions.builder().withIfNotExists().build();

		User user = new User("heisenberg", "Walter", "White");

		EntityWriteResult<User> result = template.insert(user, lwtOptions);

		assertThat(result.getEntity()).isSameAs(user);
		assertThat(template.selectOneById(user.getId(), User.class)).isNotNull();
	}

	@Test // DATACASS-250, DATACASS-573
	void insertShouldNotUpdateEntityWithLwt() {

		InsertOptions lwtOptions = InsertOptions.builder().withIfNotExists().build();

		User user = new User("heisenberg", "Walter", "White");

		template.insert(user, lwtOptions);

		user.setFirstname("Walter Hartwell");

		EntityWriteResult<User> lwt = template.insert(user, lwtOptions);

		assertThat(lwt.wasApplied()).isFalse();
		assertThat(template.selectOneById(user.getId(), User.class).getFirstname()).isEqualTo("Walter");
	}

	@Test // DATACASS-292
	void shouldInsertAndCountEntities() {

		User user = new User("heisenberg", "Walter", "White");

		template.insert(user);

		long count = template.count(User.class);
		assertThat(count).isEqualTo(1L);
	}

	@Test // DATACASS-155
	void shouldNotOverrideLaterMutation() {

		Instant now = LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant();
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
	void shouldInsertEntityAndCountByQuery() {

		User user = new User("heisenberg", "Walter", "White");

		template.insert(user);

		assertThat(template.count(Query.query(where("id").is("heisenberg")), User.class)).isOne();
		assertThat(template.count(Query.query(where("id").is("foo")), User.class)).isZero();
	}

	@Test // DATACASS-512
	void shouldInsertEntityAndExistsByQuery() {

		User user = new User("heisenberg", "Walter", "White");

		template.insert(user);

		assertThat(template.exists(Query.query(where("id").is("heisenberg")), User.class)).isTrue();
		assertThat(template.exists(Query.query(where("id").is("foo")), User.class)).isFalse();
	}

	@Test // DATACASS-292, DATACASS-573
	void updateShouldUpdateEntity() {

		User user = new User("heisenberg", "Walter", "White");
		template.insert(user);

		user.setFirstname("Walter Hartwell");

		User result = template.update(user);

		assertThat(result).isSameAs(user);
		assertThat(template.selectOneById(user.getId(), User.class)).isEqualTo(user);
	}

	@Test // DATACASS-292
	void updateShouldNotCreateEntityWithLwt() {

		UpdateOptions lwtOptions = UpdateOptions.builder().withIfExists().build();

		User user = new User("heisenberg", "Walter", "White");

		WriteResult lwt = template.update(user, lwtOptions);

		assertThat(lwt.wasApplied()).isFalse();
		assertThat(template.selectOneById(user.getId(), User.class)).isNull();
	}

	@Test // DATACASS-292
	void updateShouldUpdateEntityWithLwt() {

		UpdateOptions lwtOptions = UpdateOptions.builder().withIfExists().build();

		User user = new User("heisenberg", "Walter", "White");

		template.insert(user);

		user.setFirstname("Walter Hartwell");

		WriteResult lwt = template.update(user, lwtOptions);

		assertThat(lwt.wasApplied()).isTrue();
	}

	@Test // DATACASS-343
	void updateShouldUpdateEntityByQuery() {

		User person = new User("heisenberg", "Walter", "White");
		template.insert(person);

		Query query = Query.query(where("id").is("heisenberg"));
		boolean result = template.update(query, Update.empty().set("firstname", "Walter Hartwell"), User.class);
		assertThat(result).isTrue();

		assertThat(template.selectOneById(person.getId(), User.class).getFirstname()).isEqualTo("Walter Hartwell");
	}

	@Test // DATACASS-343
	void deleteByQueryShouldRemoveEntity() {

		User user = new User("heisenberg", "Walter", "White");
		template.insert(user);

		Query query = Query.query(where("id").is("heisenberg"));
		assertThat(template.delete(query, User.class)).isTrue();

		assertThat(template.selectOneById(user.getId(), User.class)).isNull();
	}

	@Test // DATACASS-343
	void deleteColumnsByQueryShouldRemoveColumn() {

		User user = new User("heisenberg", "Walter", "White");
		template.insert(user);

		Query query = Query.query(where("id").is("heisenberg")).columns(Columns.from("lastname"));

		assertThat(template.delete(query, User.class)).isTrue();

		User loaded = template.selectOneById(user.getId(), User.class);
		assertThat(loaded.getFirstname()).isEqualTo("Walter");
		assertThat(loaded.getLastname()).isNull();
	}

	@Test // DATACASS-292
	void deleteShouldRemoveEntity() {

		User user = new User("heisenberg", "Walter", "White");
		template.insert(user);

		template.delete(user);

		assertThat(template.selectOneById(user.getId(), User.class)).isNull();
	}

	@Test // DATACASS-292
	void deleteByIdShouldRemoveEntity() {

		User user = new User("heisenberg", "Walter", "White");
		template.insert(user);

		Boolean deleted = template.deleteById(user.getId(), User.class);
		assertThat(deleted).isTrue();

		assertThat(template.selectOneById(user.getId(), User.class)).isNull();
	}

	@Test // DATACASS-606
	void deleteShouldRemoveEntityWithLwt() {

		DeleteOptions lwtOptions = DeleteOptions.builder().withIfExists().build();

		User user = new User("heisenberg", "Walter", "White");
		template.insert(user);

		assertThat(template.delete(user, lwtOptions).wasApplied()).isTrue();
	}

	@Test // DATACASS-606
	void deleteByQueryShouldRemoveEntityWithLwt() {

		DeleteOptions lwtOptions = DeleteOptions.builder().withIfExists().build();

		User user = new User("heisenberg", "Walter", "White");
		template.insert(user);

		Query query = Query.query(where("id").is("heisenberg")).queryOptions(lwtOptions);
		assertThat(template.delete(query, User.class)).isTrue();
	}

	@Test // DATACASS-767
	void selectByQueryWithKeyspaceShouldRetrieveData() {

		assumeThat(cassandraVersion.isGreaterThanOrEqualTo(CASSANDRA_4)).isTrue();

		QueryOptions queryOptions = QueryOptions.builder().keyspace(CqlIdentifier.fromCql(keyspace)).build();

		User user = new User("heisenberg", "Walter", "White");
		template.insert(user);

		Query query = Query.query(where("id").is("heisenberg")).queryOptions(queryOptions);
		assertThat(template.select(query, User.class)).isNotEmpty();
	}

	@Test // DATACASS-767
	void selectByQueryWithNonExistingKeyspaceShouldThrowThatKeyspaceDoesNotExists() {

		assumeThat(cassandraVersion.isGreaterThanOrEqualTo(CASSANDRA_4)).isTrue();

		QueryOptions queryOptions = QueryOptions.builder().keyspace(CqlIdentifier.fromCql("non_existing")).build();

		User user = new User("heisenberg", "Walter", "White");
		template.insert(user);

		Query query = Query.query(where("id").is("heisenberg")).queryOptions(queryOptions);
		assertThatThrownBy(() -> assertThat(template.select(query, User.class)).isEmpty())
				.isInstanceOf(CassandraInvalidQueryException.class)
				.hasMessageContaining("Keyspace 'non_existing' does not exist");
	}

	@Test // DATACASS-182
	void stream() {

		User user = new User("heisenberg", "Walter", "White");
		template.insert(user);

		Stream<User> stream = template.stream("SELECT * FROM users", User.class);

		assertThat(stream.collect(Collectors.toList())).hasSize(1).contains(user);
	}

	@Test // DATACASS-343
	void streamByQuery() {

		User person = new User("heisenberg", "Walter", "White");
		template.insert(person);

		Query query = Query.query(where("id").is("heisenberg"));

		Stream<User> stream = template.stream(query, User.class);

		assertThat(stream.collect(Collectors.toList())).hasSize(1).contains(person);
	}

	@Test // DATACASS-182
	void updateShouldRemoveFields() {

		User user = new User("heisenberg", "Walter", "White");

		template.insert(user);

		user.setFirstname(null);
		template.update(user);

		User loaded = template.selectOneById(user.getId(), User.class);

		assertThat(loaded.getFirstname()).isNull();
		assertThat(loaded.getId()).isEqualTo("heisenberg");
	}

	@Test // DATACASS-182, DATACASS-420
	void insertShouldNotRemoveFields() {

		User user = new User("heisenberg", "Walter", "White");

		template.insert(user);

		user.setFirstname(null);
		template.insert(user);

		User loaded = template.selectOneById(user.getId(), User.class);

		assertThat(loaded.getFirstname()).isEqualTo("Walter");
		assertThat(loaded.getId()).isEqualTo("heisenberg");
	}

	@Test // DATACASS-182
	void insertAndUpdateToEmptyCollection() {

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

	@Test // #1007
	void updateCollection() {

		BookReference bookReference = new BookReference();

		bookReference.setIsbn("isbn");
		bookReference.setBookmarks(Arrays.asList(1, 2, 3, 4));
		bookReference.setReferences(new LinkedHashSet<>(Arrays.asList("one", "two", "three")));

		Map<String, String> credits = new LinkedHashMap<>();
		credits.put("hello", "world");
		credits.put("other", "world");
		credits.put("external", "place");

		bookReference.setCredits(credits);

		template.insert(bookReference);

		Query query = Query.query(where("isbn").is(bookReference.getIsbn()));

		Update update = Update.empty().removeFrom("bookmarks").values(3, 4).removeFrom("references").values("one", "three")
				.removeFrom("credits").values("hello", "other", "place");

		template.update(query, update, BookReference.class);

		BookReference loaded = template.selectOneById(bookReference.getIsbn(), BookReference.class);

		assertThat(loaded.getBookmarks()).containsOnly(1, 2);
		assertThat(loaded.getReferences()).containsOnly("two");
		assertThat(loaded.getCredits()).containsOnlyKeys("external");
	}

	@Test // DATACASS-206
	void shouldUseSpecifiedColumnNamesForSingleEntityModifyingOperations() {

		UserToken userToken = new UserToken();
		userToken.setToken(Uuids.startOf(System.currentTimeMillis()));
		userToken.setUserId(Uuids.endOf(System.currentTimeMillis()));

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
	void shouldPageRequests() {

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

	@Test // DATACASS-167
	void shouldSaveAndReadPrefixedEmbeddedCorrectly() {

		WithPrefixedNullableEmbeddedType entity = new WithPrefixedNullableEmbeddedType();
		entity.id = "id-1";
		entity.nested = new EmbeddedWithSimpleTypes();
		entity.nested.firstname = "fn";
		entity.nested.age = 30;

		template.insert(WithPrefixedNullableEmbeddedType.class).one(entity);

		WithPrefixedNullableEmbeddedType target = template.selectOne(Query.query(where("id").is("id-1")),
				WithPrefixedNullableEmbeddedType.class);
		assertThat(target).isEqualTo(entity);
	}

	@Test // DATACASS-167
	void shouldSaveAndReadEmbeddedCorrectly() {

		WithNullableEmbeddedType entity = new WithNullableEmbeddedType();
		entity.id = "id-1";
		entity.nested = new EmbeddedWithSimpleTypes();
		entity.nested.firstname = "fn";
		entity.nested.age = 30;

		template.insert(WithNullableEmbeddedType.class).one(entity);

		WithNullableEmbeddedType target = template.selectOne(Query.query(where("id").is("id-1")),
				WithNullableEmbeddedType.class);
		assertThat(target).isEqualTo(entity);
	}

	@Test // DATACASS-167
	void shouldSaveAndReadNullableEmbeddedCorrectly() {

		WithNullableEmbeddedType entity = new WithNullableEmbeddedType();
		entity.id = "id-1";
		entity.nested = null;

		template.insert(WithNullableEmbeddedType.class).one(entity);

		WithNullableEmbeddedType target = template.selectOne(Query.query(where("id").is("id-1")),
				WithNullableEmbeddedType.class);
		assertThat(target.id).isEqualTo("id-1");
		assertThat(target.nested).isNull();
	}

	@Test // DATACASS-167
	void shouldSaveAndReadEmptyEmbeddedCorrectly() {

		WithEmptyEmbeddedType entity = new WithEmptyEmbeddedType();
		entity.id = "id-1";
		entity.nested = null;

		template.insert(WithEmptyEmbeddedType.class).one(entity);

		WithEmptyEmbeddedType target = template.selectOne(Query.query(where("id").is("id-1")), WithEmptyEmbeddedType.class);
		assertThat(target.id).isEqualTo("id-1");
		assertThat(target.nested).isNotNull();
	}

	@Test // DATACASS-167
	void shouldSaveAndReadEmbeddedUDTCorrectly() {

		OuterWithNullableEmbeddedType entity = new OuterWithNullableEmbeddedType();
		entity.id = "id-1";
		entity.udtValue = new UDTWithNullableEmbeddedType();
		entity.udtValue.value = "value-1";
		entity.udtValue.nested = new EmbeddedWithSimpleTypes();
		entity.udtValue.nested.firstname = "fn";
		entity.udtValue.nested.age = 30;

		template.insert(OuterWithNullableEmbeddedType.class).one(entity);

		OuterWithNullableEmbeddedType target = template.selectOne(Query.query(where("id").is("id-1")),
				OuterWithNullableEmbeddedType.class);
		assertThat(target).isEqualTo(entity);
	}

	@Test // DATACASS-167
	void shouldSaveAndReadPrefixedUdtEmbeddedCorrectly() {

		OuterWithPrefixedNullableEmbeddedType entity = new OuterWithPrefixedNullableEmbeddedType();
		entity.id = "id-1";
		entity.udtValue = new UDTWithPrefixedNullableEmbeddedType();
		entity.udtValue.value = "value-1";
		entity.udtValue.nested = new EmbeddedWithSimpleTypes();
		entity.udtValue.nested.firstname = "fn";
		entity.udtValue.nested.age = 30;

		template.insert(OuterWithPrefixedNullableEmbeddedType.class).one(entity);

		OuterWithPrefixedNullableEmbeddedType target = template.selectOne(Query.query(where("id").is("id-1")),
				OuterWithPrefixedNullableEmbeddedType.class);
		assertThat(target).isEqualTo(entity);
	}

	@Test // DATACASS-167
	void shouldSaveAndReadNullEmbeddedUDTCorrectly() {

		OuterWithNullableEmbeddedType entity = new OuterWithNullableEmbeddedType();
		entity.id = "id-1";
		entity.udtValue = new UDTWithNullableEmbeddedType();
		entity.udtValue.value = "value-1";
		entity.udtValue.nested = null;

		template.insert(OuterWithNullableEmbeddedType.class).one(entity);

		OuterWithNullableEmbeddedType target = template.selectOne(Query.query(where("id").is("id-1")),
				OuterWithNullableEmbeddedType.class);
		assertThat(target).isEqualTo(entity);
	}

	@Test // DATACASS-829
	void shouldPartiallyUpdateListOfMappedUdt() {

		WithMappedUdtList entity = new WithMappedUdtList();
		entity.id = "id-1";
		entity.mappedUdts = Arrays.asList(new MappedUdt("one"), new MappedUdt("two"), new MappedUdt("three"));

		template.insert(entity);

		Update update = Update.empty().set("mappedUdts").atIndex(1).to(new MappedUdt("replacement"));

		template.update(Query.query(where("id").is("id-1")), update, WithMappedUdtList.class);

		WithMappedUdtList updated = template.selectOne(Query.query(where("id").is("id-1")), WithMappedUdtList.class);
		assertThat(updated.getMappedUdts()).extracting(MappedUdt::getName).containsExactly("one", "replacement", "three");
	}

	@Data
	@UserDefinedType
	static class MappedUdt {

		final String name;
	}

	@Data
	@Table
	static class WithMappedUdtList {

		@Id String id;

		List<MappedUdt> mappedUdts;
	}

	@Data
	static class TimeClass {

		@Id LocalTime id;
		LocalTime bar;
	}

	@Data
	@AllArgsConstructor
	static class TypeWithCompositeKey {
		@PrimaryKey CompositeKey key;
		String comment;
	}

	interface WithCompositeKeyProjection {

		CompositeKey getKey();
	}

	@Data
	@PrimaryKeyClass
	@AllArgsConstructor
	static class CompositeKey {

		@PrimaryKeyColumn(type = PrimaryKeyType.PARTITIONED) String firstname;
		@PrimaryKeyColumn(type = PrimaryKeyType.PARTITIONED) String lastname;
	}

	@Data
	static class WithNullableEmbeddedType {

		@Id String id;

		@Embedded.Nullable EmbeddedWithSimpleTypes nested;
	}

	@Data
	static class WithPrefixedNullableEmbeddedType {

		@Id String id;

		@Embedded.Nullable(prefix = "prefix") EmbeddedWithSimpleTypes nested;
	}

	@Data
	static class WithEmptyEmbeddedType {

		@Id String id;

		@Embedded.Empty EmbeddedWithSimpleTypes nested;
	}

	@Data
	static class EmbeddedWithSimpleTypes {

		String firstname;
		Integer age;
	}

	@Data
	static class OuterWithNullableEmbeddedType {

		@Id String id;

		UDTWithNullableEmbeddedType udtValue;
	}

	@Data
	static class OuterWithPrefixedNullableEmbeddedType {

		@Id String id;

		UDTWithPrefixedNullableEmbeddedType udtValue;
	}

	@UserDefinedType
	@Data
	static class UDTWithNullableEmbeddedType {

		String value;

		@Embedded.Nullable EmbeddedWithSimpleTypes nested;
	}

	@UserDefinedType
	@Data
	static class UDTWithPrefixedNullableEmbeddedType {

		String value;

		@Embedded.Nullable(prefix = "prefix") EmbeddedWithSimpleTypes nested;
	}

}
