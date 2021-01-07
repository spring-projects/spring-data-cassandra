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
import static org.springframework.data.cassandra.core.query.Criteria.*;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.Future;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.cql.AsyncCqlTemplate;
import org.springframework.data.cassandra.core.query.CassandraPageRequest;
import org.springframework.data.cassandra.core.query.Columns;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.cassandra.core.query.Update;
import org.springframework.data.cassandra.domain.User;
import org.springframework.data.cassandra.domain.UserToken;
import org.springframework.data.cassandra.repository.support.SchemaTestUtils;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTests;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;
import org.springframework.util.concurrent.ListenableFuture;

import com.datastax.oss.driver.api.core.uuid.Uuids;

/**
 * Integration tests for {@link AsyncCassandraTemplate}.
 *
 * @author Mark Paluch
 */
class AsyncCassandraTemplateIntegrationTests extends AbstractKeyspaceCreatingIntegrationTests {

	private AsyncCassandraTemplate template;

	@BeforeEach
	void setUp() {

		MappingCassandraConverter converter = new MappingCassandraConverter();
		CassandraTemplate cassandraTemplate = new CassandraTemplate(session, converter);
		template = new AsyncCassandraTemplate(new AsyncCqlTemplate(session), converter);
		prepareTemplate(template);

		SchemaTestUtils.potentiallyCreateTableFor(User.class, cassandraTemplate);
		SchemaTestUtils.potentiallyCreateTableFor(UserToken.class, cassandraTemplate);
		SchemaTestUtils.truncate(User.class, cassandraTemplate);
		SchemaTestUtils.truncate(UserToken.class, cassandraTemplate);
	}

	/**
	 * Post-process the {@link AsyncCassandraTemplate} before running the tests.
	 *
	 * @param template
	 */
	void prepareTemplate(AsyncCassandraTemplate template) {
		template.setUsePreparedStatements(false);
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

		getUninterruptibly(template.insert(token1));
		getUninterruptibly(template.insert(token2));

		Query query = Query.query(where("userId").is(token1.getUserId())).sort(Sort.by("token"));

		assertThat(getUninterruptibly(template.select(query, UserToken.class))).containsSequence(token1, token2);
	}

	@Test // DATACASS-343
	void shouldSelectOneByQuery() {

		UserToken token1 = new UserToken();
		token1.setUserId(Uuids.endOf(System.currentTimeMillis()));
		token1.setToken(Uuids.startOf(System.currentTimeMillis()));
		token1.setUserComment("foo");

		getUninterruptibly(template.insert(token1));

		Query query = Query.query(where("userId").is(token1.getUserId()));

		assertThat(getUninterruptibly(template.selectOne(query, UserToken.class))).isEqualTo(token1);
	}

	@Test // DATACASS-292
	void insertShouldInsertEntity() {

		User user = new User("heisenberg", "Walter", "White");

		assertThat(getUser(user.getId())).isNull();

		ListenableFuture<User> insert = template.insert(user);

		assertThat(getUninterruptibly(insert)).isEqualTo(user);
		assertThat(getUser(user.getId())).isEqualTo(user);
	}

	@Test // DATACASS-250
	void insertShouldCreateEntityWithLwt() {

		InsertOptions lwtOptions = InsertOptions.builder().withIfNotExists().build();

		User user = new User("heisenberg", "Walter", "White");

		ListenableFuture<EntityWriteResult<User>> inserted = template.insert(user, lwtOptions);

		assertThat(getUninterruptibly(inserted).wasApplied()).isTrue();
	}

	@Test // DATACASS-250
	void insertShouldNotUpdateEntityWithLwt() {

		InsertOptions lwtOptions = InsertOptions.builder().withIfNotExists().build();

		User user = new User("heisenberg", "Walter", "White");

		getUninterruptibly(template.insert(user, lwtOptions));

		user.setFirstname("Walter Hartwell");

		ListenableFuture<EntityWriteResult<User>> lwt = template.insert(user, lwtOptions);

		assertThat(getUninterruptibly(lwt).wasApplied()).isFalse();
		assertThat(getUser(user.getId()).getFirstname()).isEqualTo("Walter");
	}

	@Test // DATACASS-292
	void shouldInsertAndCountEntities() {

		User user = new User("heisenberg", "Walter", "White");

		User result = getUninterruptibly(template.insert(user));

		ListenableFuture<Long> count = template.count(User.class);
		assertThat(result).isSameAs(user);
		assertThat(getUninterruptibly(count)).isEqualTo(1L);
	}

	@Test // DATACASS-512
	void shouldInsertEntityAndCountByQuery() {

		User user = new User("heisenberg", "Walter", "White");

		getUninterruptibly(template.insert(user));

		assertThat(getUninterruptibly(template.count(Query.query(where("id").is("heisenberg")), User.class))).isOne();
		assertThat(getUninterruptibly(template.count(Query.query(where("id").is("foo")), User.class))).isZero();
	}

	@Test // DATACASS-512
	void shouldInsertEntityAndExistsByQuery() {

		User user = new User("heisenberg", "Walter", "White");

		getUninterruptibly(template.insert(user));

		assertThat(getUninterruptibly(template.exists(Query.query(where("id").is("heisenberg")), User.class))).isTrue();
		assertThat(getUninterruptibly(template.exists(Query.query(where("id").is("foo")), User.class))).isFalse();
	}

	@Test // DATACASS-292
	void updateShouldUpdateEntity() {

		User user = new User("heisenberg", "Walter", "White");
		getUninterruptibly(template.insert(user));

		user.setFirstname("Walter Hartwell");

		User updated = getUninterruptibly(template.update(user));

		assertThat(updated).isNotNull();
		assertThat(getUser(user.getId())).isEqualTo(user);
	}

	@Test // DATACASS-292
	void updateShouldNotCreateEntityWithLwt() {

		UpdateOptions lwtOptions = UpdateOptions.builder().withIfExists().build();

		User user = new User("heisenberg", "Walter", "White");

		ListenableFuture<EntityWriteResult<User>> lwt = template.update(user, lwtOptions);

		assertThat(getUninterruptibly(lwt).wasApplied()).isFalse();
		assertThat(getUser(user.getId())).isNull();
	}

	@Test // DATACASS-292
	void updateShouldUpdateEntityWithLwt() throws InterruptedException {

		UpdateOptions lwtOptions = UpdateOptions.builder().withIfExists().build();

		User user = new User("heisenberg", "Walter", "White");
		getUninterruptibly(template.insert(user));

		user.setFirstname("Walter Hartwell");

		ListenableFuture<EntityWriteResult<User>> updated = template.update(user, lwtOptions);

		assertThat(getUninterruptibly(updated).wasApplied()).isTrue();
		assertThat(getUninterruptibly(updated).getEntity()).isSameAs(user);
	}

	@Test // DATACASS-343
	void updateShouldUpdateEntityByQuery() {

		User user = new User("heisenberg", "Walter", "White");
		getUninterruptibly(template.insert(user));

		Query query = Query.query(where("id").is("heisenberg"));
		boolean result = getUninterruptibly(
				template.update(query, Update.empty().set("firstname", "Walter Hartwell"), User.class));
		assertThat(result).isTrue();

		assertThat(getUser(user.getId()).getFirstname()).isEqualTo("Walter Hartwell");
	}

	@Test // DATACASS-343
	void deleteByQueryShouldRemoveEntity() {

		User user = new User("heisenberg", "Walter", "White");
		getUninterruptibly(template.insert(user));

		Query query = Query.query(where("id").is("heisenberg"));
		assertThat(getUninterruptibly(template.delete(query, User.class))).isTrue();

		assertThat(getUser(user.getId())).isNull();
	}

	@Test // DATACASS-343
	void deleteColumnsByQueryShouldRemoveColumn() {

		User user = new User("heisenberg", "Walter", "White");
		getUninterruptibly(template.insert(user));

		Query query = Query.query(where("id").is("heisenberg")).columns(Columns.from("lastname"));

		assertThat(getUninterruptibly(template.delete(query, User.class))).isTrue();

		User loaded = getUser(user.getId());
		assertThat(loaded.getFirstname()).isEqualTo("Walter");
		assertThat(loaded.getLastname()).isNull();
	}

	@Test // DATACASS-292
	void deleteShouldRemoveEntity() {

		User user = new User("heisenberg", "Walter", "White");
		getUninterruptibly(template.insert(user));

		User deleted = getUninterruptibly(template.delete(user));

		assertThat(deleted).isNotNull();
		assertThat(getUser(user.getId())).isNull();
	}

	@Test // DATACASS-292
	void deleteByIdShouldRemoveEntity() {

		User user = new User("heisenberg", "Walter", "White");
		getUninterruptibly(template.insert(user));

		Boolean deleted = getUninterruptibly(template.deleteById(user.getId(), User.class));
		assertThat(deleted).isTrue();

		assertThat(getUser(user.getId())).isNull();
	}

	@Test // DATACASS-606
	void deleteShouldRemoveEntityWithLwt() {

		DeleteOptions lwtOptions = DeleteOptions.builder().withIfExists().build();

		User user = new User("heisenberg", "Walter", "White");
		getUninterruptibly(template.insert(user));

		assertThat(getUninterruptibly(template.delete(user, lwtOptions)).wasApplied()).isTrue();
	}

	@Test // DATACASS-606
	void deleteByQueryShouldRemoveEntityWithLwt() {

		DeleteOptions lwtOptions = DeleteOptions.builder().withIfExists().build();

		User user = new User("heisenberg", "Walter", "White");
		getUninterruptibly(template.insert(user));

		Query query = Query.query(where("id").is("heisenberg")).queryOptions(lwtOptions);
		assertThat(getUninterruptibly(template.delete(query, User.class))).isTrue();
		assertThat(getUninterruptibly(template.delete(query, User.class))).isFalse();
	}

	@Test // DATACASS-56
	void shouldPageRequests() {

		Set<String> expectedIds = new LinkedHashSet<>();

		for (int count = 0; count < 100; count++) {
			User user = new User("heisenberg" + count, "Walter", "White");
			expectedIds.add(user.getId());
			getUninterruptibly(template.insert(user));
		}

		Set<String> ids = new HashSet<>();

		Query query = Query.empty();

		Slice<User> slice = getUninterruptibly(
				template.slice(query.pageRequest(CassandraPageRequest.first(10)), User.class));

		int iterations = 0;

		do {

			iterations++;

			assertThat(slice).hasSize(10);

			slice.stream().map(User::getId).forEach(ids::add);

			if (slice.hasNext()) {
				slice = getUninterruptibly(template.slice(query.pageRequest(slice.nextPageable()), User.class));
			} else {
				break;
			}
		} while (!slice.getContent().isEmpty());

		assertThat(ids).containsAll(expectedIds);
		assertThat(iterations).isEqualTo(10);
	}

	private User getUser(String id) {
		return getUninterruptibly(template.selectOneById(id, User.class));
	}

	private static <T> T getUninterruptibly(Future<T> future) {

		try {
			return future.get();
		} catch (Exception cause) {
			throw new IllegalStateException(cause);
		}
	}
}
