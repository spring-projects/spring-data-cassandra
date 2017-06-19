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

import java.util.concurrent.Future;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.cql.AsyncCqlTemplate;
import org.springframework.data.cassandra.core.query.Columns;
import org.springframework.data.cassandra.core.query.Criteria;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.cassandra.core.query.Update;
import org.springframework.data.cassandra.domain.User;
import org.springframework.data.cassandra.domain.UserToken;
import org.springframework.data.cassandra.repository.support.SchemaTestUtils;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTest;
import org.springframework.data.domain.Sort;
import org.springframework.util.concurrent.ListenableFuture;

import com.datastax.driver.core.utils.UUIDs;

/**
 * Integration tests for {@link AsyncCassandraTemplate}.
 *
 * @author Mark Paluch
 */
public class AsyncCassandraTemplateIntegrationTests extends AbstractKeyspaceCreatingIntegrationTest {

	private AsyncCassandraTemplate template;

	@Before
	public void setUp() throws Exception {

		MappingCassandraConverter converter = new MappingCassandraConverter();
		CassandraTemplate cassandraTemplate = new CassandraTemplate(session, converter);
		template = new AsyncCassandraTemplate(new AsyncCqlTemplate(session), converter);

		SchemaTestUtils.potentiallyCreateTableFor(User.class, cassandraTemplate);
		SchemaTestUtils.potentiallyCreateTableFor(UserToken.class, cassandraTemplate);
		SchemaTestUtils.truncate(User.class, cassandraTemplate);
		SchemaTestUtils.truncate(UserToken.class, cassandraTemplate);
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

		getUninterruptibly(template.insert(token1));
		getUninterruptibly(template.insert(token2));

		Query query = Query.query(Criteria.where("userId").is(token1.getUserId())).sort(Sort.by("token"));

		assertThat(getUninterruptibly(template.select(query, UserToken.class))).containsSequence(token1, token2);
	}

	@Test // DATACASS-343
	public void shouldSelectOneByQuery() {

		UserToken token1 = new UserToken();
		token1.setUserId(UUIDs.endOf(System.currentTimeMillis()));
		token1.setToken(UUIDs.startOf(System.currentTimeMillis()));
		token1.setUserComment("foo");

		getUninterruptibly(template.insert(token1));

		Query query = Query.query(Criteria.where("userId").is(token1.getUserId()));

		assertThat(getUninterruptibly(template.selectOne(query, UserToken.class))).isEqualTo(token1);
	}

	@Test // DATACASS-292
	public void insertShouldInsertEntity() {

		User user = new User("heisenberg", "Walter", "White");

		assertThat(getUser(user.getId())).isNull();

		ListenableFuture<User> insert = template.insert(user);

		assertThat(getUninterruptibly(insert)).isEqualTo(user);
		assertThat(getUser(user.getId())).isEqualTo(user);
	}

	@Test // DATACASS-250
	public void insertShouldCreateEntityWithLwt() {

		InsertOptions lwtOptions = InsertOptions.builder().withIfNotExists().build();

		User user = new User("heisenberg", "Walter", "White");

		ListenableFuture<WriteResult> inserted = template.insert(user, lwtOptions);

		assertThat(getUninterruptibly(inserted).wasApplied()).isTrue();
	}

	@Test // DATACASS-250
	public void insertShouldNotUpdateEntityWithLwt() {

		InsertOptions lwtOptions = InsertOptions.builder().withIfNotExists().build();

		User user = new User("heisenberg", "Walter", "White");

		getUninterruptibly(template.insert(user, lwtOptions));

		user.setFirstname("Walter Hartwell");

		ListenableFuture<WriteResult> lwt = template.insert(user, lwtOptions);

		assertThat(getUninterruptibly(lwt).wasApplied()).isFalse();
		assertThat(getUser(user.getId()).getFirstname()).isEqualTo("Walter");
	}

	@Test // DATACASS-292
	public void shouldInsertAndCountEntities() throws Exception {

		User user = new User("heisenberg", "Walter", "White");

		getUninterruptibly(template.insert(user));

		ListenableFuture<Long> count = template.count(User.class);
		assertThat(getUninterruptibly(count)).isEqualTo(1L);
	}

	@Test // DATACASS-292
	public void updateShouldUpdateEntity() throws Exception {

		User user = new User("heisenberg", "Walter", "White");
		getUninterruptibly(template.insert(user));

		user.setFirstname("Walter Hartwell");

		User updated = getUninterruptibly(template.update(user));

		assertThat(updated).isNotNull();
		assertThat(getUser(user.getId())).isEqualTo(user);
	}

	@Test // DATACASS-292
	public void updateShouldNotCreateEntityWithLwt() {

		UpdateOptions lwtOptions = UpdateOptions.builder().withIfExists().build();

		User user = new User("heisenberg", "Walter", "White");

		ListenableFuture<WriteResult> lwt = template.update(user, lwtOptions);

		assertThat(getUninterruptibly(lwt).wasApplied()).isFalse();
		assertThat(getUser(user.getId())).isNull();
	}

	@Test // DATACASS-292
	public void updateShouldUpdateEntityWithLwt() {

		UpdateOptions lwtOptions = UpdateOptions.builder().withIfExists().build();

		User user = new User("heisenberg", "Walter", "White");
		getUninterruptibly(template.insert(user));

		user.setFirstname("Walter Hartwell");

		ListenableFuture<WriteResult> updated = template.update(user, lwtOptions);

		assertThat(getUninterruptibly(updated).wasApplied()).isTrue();
		assertThat(getUser(user.getId()).getFirstname()).isEqualTo("Walter Hartwell");
	}

	@Test // DATACASS-343
	public void updateShouldUpdateEntityByQuery() throws Exception {

		User user = new User("heisenberg", "Walter", "White");
		template.insert(user).get();

		Query query = Query.query(Criteria.where("id").is("heisenberg"));
		boolean result = getUninterruptibly(
				template.update(query, Update.empty().set("firstname", "Walter Hartwell"), User.class));
		assertThat(result).isTrue();

		assertThat(getUser(user.getId()).getFirstname()).isEqualTo("Walter Hartwell");
	}

	@Test // DATACASS-343
	public void deleteByQueryShouldRemoveEntity() throws Exception {

		User user = new User("heisenberg", "Walter", "White");
		template.insert(user).get();

		Query query = Query.query(Criteria.where("id").is("heisenberg"));
		assertThat(getUninterruptibly(template.delete(query, User.class))).isTrue();

		assertThat(getUser(user.getId())).isNull();
	}

	@Test // DATACASS-343
	public void deleteColumnsByQueryShouldRemoveColumn() throws Exception {

		User user = new User("heisenberg", "Walter", "White");
		template.insert(user).get();

		Query query = Query.query(Criteria.where("id").is("heisenberg")).columns(Columns.from("lastname"));

		assertThat(getUninterruptibly(template.delete(query, User.class))).isTrue();

		User loaded = getUser(user.getId());
		assertThat(loaded.getFirstname()).isEqualTo("Walter");
		assertThat(loaded.getLastname()).isNull();
	}

	@Test // DATACASS-292
	public void deleteShouldRemoveEntity() throws Exception {

		User user = new User("heisenberg", "Walter", "White");
		getUninterruptibly(template.insert(user));

		User deleted = getUninterruptibly(template.delete(user));

		assertThat(deleted).isNotNull();
		assertThat(getUser(user.getId())).isNull();
	}

	@Test // DATACASS-292
	public void deleteByIdShouldRemoveEntity() throws Exception {

		User user = new User("heisenberg", "Walter", "White");
		getUninterruptibly(template.insert(user));

		Boolean deleted = getUninterruptibly(template.deleteById(user.getId(), User.class));
		assertThat(deleted).isTrue();

		assertThat(getUser(user.getId())).isNull();
	}

	private User getUser(String id) {
		return getUninterruptibly(template.selectOneById(id, User.class));
	}

	private static <T> T getUninterruptibly(Future<T> future) {

		try {
			return future.get();
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}
}
