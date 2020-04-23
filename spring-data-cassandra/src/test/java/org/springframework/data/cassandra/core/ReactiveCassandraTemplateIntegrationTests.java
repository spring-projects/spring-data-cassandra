/*
 * Copyright 2016-2020 the original author or authors.
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

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.StepVerifier.FirstStep;

import org.junit.Before;
import org.junit.Test;

import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.cql.ReactiveCqlTemplate;
import org.springframework.data.cassandra.core.cql.session.DefaultBridgedReactiveSession;
import org.springframework.data.cassandra.core.query.CassandraPageRequest;
import org.springframework.data.cassandra.core.query.Columns;
import org.springframework.data.cassandra.core.query.Criteria;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.cassandra.core.query.Update;
import org.springframework.data.cassandra.domain.User;
import org.springframework.data.cassandra.domain.UserToken;
import org.springframework.data.cassandra.repository.support.SchemaTestUtils;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTest;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;

import com.datastax.oss.driver.api.core.uuid.Uuids;

/**
 * Integration tests for {@link ReactiveCassandraTemplate}.
 *
 * @author Mark Paluch
 */
public class ReactiveCassandraTemplateIntegrationTests extends AbstractKeyspaceCreatingIntegrationTest {

	ReactiveCassandraTemplate template;

	@Before
	public void setUp() {

		MappingCassandraConverter converter = new MappingCassandraConverter();
		CassandraTemplate cassandraTemplate = new CassandraTemplate(this.session, converter);
		DefaultBridgedReactiveSession session = new DefaultBridgedReactiveSession(this.session);

		template = new ReactiveCassandraTemplate(new ReactiveCqlTemplate(session), converter);

		SchemaTestUtils.potentiallyCreateTableFor(User.class, cassandraTemplate);
		SchemaTestUtils.potentiallyCreateTableFor(UserToken.class, cassandraTemplate);
		SchemaTestUtils.truncate(User.class, cassandraTemplate);
		SchemaTestUtils.truncate(UserToken.class, cassandraTemplate);
	}

	@Test // DATACASS-335
	public void insertShouldInsertEntity() {

		User user = new User("heisenberg", "Walter", "White");

		Mono<User> insert = template.insert(user);
		verifyUser(user.getId()).verifyComplete();

		insert.as(StepVerifier::create).expectNext(user).verifyComplete();

		verifyUser(user.getId()).expectNext(user).verifyComplete();
	}

	@Test // DATACASS-250, DATACASS-573
	public void insertShouldCreateEntityWithLwt() {

		InsertOptions lwtOptions = InsertOptions.builder().withIfNotExists().build();

		User user = new User("heisenberg", "Walter", "White");

		Mono<EntityWriteResult<User>> inserted = template.insert(user, lwtOptions);

		inserted.as(StepVerifier::create).consumeNextWith(actual -> {

			assertThat(actual.wasApplied()).isTrue();
			assertThat(actual.getEntity()).isSameAs(user);
		}).verifyComplete();
	}

	@Test // DATACASS-250
	public void insertShouldNotUpdateEntityWithLwt() {

		InsertOptions lwtOptions = InsertOptions.builder().withIfNotExists().build();

		User user = new User("heisenberg", "Walter", "White");

		template.insert(user, lwtOptions).map(WriteResult::wasApplied).as(StepVerifier::create).expectNext(true)
				.verifyComplete();

		user.setFirstname("Walter Hartwell");

		template.insert(user, lwtOptions).map(WriteResult::wasApplied).as(StepVerifier::create).expectNext(false)
				.verifyComplete();

		verifyUser(user.getId()).consumeNextWith(it -> assertThat(it.getFirstname()).isEqualTo("Walter")).verifyComplete();
	}

	@Test // DATACASS-335
	public void shouldInsertEntityAndCount() {

		User user = new User("heisenberg", "Walter", "White");

		template.insert(user).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		template.count(User.class).as(StepVerifier::create).expectNext(1L).verifyComplete();
	}

	@Test // DATACASS-335
	public void shouldInsertEntityAndCountByQuery() {

		User user = new User("heisenberg", "Walter", "White");

		template.insert(user).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		template.count(Query.query(where("id").is("heisenberg")), User.class).as(StepVerifier::create) //
				.expectNext(1L) //
				.verifyComplete();

		template.count(Query.query(where("id").is("foo")), User.class).as(StepVerifier::create) //
				.expectNext(0L) //
				.verifyComplete();
	}

	@Test // DATACASS-335
	public void shouldInsertAndExistsByQueryEntities() {

		User user = new User("heisenberg", "Walter", "White");

		template.insert(user).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		template.exists(Query.query(where("id").is("heisenberg")), User.class).as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();

		template.exists(Query.query(where("id").is("foo")), User.class).as(StepVerifier::create) //
				.expectNext(false) //
				.verifyComplete();
	}

	@Test // DATACASS-335
	public void updateShouldUpdateEntity() {

		User user = new User("heisenberg", "Walter", "White");

		template.insert(user).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		user.setFirstname("Walter Hartwell");

		template.insert(user).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		verifyUser(user.getId()).expectNext(user).verifyComplete();
	}

	@Test // DATACASS-292
	public void updateShouldNotCreateEntityWithLwt() {

		UpdateOptions lwtOptions = UpdateOptions.builder().withIfExists().build();

		User user = new User("heisenberg", "Walter", "White");

		template.update(user, lwtOptions).map(WriteResult::wasApplied).as(StepVerifier::create).expectNext(false)
				.verifyComplete();

		verifyUser(user.getId()).verifyComplete();
	}

	@Test // DATACASS-292
	public void updateShouldUpdateEntityWithLwt() {

		UpdateOptions lwtOptions = UpdateOptions.builder().withIfExists().build();

		User user = new User("heisenberg", "Walter", "White");
		template.insert(user).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		user.setFirstname("Walter Hartwell");

		template.update(user, lwtOptions).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		verifyUser(user.getId()).consumeNextWith(it -> assertThat(it.getFirstname()).isEqualTo("Walter Hartwell"))
				.verifyComplete();
	}

	@Test // DATACASS-343
	public void updateShouldUpdateEntityByQuery() {

		User user = new User("heisenberg", "Walter", "White");

		template.insert(user).block();

		Query query = Query.query(Criteria.where("id").is("heisenberg"));
		boolean result = template.update(query, Update.empty().set("firstname", "Walter Hartwell"), User.class).block();
		assertThat(result).isTrue();

		assertThat(template.selectOneById(user.getId(), User.class).block().getFirstname()).isEqualTo("Walter Hartwell");
	}

	@Test // DATACASS-343
	public void deleteByQueryShouldRemoveEntity() {

		User user = new User("heisenberg", "Walter", "White");
		template.insert(user).block();

		Query query = Query.query(Criteria.where("id").is("heisenberg"));
		assertThat(template.delete(query, User.class).block()).isTrue();

		assertThat(template.selectOneById(user.getId(), User.class).block()).isNull();
	}

	@Test // DATACASS-343
	public void deleteColumnsByQueryShouldRemoveColumn() {

		User user = new User("heisenberg", "Walter", "White");
		template.insert(user).block();

		Query query = Query.query(Criteria.where("id").is("heisenberg")).columns(Columns.from("lastname"));

		assertThat(template.delete(query, User.class).block()).isTrue();

		User loaded = template.selectOneById(user.getId(), User.class).block();
		assertThat(loaded.getFirstname()).isEqualTo("Walter");
		assertThat(loaded.getLastname()).isNull();
	}

	@Test // DATACASS-335
	public void deleteShouldRemoveEntity() {

		User user = new User("heisenberg", "Walter", "White");

		template.insert(user).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		template.delete(user).as(StepVerifier::create).expectNext(user).verifyComplete();

		verifyUser(user.getId()).verifyComplete();
	}

	@Test // DATACASS-335
	public void deleteByIdShouldRemoveEntity() {

		User user = new User("heisenberg", "Walter", "White");

		template.insert(user).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		template.deleteById(user.getId(), User.class).as(StepVerifier::create).expectNext(true).verifyComplete();

		verifyUser(user.getId()).verifyComplete();
	}

	@Test // DATACASS-606
	public void deleteShouldRemoveEntityWithLwt() {

		DeleteOptions lwtOptions = DeleteOptions.builder().withIfExists().build();

		User user = new User("heisenberg", "Walter", "White");
		template.insert(user).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		template.delete(user, lwtOptions).map(WriteResult::wasApplied) //
				.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();

		template.delete(user, lwtOptions).map(WriteResult::wasApplied) //
				.as(StepVerifier::create) //
				.expectNext(false) //
				.verifyComplete();
	}

	@Test // DATACASS-606
	public void deleteByQueryShouldRemoveEntityWithLwt() {

		DeleteOptions lwtOptions = DeleteOptions.builder().withIfExists().build();

		User user = new User("heisenberg", "Walter", "White");
		template.insert(user).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		Query query = Query.query(where("id").is("heisenberg")).queryOptions(lwtOptions);

		template.delete(query, User.class) //
				.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();

		template.delete(query, User.class) //
				.as(StepVerifier::create) //
				.expectNext(false) //
				.verifyComplete();
	}

	@Test // DATACASS-343
	public void shouldSelectByQueryWithSorting() {

		UserToken token1 = new UserToken();
		token1.setUserId(Uuids.endOf(System.currentTimeMillis()));
		token1.setToken(Uuids.startOf(System.currentTimeMillis()));
		token1.setUserComment("foo");

		UserToken token2 = new UserToken();
		token2.setUserId(token1.getUserId());
		token2.setToken(Uuids.endOf(System.currentTimeMillis() + 100));
		token2.setUserComment("bar");

		template.insert(token1).block();
		template.insert(token2).block();

		Query query = Query.query(Criteria.where("userId").is(token1.getUserId())).sort(Sort.by("token"));

		assertThat(template.select(query, UserToken.class).collectList().block()).containsSequence(token1, token2);
	}

	@Test // DATACASS-343
	public void shouldSelectOneByQuery() {

		UserToken token1 = new UserToken();
		token1.setUserId(Uuids.endOf(System.currentTimeMillis()));
		token1.setToken(Uuids.startOf(System.currentTimeMillis()));
		token1.setUserComment("foo");

		template.insert(token1).block();

		Query query = Query.query(Criteria.where("userId").is(token1.getUserId()));

		assertThat(template.selectOne(query, UserToken.class).block()).isEqualTo(token1);
	}

	@Test // DATACASS-529
	public void shouldReturnEmptySliceOnEmptyResult() {

		Query query = Query.query(where("id").is("foo")).pageRequest(CassandraPageRequest.first(10));

		Mono<Slice<User>> slice = template.slice(query, User.class);

		slice.as(StepVerifier::create).consumeNextWith(it -> {
			assertThat(it).isEmpty();
		}).verifyComplete();
	}

	private FirstStep<User> verifyUser(String userId) {
		return template.selectOneById(userId, User.class).as(StepVerifier::create);
	}

}
