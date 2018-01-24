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

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.cassandra.core.query.Criteria.where;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.StepVerifier.FirstStep;

import org.junit.Before;
import org.junit.Test;

import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.cql.ReactiveCqlTemplate;
import org.springframework.data.cassandra.core.cql.session.DefaultBridgedReactiveSession;
import org.springframework.data.cassandra.core.query.Columns;
import org.springframework.data.cassandra.core.query.Criteria;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.cassandra.core.query.Update;
import org.springframework.data.cassandra.domain.User;
import org.springframework.data.cassandra.domain.UserToken;
import org.springframework.data.cassandra.repository.support.SchemaTestUtils;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTest;
import org.springframework.data.domain.Sort;

import com.datastax.driver.core.utils.UUIDs;

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
		DefaultBridgedReactiveSession session = new DefaultBridgedReactiveSession(this.session, Schedulers.elastic());

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

		StepVerifier.create(insert).expectNext(user).verifyComplete();

		verifyUser(user.getId()).expectNext(user).verifyComplete();
	}

	@Test // DATACASS-250
	public void insertShouldCreateEntityWithLwt() {

		InsertOptions lwtOptions = InsertOptions.builder().withIfNotExists().build();

		User user = new User("heisenberg", "Walter", "White");

		Mono<WriteResult> inserted = template.insert(user, lwtOptions);

		StepVerifier.create(inserted.map(WriteResult::wasApplied)).expectNext(true).verifyComplete();
	}

	@Test // DATACASS-250
	public void insertShouldNotUpdateEntityWithLwt() {

		InsertOptions lwtOptions = InsertOptions.builder().withIfNotExists().build();

		User user = new User("heisenberg", "Walter", "White");

		StepVerifier.create(template.insert(user, lwtOptions).map(WriteResult::wasApplied)).expectNext(true)
				.verifyComplete();

		user.setFirstname("Walter Hartwell");

		StepVerifier.create(template.insert(user, lwtOptions).map(WriteResult::wasApplied)).expectNext(false)
				.verifyComplete();

		verifyUser(user.getId()).consumeNextWith(it -> assertThat(it.getFirstname()).isEqualTo("Walter")).verifyComplete();
	}

	@Test // DATACASS-335
	public void shouldInsertEntityAndCount() {

		User user = new User("heisenberg", "Walter", "White");

		StepVerifier.create(template.insert(user)).expectNextCount(1).verifyComplete();

		StepVerifier.create(template.count(User.class)).expectNext(1L).verifyComplete();
	}

	@Test // DATACASS-335
	public void shouldInsertEntityAndCountByQuery() {

		User user = new User("heisenberg", "Walter", "White");

		StepVerifier.create(template.insert(user)).expectNextCount(1).verifyComplete();

		StepVerifier.create(template.count(Query.query(where("id").is("heisenberg")), User.class)) //
				.expectNext(1L) //
				.verifyComplete();

		StepVerifier.create(template.count(Query.query(where("id").is("foo")), User.class)) //
				.expectNext(0L) //
				.verifyComplete();
	}

	@Test // DATACASS-335
	public void shouldInsertAndExistsByQueryEntities() {

		User user = new User("heisenberg", "Walter", "White");

		StepVerifier.create(template.insert(user)).expectNextCount(1).verifyComplete();

		StepVerifier.create(template.exists(Query.query(where("id").is("heisenberg")), User.class)) //
				.expectNext(true) //
				.verifyComplete();

		StepVerifier.create(template.exists(Query.query(where("id").is("foo")), User.class)) //
				.expectNext(false) //
				.verifyComplete();
	}

	@Test // DATACASS-335
	public void updateShouldUpdateEntity() {

		User user = new User("heisenberg", "Walter", "White");

		StepVerifier.create(template.insert(user)).expectNextCount(1).verifyComplete();

		user.setFirstname("Walter Hartwell");

		StepVerifier.create(template.insert(user)).expectNextCount(1).verifyComplete();

		verifyUser(user.getId()).expectNext(user).verifyComplete();
	}

	@Test // DATACASS-292
	public void updateShouldNotCreateEntityWithLwt() {

		UpdateOptions lwtOptions = UpdateOptions.builder().withIfExists().build();

		User user = new User("heisenberg", "Walter", "White");

		StepVerifier.create(template.update(user, lwtOptions).map(WriteResult::wasApplied)).expectNext(false)
				.verifyComplete();

		verifyUser(user.getId()).verifyComplete();
	}

	@Test // DATACASS-292
	public void updateShouldUpdateEntityWithLwt() {

		UpdateOptions lwtOptions = UpdateOptions.builder().withIfExists().build();

		User user = new User("heisenberg", "Walter", "White");
		StepVerifier.create(template.insert(user)).expectNextCount(1).verifyComplete();

		user.setFirstname("Walter Hartwell");

		StepVerifier.create(template.update(user, lwtOptions)).expectNextCount(1).verifyComplete();

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

		StepVerifier.create(template.insert(user)).expectNextCount(1).verifyComplete();

		StepVerifier.create(template.delete(user)).expectNext(user).verifyComplete();

		verifyUser(user.getId()).verifyComplete();
	}

	@Test // DATACASS-335
	public void deleteByIdShouldRemoveEntity() {

		User user = new User("heisenberg", "Walter", "White");

		StepVerifier.create(template.insert(user)).expectNextCount(1).verifyComplete();

		StepVerifier.create(template.deleteById(user.getId(), User.class)).expectNext(true).verifyComplete();

		verifyUser(user.getId()).verifyComplete();
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

		template.insert(token1).block();
		template.insert(token2).block();

		Query query = Query.query(Criteria.where("userId").is(token1.getUserId())).sort(Sort.by("token"));

		assertThat(template.select(query, UserToken.class).collectList().block()).containsSequence(token1, token2);
	}

	@Test // DATACASS-343
	public void shouldSelectOneByQuery() {

		UserToken token1 = new UserToken();
		token1.setUserId(UUIDs.endOf(System.currentTimeMillis()));
		token1.setToken(UUIDs.startOf(System.currentTimeMillis()));
		token1.setUserComment("foo");

		template.insert(token1).block();

		Query query = Query.query(Criteria.where("userId").is(token1.getUserId()));

		assertThat(template.selectOne(query, UserToken.class).block()).isEqualTo(token1);
	}

	private FirstStep<User> verifyUser(String userId) {
		return StepVerifier.create(template.selectOneById(userId, User.class));
	}
}
