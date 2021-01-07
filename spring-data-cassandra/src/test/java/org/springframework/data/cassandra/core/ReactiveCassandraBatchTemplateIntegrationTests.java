/*
 * Copyright 2018-2021 the original author or authors.
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

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.cassandra.ReactiveResultSet;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.cql.ReactiveCqlTemplate;
import org.springframework.data.cassandra.core.cql.WriteOptions;
import org.springframework.data.cassandra.core.cql.session.DefaultBridgedReactiveSession;
import org.springframework.data.cassandra.domain.FlatGroup;
import org.springframework.data.cassandra.domain.Group;
import org.springframework.data.cassandra.domain.GroupKey;
import org.springframework.data.cassandra.repository.support.SchemaTestUtils;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTests;

/**
 * Integration tests for {@link ReactiveCassandraBatchTemplate}.
 *
 * @author Oleh Dokuka
 * @author Mark Paluch
 */
class ReactiveCassandraBatchTemplateIntegrationTests extends AbstractKeyspaceCreatingIntegrationTests {

	private ReactiveCassandraTemplate template;

	private Group walter = new Group(new GroupKey("users", "0x1", "walter"));
	private Group mike = new Group(new GroupKey("users", "0x1", "mike"));

	@BeforeEach
	void setUp() {

		MappingCassandraConverter converter = new MappingCassandraConverter();
		CassandraTemplate cassandraTemplate = new CassandraTemplate(this.session, converter);

		DefaultBridgedReactiveSession session = new DefaultBridgedReactiveSession(this.session);
		template = new ReactiveCassandraTemplate(new ReactiveCqlTemplate(session), converter);

		SchemaTestUtils.potentiallyCreateTableFor(Group.class, cassandraTemplate);
		SchemaTestUtils.potentiallyCreateTableFor(FlatGroup.class, cassandraTemplate);

		SchemaTestUtils.truncate(Group.class, cassandraTemplate);
		SchemaTestUtils.truncate(FlatGroup.class, cassandraTemplate);

		this.template.insert(walter).then(this.template.insert(mike)) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();
	}

	@Test // DATACASS-574
	void shouldInsertEntities() {

		ReactiveCassandraBatchOperations batchOperations = new ReactiveCassandraBatchTemplate(template);
		Mono<WriteResult> execution = batchOperations.insert(walter).insert(mike).execute();

		Mono<Group> loadedMono = execution.then(template.selectOneById(walter.getId(), Group.class));

		loadedMono //
				.as(StepVerifier::create) //
				.assertNext(loaded -> assertThat(loaded.getId().getUsername()).isEqualTo(walter.getId().getUsername()))
				.verifyComplete();
	}

	@Test // DATACASS-574
	void shouldInsertCollectionOfEntities() {

		ReactiveCassandraBatchOperations batchOperations = new ReactiveCassandraBatchTemplate(template);
		Mono<Group> loadedMono = batchOperations.insert(Arrays.asList(walter, mike)).execute()
				.then(template.selectOneById(walter.getId(), Group.class));

		loadedMono //
				.as(StepVerifier::create) //
				.assertNext(loaded -> assertThat(loaded.getId().getUsername()).isEqualTo(walter.getId().getUsername()))
				.verifyComplete();
	}

	@Test // DATACASS-574
	void shouldInsertCollectionOfEntitiesWithTtl() {

		walter.setEmail("walter@white.com");
		mike.setEmail("mike@sauls.com");

		int ttl = 30;
		WriteOptions options = WriteOptions.builder().ttl(30).build();

		ReactiveCassandraBatchOperations batchOperations = new ReactiveCassandraBatchTemplate(template);
		Mono<ReactiveResultSet> resultSet = batchOperations.insert(Arrays.asList(walter, mike), options).execute()
				.then(template.getReactiveCqlOperations().queryForResultSet("SELECT TTL(email) FROM group;"));

		resultSet.flatMapMany(ReactiveResultSet::availableRows) //
				.as(StepVerifier::create) //
				.assertNext(row -> assertThat(row.getInt(0)).isBetween(1, ttl))
				.assertNext(row -> assertThat(row.getInt(0)).isBetween(1, ttl)).verifyComplete();
	}

	@Test // DATACASS-574
	void shouldInsertMonoOfEntitiesWithTtl() {

		walter.setEmail("walter@white.com");
		mike.setEmail("mike@sauls.com");

		int ttl = 30;
		WriteOptions options = WriteOptions.builder().ttl(30).build();

		ReactiveCassandraBatchOperations batchOperations = new ReactiveCassandraBatchTemplate(template);
		Mono<ReactiveResultSet> resultSet = batchOperations.insert(Mono.just(Arrays.asList(walter, mike)), options)
				.execute().then(template.getReactiveCqlOperations().queryForResultSet("SELECT TTL(email) FROM group;"));

		resultSet.flatMapMany(ReactiveResultSet::availableRows) //
				.as(StepVerifier::create) //
				.assertNext(row -> assertThat(row.getInt(0)).isBetween(1, ttl))
				.assertNext(row -> assertThat(row.getInt(0)).isBetween(1, ttl)).verifyComplete();
	}

	@Test // DATACASS-574
	void shouldUpdateEntities() {

		walter.setEmail("walter@white.com");
		mike.setEmail("mike@sauls.com");

		ReactiveCassandraBatchOperations batchOperations = new ReactiveCassandraBatchTemplate(template);
		Mono<Group> loadedMono = batchOperations.update(walter).update(mike).execute()
				.then(template.selectOneById(walter.getId(), Group.class));

		loadedMono //
				.as(StepVerifier::create) //
				.assertNext(loaded -> assertThat(loaded.getEmail()).isEqualTo(walter.getEmail())).verifyComplete();
	}

	@Test // DATACASS-574
	void shouldUpdateMonoEntities() {

		walter.setEmail("walter@white.com");
		mike.setEmail("mike@sauls.com");

		ReactiveCassandraBatchOperations batchOperations = new ReactiveCassandraBatchTemplate(template);
		Mono<Group> loadedMono = batchOperations.update(walter).update(Mono.just(Collections.singletonList(mike))).execute()
				.then(template.selectOneById(walter.getId(), Group.class));

		loadedMono //
				.as(StepVerifier::create) //
				.assertNext(loaded -> assertThat(loaded.getEmail()).isEqualTo(walter.getEmail())).verifyComplete();
	}

	@Test // DATACASS-574
	void shouldUpdateCollectionOfEntities() {

		walter.setEmail("walter@white.com");
		mike.setEmail("mike@sauls.com");

		ReactiveCassandraBatchOperations batchOperations = new ReactiveCassandraBatchTemplate(template);
		Mono<Group> loadedMono = batchOperations.update(Arrays.asList(walter, mike)).execute()
				.then(template.selectOneById(walter.getId(), Group.class));

		loadedMono //
				.as(StepVerifier::create) //
				.assertNext(loaded -> assertThat(loaded.getEmail()).isEqualTo(walter.getEmail())).verifyComplete();
	}

	@Test // DATACASS-574
	void shouldUpdateCollectionOfEntitiesWithTtl() {

		walter.setEmail("walter@white.com");
		mike.setEmail("mike@sauls.com");

		int ttl = 30;
		WriteOptions options = WriteOptions.builder().ttl(ttl).build();

		ReactiveCassandraBatchOperations batchOperations = new ReactiveCassandraBatchTemplate(template);
		Mono<ReactiveResultSet> resultSet = batchOperations.update(Arrays.asList(walter, mike), options).execute()
				.then(template.getReactiveCqlOperations().queryForResultSet("SELECT TTL(email) FROM group;"));

		resultSet.flatMapMany(ReactiveResultSet::availableRows) //
				.as(StepVerifier::create) //

				.assertNext(row -> assertThat(row.getInt(0)).isBetween(1, ttl))
				.assertNext(row -> assertThat(row.getInt(0)).isBetween(1, ttl)).verifyComplete();
	}

	@Test // DATACASS-574
	void shouldUpdateMonoCollectionOfEntitiesWithTtl() {

		walter.setEmail("walter@white.com");
		mike.setEmail("mike@sauls.com");

		int ttl = 30;
		WriteOptions options = WriteOptions.builder().ttl(ttl).build();

		ReactiveCassandraBatchOperations batchOperations = new ReactiveCassandraBatchTemplate(template);
		Mono<ReactiveResultSet> resultSet = batchOperations.update(Collections.singletonList(walter), options)
				.update(Mono.just(Collections.singletonList(mike)), options).execute()
				.then(template.getReactiveCqlOperations().queryForResultSet("SELECT TTL(email) FROM group;"));

		resultSet.flatMapMany(ReactiveResultSet::availableRows) //
				.as(StepVerifier::create) //
				.assertNext(row -> assertThat(row.getInt(0)).isBetween(1, ttl))
				.assertNext(row -> assertThat(row.getInt(0)).isBetween(1, ttl)).verifyComplete();
	}

	@Test // DATACASS-574
	void shouldUpdateMonoOfEntities() {

		FlatGroup walter = new FlatGroup("users", "0x1", "walter");
		FlatGroup mike = new FlatGroup("users", "0x1", "mike");

		ReactiveCassandraBatchOperations batchOperations = new ReactiveCassandraBatchTemplate(template);
		Mono<FlatGroup> loadedMono = template.insert(walter).then(template.insert(mike)).then(Mono.fromRunnable(() -> {
			walter.setEmail("walter@white.com");
			mike.setEmail("mike@sauls.com");
		})).then(Mono.defer(() -> batchOperations.update(Arrays.asList(walter, mike)).execute()))
				.then(template.selectOneById(walter, FlatGroup.class));

		loadedMono //
				.as(StepVerifier::create) //
				.assertNext(loaded -> assertThat(loaded.getEmail()).isEqualTo(walter.getEmail())).verifyComplete();
	}

	@Test // DATACASS-574
	void shouldUpdateMonoCollectionOfEntities() {

		FlatGroup walter = new FlatGroup("users", "0x1", "walter");
		FlatGroup mike = new FlatGroup("users", "0x1", "mike");

		ReactiveCassandraBatchOperations batchOperations = new ReactiveCassandraBatchTemplate(template);
		Mono<FlatGroup> loadedMono = template.insert(walter).then(template.insert(mike)).then(Mono.fromRunnable(() -> {
			walter.setEmail("walter@white.com");
			mike.setEmail("mike@sauls.com");
		})).then(Mono.defer(() -> batchOperations.update(Collections.singletonList(walter))
				.update(Mono.just(Collections.singletonList(mike))).execute()))
				.then(template.selectOneById(walter, FlatGroup.class));

		loadedMono //
				.as(StepVerifier::create) //
				.assertNext(loaded -> assertThat(loaded.getEmail()).isEqualTo(walter.getEmail())).verifyComplete();
	}

	@Test // DATACASS-574
	void shouldDeleteEntities() {

		ReactiveCassandraBatchOperations batchOperations = new ReactiveCassandraBatchTemplate(template);

		Mono<Group> loadedMono = batchOperations.delete(walter).delete(mike).execute()
				.then(template.selectOneById(walter.getId(), Group.class));

		loadedMono //
				.as(StepVerifier::create) //
				.verifyComplete();
	}

	@Test // DATACASS-574
	void shouldDeleteCollectionOfEntities() {

		ReactiveCassandraBatchOperations batchOperations = new ReactiveCassandraBatchTemplate(template);

		Mono<Group> loadedMono = batchOperations.delete(Arrays.asList(walter, mike)).execute()
				.then(template.selectOneById(walter.getId(), Group.class));

		loadedMono //
				.as(StepVerifier::create) //
				.verifyComplete();
	}

	@Test // DATACASS-574
	void shouldDeleteMonoOfEntities() {

		ReactiveCassandraBatchOperations batchOperations = new ReactiveCassandraBatchTemplate(template);

		Mono<Group> loadedMono = batchOperations.delete(Mono.just(Arrays.asList(walter, mike))).execute()
				.then(template.selectOneById(walter.getId(), Group.class));

		loadedMono //
				.as(StepVerifier::create) //
				.verifyComplete();
	}

	@Test // DATACASS-574
	void shouldApplyTimestampToAllEntities() {

		walter.setEmail("walter@white.com");
		mike.setEmail("mike@sauls.com");

		long timestamp = (System.currentTimeMillis() + TimeUnit.DAYS.toMillis(1)) * 1000;

		ReactiveCassandraBatchOperations batchOperations = new ReactiveCassandraBatchTemplate(template);
		Mono<ReactiveResultSet> resultSet = batchOperations.insert(walter).insert(mike).withTimestamp(timestamp).execute()
				.then(template.getReactiveCqlOperations().queryForResultSet("SELECT writetime(email) FROM group;"));

		resultSet.flatMapMany(ReactiveResultSet::availableRows) //
				.as(StepVerifier::create) //
				.assertNext(row -> assertThat(row.getLong(0)).isEqualTo(timestamp))
				.assertNext(row -> assertThat(row.getLong(0)).isEqualTo(timestamp)).verifyComplete();
	}

	@Test // DATACASS-574
	void shouldNotExecuteTwice() {

		ReactiveCassandraBatchOperations batchOperations = new ReactiveCassandraBatchTemplate(template);

		batchOperations.insert(walter).execute() //
				.then(batchOperations.execute()) //
				.as(StepVerifier::create) //
				.verifyError(IllegalStateException.class);
	}

	@Test // DATACASS-574
	void shouldNotAllowModificationAfterExecution() {

		ReactiveCassandraBatchOperations batchOperations = new ReactiveCassandraBatchTemplate(template);

		batchOperations.insert(walter).execute().then(Mono.fromRunnable(() -> batchOperations.update(new Group()))) //
				.as(StepVerifier::create) //
				.verifyError(IllegalStateException.class);
	}

	@Test // DATACASS-574
	void shouldNotAllowModificationAfterExecutionMonoCase() {

		ReactiveCassandraBatchOperations batchOperations = new ReactiveCassandraBatchTemplate(template);

		batchOperations.insert(Mono.just(Collections.singletonList(walter))).execute()
				.then(Mono.fromRunnable(() -> batchOperations.update(new Group()))) //
				.as(StepVerifier::create) //
				.verifyError(IllegalStateException.class);
	}

	@Test // DATACASS-574
	void shouldSupportMultithreadedMerge() {

		ReactiveCassandraBatchOperations batchOperations = new ReactiveCassandraBatchTemplate(template);
		Random random = new Random();

		for (int i = 0; i < 100; i++) {

			batchOperations.insert(Mono.just(Arrays.asList(new Group(new GroupKey("users", "0x1", "walter" + random.longs())),
					new Group(new GroupKey("users", "0x1", "walter" + random.longs())),
					new Group(new GroupKey("users", "0x1", "walter" + random.longs())),
					new Group(new GroupKey("users", "0x1", "walter" + random.longs())))).publishOn(Schedulers.elastic()));
		}

		batchOperations.execute()
				.then(template.getReactiveCqlOperations().queryForResultSet("SELECT TTL(email) FROM group;")) //
				.flatMapMany(ReactiveResultSet::availableRows) //
				.as(StepVerifier::create) //
				.expectNextCount(402).verifyComplete();
	}
}
