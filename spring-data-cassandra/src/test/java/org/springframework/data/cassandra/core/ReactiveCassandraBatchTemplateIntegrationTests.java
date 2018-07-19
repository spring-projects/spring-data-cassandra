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

import java.util.Arrays;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import org.springframework.data.cassandra.ReactiveResultSet;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.cql.ReactiveCqlTemplate;
import org.springframework.data.cassandra.core.cql.WriteOptions;
import org.springframework.data.cassandra.core.cql.session.DefaultBridgedReactiveSession;
import org.springframework.data.cassandra.domain.FlatGroup;
import org.springframework.data.cassandra.domain.Group;
import org.springframework.data.cassandra.domain.GroupKey;
import org.springframework.data.cassandra.repository.support.SchemaTestUtils;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTest;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link ReactiveCassandraBatchTemplate}.
 *
 * @author Mark Paluch
 * @author Oleh Dokuka
 */
public class ReactiveCassandraBatchTemplateIntegrationTests
        extends AbstractKeyspaceCreatingIntegrationTest {

	ReactiveCassandraTemplate template;

	Group walter = new Group(new GroupKey("users", "0x1", "walter"));
	Group mike = new Group(new GroupKey("users", "0x1", "mike"));

	@Before
	public void setUp() throws Exception {

		MappingCassandraConverter converter = new MappingCassandraConverter();
		CassandraTemplate cassandraTemplate = new CassandraTemplate(this.session, converter);

		DefaultBridgedReactiveSession session = new DefaultBridgedReactiveSession(this.session);
		template = new ReactiveCassandraTemplate(new ReactiveCqlTemplate(session), converter);

		SchemaTestUtils.potentiallyCreateTableFor(Group.class, cassandraTemplate);
		SchemaTestUtils.potentiallyCreateTableFor(FlatGroup.class, cassandraTemplate);

		SchemaTestUtils.truncate(Group.class, cassandraTemplate);
		SchemaTestUtils.truncate(FlatGroup.class, cassandraTemplate);

		this.template.insert(walter)
		             .then(this.template.insert(mike))
		             .block();
	}

	@Test // DATACASS-574
	public void shouldInsertEntities() {

		ReactiveCassandraBatchOperations batchOperations = new ReactiveCassandraBatchTemplate(template);
		Mono<WriteResult> execution = batchOperations.insert(walter)
		                                             .insert(mike)
		                                             .execute();

		Mono<Group> loadedMono = execution.then(template.selectOneById(walter.getId(), Group.class));

		StepVerifier.create(loadedMono)
		            .assertNext(loaded -> assertThat(loaded.getId().getUsername()).isEqualTo(walter.getId().getUsername()))
		            .verifyComplete();
	}

	@Test // DATACASS-574
	@SuppressWarnings("unchecked")
	public void shouldInsertEntitiesWithLwt() {

		InsertOptions lwtOptions = InsertOptions.builder().withIfNotExists().build();
        ReactiveCassandraBatchOperations batchOperations = new ReactiveCassandraBatchTemplate(template);

		Group previousWalter = new Group(new GroupKey("users", "0x1", "walter"));
		previousWalter.setAge(42);

		Flux concatenatedExecution = Flux.concat(
			template
				.insert(previousWalter)
				.then(Mono.fromRunnable(() -> walter.setAge(100)))
				.then(Mono.defer(() -> batchOperations
                        .insert(Collections.singleton(walter), lwtOptions)
                        .insert(mike)
                        .execute())),
			template.selectOneById(walter.getId(), Group.class),
			template.selectOneById(mike.getId(), Group.class)
		);

		StepVerifier.create(concatenatedExecution)
		            .assertNext(o -> {
		            	WriteResult writeResult = (WriteResult) o;

			            assertThat(writeResult.wasApplied()).isFalse();
			            assertThat(writeResult.getExecutionInfo()).isNotEmpty();
			            assertThat(writeResult.getRows()).isNotEmpty();
		            })
		            .assertNext(o -> {
			            Group loadedWalter = (Group) o;

			            assertThat(loadedWalter.getId().getUsername()).isEqualTo(walter.getId().getUsername());
			            assertThat(loadedWalter.getAge()).isEqualTo(42);
		            })
		            .assertNext(o -> {
		            	Group loadedMike = (Group) o;

		            	assertThat(loadedMike).isNotNull();
		            })
		            .verifyComplete();

	}

	@Test // DATACASS-574
	public void shouldInsertCollectionOfEntities() {

		ReactiveCassandraBatchOperations batchOperations = new ReactiveCassandraBatchTemplate(template);
		Mono<Group> loadedMono = batchOperations
				.insert(Arrays.asList(walter, mike))
				.execute()
				.then(template.selectOneById(walter.getId(), Group.class));

		StepVerifier.create(loadedMono)
		            .assertNext(loaded -> assertThat(loaded.getId().getUsername()).isEqualTo(walter.getId().getUsername()))
		            .verifyComplete();
	}

	@Test // DATACASS-443
	public void shouldInsertCollectionOfEntitiesWithTtl() {

		walter.setEmail("walter@white.com");
		mike.setEmail("mike@sauls.com");

		int ttl = 30;
		WriteOptions options = WriteOptions.builder().ttl(30).build();

		ReactiveCassandraBatchOperations batchOperations = new ReactiveCassandraBatchTemplate(template);
		Mono<ReactiveResultSet> resultSet = batchOperations
				.insert(Arrays.asList(walter, mike), options)
				.execute()
				.then(template.getReactiveCqlOperations()
				              .queryForResultSet("SELECT TTL(email) FROM group;"));

		StepVerifier.create(resultSet.flatMapMany(ReactiveResultSet::availableRows))
		            .assertNext(row -> assertThat(row.getInt(0)).isBetween(1, ttl))
		            .assertNext(row -> assertThat(row.getInt(0)).isBetween(1, ttl))
		            .verifyComplete();
	}


    @Test // DATACASS-443
    public void shouldInsertMonoCollectionOfEntitiesWithTtl() {

        walter.setEmail("walter@white.com");
        mike.setEmail("mike@sauls.com");

        int ttl = 30;
        WriteOptions options = WriteOptions.builder().ttl(30).build();

        ReactiveCassandraBatchOperations batchOperations = new ReactiveCassandraBatchTemplate(template);
        Mono<ReactiveResultSet> resultSet = batchOperations
                .insert(Mono.just(Arrays.asList(walter, mike)), options)
                .execute()
                .then(template.getReactiveCqlOperations()
                              .queryForResultSet("SELECT TTL(email) FROM group;"));

        StepVerifier.create(resultSet.flatMapMany(ReactiveResultSet::availableRows))
                    .assertNext(row -> assertThat(row.getInt(0)).isBetween(1, ttl))
                    .assertNext(row -> assertThat(row.getInt(0)).isBetween(1, ttl))
                    .verifyComplete();
    }

	@Test // DATACASS-574
	public void shouldUpdateEntities() {

		walter.setEmail("walter@white.com");
		mike.setEmail("mike@sauls.com");

		ReactiveCassandraBatchOperations batchOperations = new ReactiveCassandraBatchTemplate(template);
		Mono<Group> loadedMono = batchOperations
				.update(walter)
				.update(mike)
				.execute()
				.then(template.selectOneById(walter.getId(), Group.class));


		StepVerifier.create(loadedMono)
		            .assertNext(loaded -> assertThat(loaded.getEmail()).isEqualTo(walter.getEmail()))
		            .verifyComplete();
	}

    @Test // DATACASS-574
    public void shouldUpdateMonoEntities() {

        walter.setEmail("walter@white.com");
        mike.setEmail("mike@sauls.com");

        ReactiveCassandraBatchOperations batchOperations = new ReactiveCassandraBatchTemplate(template);
        Mono<Group> loadedMono = batchOperations
                .update(walter)
                .update(Mono.just(Arrays.asList(mike)))
                .execute()
                .then(template.selectOneById(walter.getId(), Group.class));


        StepVerifier.create(loadedMono)
                    .assertNext(loaded -> assertThat(loaded.getEmail()).isEqualTo(walter.getEmail()))
                    .verifyComplete();
    }

	@Test // DATACASS-574
	public void shouldUpdateCollectionOfEntities() {

		walter.setEmail("walter@white.com");
		mike.setEmail("mike@sauls.com");

		ReactiveCassandraBatchOperations batchOperations = new ReactiveCassandraBatchTemplate(template);
		Mono<Group> loadedMono = batchOperations
				.update(Arrays.asList(walter, mike))
				.execute()
				.then(template.selectOneById(walter.getId(), Group.class));

		StepVerifier.create(loadedMono)
		            .assertNext(loaded -> assertThat(loaded.getEmail()).isEqualTo(walter.getEmail()))
		            .verifyComplete();
	}

	@Test // DATACASS-443
	public void shouldUpdateCollectionOfEntitiesWithTtl() {

		walter.setEmail("walter@white.com");
		mike.setEmail("mike@sauls.com");

		int ttl = 30;
		WriteOptions options = WriteOptions.builder().ttl(ttl).build();

		ReactiveCassandraBatchOperations batchOperations = new ReactiveCassandraBatchTemplate(template);
		Mono<ReactiveResultSet> resultSet = batchOperations
				.update(Arrays.asList(walter, mike), options)
				.execute()
				.then(template.getReactiveCqlOperations()
				              .queryForResultSet("SELECT TTL(email) FROM group;"));


		StepVerifier.create(resultSet.flatMapMany(ReactiveResultSet::availableRows))
		            .assertNext(row -> assertThat(row.getInt(0)).isBetween(1, ttl))
		            .assertNext(row -> assertThat(row.getInt(0)).isBetween(1, ttl))
		            .verifyComplete();
	}

    @Test // DATACASS-443
    public void shouldUpdateMonoCollectionOfEntitiesWithTtl() {

        walter.setEmail("walter@white.com");
        mike.setEmail("mike@sauls.com");

        int ttl = 30;
        WriteOptions options = WriteOptions.builder().ttl(ttl).build();

        ReactiveCassandraBatchOperations batchOperations = new ReactiveCassandraBatchTemplate(template);
        Mono<ReactiveResultSet> resultSet = batchOperations
                .update(Arrays.asList(walter), options)
                .update(Mono.just(Arrays.asList(mike)), options)
                .execute()
                .then(template.getReactiveCqlOperations()
                              .queryForResultSet("SELECT TTL(email) FROM group;"));


        StepVerifier.create(resultSet.flatMapMany(ReactiveResultSet::availableRows))
                    .assertNext(row -> assertThat(row.getInt(0)).isBetween(1, ttl))
                    .assertNext(row -> assertThat(row.getInt(0)).isBetween(1, ttl))
                    .verifyComplete();
    }

	@Test // DATACASS-574
	public void shouldUpdatesCollectionOfEntities() {

		FlatGroup walter = new FlatGroup("users", "0x1", "walter");
		FlatGroup mike = new FlatGroup("users", "0x1", "mike");

		ReactiveCassandraBatchOperations batchOperations = new ReactiveCassandraBatchTemplate(template);
		Mono<FlatGroup> loadedMono =
				template.insert(walter)
				        .then(template.insert(mike))
				        .then(Mono.fromRunnable(() -> {
					        walter.setEmail("walter@white.com");
					        mike.setEmail("mike@sauls.com");
				        }))
				        .then(Mono.defer(() -> batchOperations.update(Arrays.asList(walter, mike))
				                                              .execute()))
				        .then(template.selectOneById(walter, FlatGroup.class));

		StepVerifier.create(loadedMono)
		            .assertNext(loaded -> assertThat(loaded.getEmail()).isEqualTo(walter.getEmail()))
		            .verifyComplete();
	}

    @Test // DATACASS-574
    public void shouldUpdatesMonoCollectionOfEntities() {

        FlatGroup walter = new FlatGroup("users", "0x1", "walter");
        FlatGroup mike = new FlatGroup("users", "0x1", "mike");

        ReactiveCassandraBatchOperations batchOperations = new ReactiveCassandraBatchTemplate(template);
        Mono<FlatGroup> loadedMono =
                template.insert(walter)
                        .then(template.insert(mike))
                        .then(Mono.fromRunnable(() -> {
                            walter.setEmail("walter@white.com");
                            mike.setEmail("mike@sauls.com");
                        }))
                        .then(Mono.defer(() -> batchOperations.update(Arrays.asList(walter))
                                                              .update(Mono.just(Arrays.asList(mike)))
                                                              .execute()))
                        .then(template.selectOneById(walter, FlatGroup.class));

        StepVerifier.create(loadedMono)
                    .assertNext(loaded -> assertThat(loaded.getEmail()).isEqualTo(walter.getEmail()))
                    .verifyComplete();
    }

	@Test // DATACASS-574
	public void shouldDeleteEntities() {

		ReactiveCassandraBatchOperations batchOperations = new ReactiveCassandraBatchTemplate(template);

		Mono<Group> loadedMono = batchOperations
				.delete(walter)
				.delete(mike)
				.execute()
				.then(template.selectOneById(walter.getId(), Group.class));

		StepVerifier.create(loadedMono)
		            .verifyComplete();
	}

	@Test // DATACASS-574
	public void shouldDeleteCollectionOfEntities() {

		ReactiveCassandraBatchOperations batchOperations = new ReactiveCassandraBatchTemplate(template);

		Mono<Group> loadedMono = batchOperations
				.delete(Arrays.asList(walter, mike))
				.execute()
				.then(template.selectOneById(walter.getId(), Group.class));

		StepVerifier.create(loadedMono)
		            .verifyComplete();
	}

    @Test // DATACASS-574
    public void shouldDeleteMonoCollectionOfEntities() {

        ReactiveCassandraBatchOperations batchOperations = new ReactiveCassandraBatchTemplate(template);

        Mono<Group> loadedMono = batchOperations
                .delete(Mono.just(Arrays.asList(walter, mike)))
                .execute()
                .then(template.selectOneById(walter.getId(), Group.class));

        StepVerifier.create(loadedMono)
                    .verifyComplete();
    }

	@Test // DATACASS-574
	public void shouldApplyTimestampToAllEntities() {

		walter.setEmail("walter@white.com");
		mike.setEmail("mike@sauls.com");

		long timestamp = (System.currentTimeMillis() + TimeUnit.DAYS.toMillis(1)) * 1000;

		ReactiveCassandraBatchOperations batchOperations = new ReactiveCassandraBatchTemplate(template);
		Mono<ReactiveResultSet> resultSet = batchOperations
				.insert(walter)
				.insert(mike)
				.withTimestamp(timestamp)
				.execute()
				.then(template.getReactiveCqlOperations()
				              .queryForResultSet("SELECT writetime(email) FROM group;"));

		StepVerifier.create(resultSet.flatMapMany(ReactiveResultSet::availableRows))
		            .assertNext(row -> assertThat(row.getLong(0)).isEqualTo(timestamp))
		            .assertNext(row -> assertThat(row.getLong(0)).isEqualTo(timestamp))
		            .verifyComplete();
	}

	@Test // DATACASS-574
	public void shouldNotExecuteTwice() {

		ReactiveCassandraBatchOperations batchOperations = new ReactiveCassandraBatchTemplate(template);
		StepVerifier.create(
			batchOperations.insert(walter)
			               .execute()
			               .then(Mono.fromRunnable(batchOperations::execute))
		).verifyError(IllegalStateException.class);
	}

	@Test // DATACASS-574
	public void shouldNotAllowModificationAfterExecution() {

		ReactiveCassandraBatchOperations batchOperations = new ReactiveCassandraBatchTemplate(template);
		StepVerifier.create(
			batchOperations.insert(walter)
                           .execute()
                           .then(Mono.fromRunnable(() -> batchOperations.update(new Group())))
		).verifyError(IllegalStateException.class);
	}

    @Test // DATACASS-574
    public void shouldNotAllowModificationAfterExecutionMonoCase() {

        ReactiveCassandraBatchOperations batchOperations = new ReactiveCassandraBatchTemplate(template);
        StepVerifier.create(
                batchOperations.insert(Mono.just(Arrays.asList(walter)))
                               .execute()
                               .then(Mono.fromRunnable(() -> batchOperations.update(new Group())))
        ).verifyError(IllegalStateException.class);
    }

	@Test // DATACASS-574
	public void shouldSupportMultithreadedMerge() {

		ReactiveCassandraBatchOperations batchOperations = new ReactiveCassandraBatchTemplate(template);
		Random random = new Random();

		for (int i = 0; i < 100; i++) {
			batchOperations.insert(Mono.just(Arrays.asList(
					new Group(new GroupKey("users", "0x1", "walter" + random.longs())),
					new Group(new GroupKey("users", "0x1", "walter" + random.longs())),
					new Group(new GroupKey("users", "0x1", "walter" + random.longs())),
					new Group(new GroupKey("users", "0x1", "walter" + random.longs()))
			)).publishOn(Schedulers.elastic()));
		}

		StepVerifier.create(batchOperations.execute()
		                                   .then(template.getReactiveCqlOperations()
		                                                 .queryForResultSet("SELECT TTL(email) FROM group;"))
		                                   .flatMapMany(ReactiveResultSet::availableRows))
		            .expectNextCount(402)
		            .verifyComplete();
	}
}
