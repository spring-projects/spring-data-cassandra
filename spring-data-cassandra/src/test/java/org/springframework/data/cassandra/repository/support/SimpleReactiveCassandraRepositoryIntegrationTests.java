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
package org.springframework.data.cassandra.repository.support;

import static org.assertj.core.api.Assertions.*;
import static org.junit.Assume.*;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.core.ReactiveCassandraOperations;
import org.springframework.data.cassandra.core.query.CassandraPageRequest;
import org.springframework.data.cassandra.domain.User;
import org.springframework.data.cassandra.domain.UserToken;
import org.springframework.data.cassandra.repository.ReactiveCassandraRepository;
import org.springframework.data.cassandra.support.CassandraVersion;
import org.springframework.data.cassandra.test.util.IntegrationTestsSupport;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;
import org.springframework.data.util.Version;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import com.datastax.oss.driver.api.core.CqlSession;

/**
 * Integration tests for {@link SimpleReactiveCassandraRepository}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Jens Schauder
 */
@SpringJUnitConfig
public class SimpleReactiveCassandraRepositoryIntegrationTests extends IntegrationTestsSupport
		implements BeanClassLoaderAware, BeanFactoryAware {

	private static final Version CASSANDRA_3 = Version.parse("3.0");

	@Configuration
	public static class Config extends IntegrationTestConfig {

		@Override
		public String[] getEntityBasePackages() {
			return new String[] { User.class.getPackage().getName() };
		}
	}

	@Autowired private CqlSession session;
	@Autowired private ReactiveCassandraOperations operations;

	private Version cassandraVersion;
	private BeanFactory beanFactory;
	private ClassLoader classLoader;
	private ReactiveCassandraRepositoryFactory factory;
	private UserRepostitory repository;

	private User dave, oliver, carter, boyd;

	@Override
	public void setBeanClassLoader(ClassLoader classLoader) {
		this.classLoader = classLoader == null ? org.springframework.util.ClassUtils.getDefaultClassLoader() : classLoader;
	}

	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.beanFactory = beanFactory;
	}

	@BeforeEach
	void setUp() {

		factory = new ReactiveCassandraRepositoryFactory(operations);
		factory.setRepositoryBaseClass(SimpleReactiveCassandraRepository.class);
		factory.setBeanClassLoader(classLoader);
		factory.setBeanFactory(beanFactory);

		repository = factory.getRepository(UserRepostitory.class);

		cassandraVersion = CassandraVersion.get(session);

		deleteAll();

		dave = new User("42", "Dave", "Matthews");
		oliver = new User("4", "Oliver August", "Matthews");
		carter = new User("49", "Carter", "Beauford");
		boyd = new User("45", "Boyd", "Tinsley");
	}

	private void insertTestData() {
		repository.saveAll(Arrays.asList(oliver, dave, carter, boyd)).as(StepVerifier::create).expectNextCount(4)
				.verifyComplete();
	}

	private void deleteAll() {
		repository.deleteAll().as(StepVerifier::create).verifyComplete();
	}

	@Test // DATACASS-335
	void existsByIdShouldReturnTrueForExistingObject() {

		insertTestData();

		repository.existsById(dave.getId()).as(StepVerifier::create).expectNext(true).verifyComplete();
	}

	@Test // DATACASS-335
	void existsByIdShouldReturnFalseForAbsentObject() {
		repository.existsById("unknown").as(StepVerifier::create).expectNext(false).verifyComplete();
	}

	@Test // DATACASS-335
	void existsByMonoOfIdShouldReturnTrueForExistingObject() {

		insertTestData();

		repository.existsById(Mono.just(dave.getId())).as(StepVerifier::create).expectNext(true).verifyComplete();
	}

	@Test // DATACASS-462
	void existsByIdUsingFluxShouldReturnTrueForExistingObject() {

		insertTestData();

		repository.existsById(Flux.just(dave.getId(), oliver.getId())).as(StepVerifier::create).expectNext(true)
				.verifyComplete();
	}

	@Test // DATACASS-335
	void existsByEmptyMonoOfIdShouldReturnEmptyMono() {
		repository.existsById(Mono.empty()).as(StepVerifier::create).verifyComplete();
	}

	@Test // DATACASS-335
	void findByIdShouldReturnObject() {

		insertTestData();

		repository.findById(dave.getId()).as(StepVerifier::create).expectNext(dave).verifyComplete();
	}

	@Test // DATACASS-335
	void findByIdShouldCompleteWithoutValueForAbsentObject() {
		repository.findById("unknown").as(StepVerifier::create).verifyComplete();
	}

	@Test // DATACASS-335
	void findByIdByMonoOfIdShouldReturnTrueForExistingObject() {

		insertTestData();

		repository.findById(Mono.just(dave.getId())).as(StepVerifier::create).expectNext(dave).verifyComplete();
	}

	@Test // DATACASS-462
	void findByIdUsingFluxShouldReturnTrueForExistingObject() {

		insertTestData();

		repository.findById(Flux.just(dave.getId(), oliver.getId())).as(StepVerifier::create).expectNext(dave)
				.verifyComplete();
	}

	@Test // DATACASS-335
	void findByIdByEmptyMonoOfIdShouldReturnEmptyMono() {
		repository.findById(Mono.empty()).as(StepVerifier::create).verifyComplete();
	}

	@Test // DATACASS-335
	void findAllShouldReturnAllResults() {

		insertTestData();

		repository.findAll().as(StepVerifier::create).expectNextCount(4).verifyComplete();
	}

	@Test // DATACASS-335
	void findAllByIterableOfIdShouldReturnResults() {

		insertTestData();

		repository.findAllById(Arrays.asList(dave.getId(), boyd.getId())).as(StepVerifier::create) //
				.expectNextCount(2) //
				.verifyComplete();
	}

	@Test // DATACASS-335
	void findAllByPublisherOfIdShouldReturnResults() {

		insertTestData();

		repository.findAllById(Flux.just(dave.getId(), boyd.getId())).as(StepVerifier::create) //
				.expectNextCount(2) //
				.verifyComplete();
	}

	@Test // DATACASS-335
	void findAllByEmptyPublisherOfIdShouldReturnResults() {
		repository.findAllById(Flux.empty()).as(StepVerifier::create).verifyComplete();
	}

	@Test // DATACASS-700
	void findAllWithPagingAndSorting() {

		assumeTrue(cassandraVersion.isGreaterThan(CASSANDRA_3));

		UserTokenRepostitory repository = factory.getRepository(UserTokenRepostitory.class);
		repository.deleteAll();

		UUID id = UUID.randomUUID();
		List<UserToken> users = IntStream.range(0, 100).mapToObj(value -> {

			UserToken token = new UserToken();
			token.setUserId(id);
			token.setToken(UUID.randomUUID());

			return token;
		}).collect(Collectors.toList());

		repository.saveAll(users).then().as(StepVerifier::create).verifyComplete();

		List<UserToken> result = new ArrayList<>();
		Slice<UserToken> slice = repository.findAllByUserId(id, CassandraPageRequest.first(10, Sort.by("token")))
				.block(Duration.ofSeconds(10));

		while (!slice.isEmpty() || slice.hasNext()) {

			result.addAll(slice.getContent());
			slice = repository.findAllByUserId(id, slice.nextPageable()).block(Duration.ofSeconds(10));
		}

		assertThat(result).hasSize(100);
	}

	@Test // DATACASS-335
	void countShouldReturnNumberOfRecords() {

		insertTestData();

		repository.count().as(StepVerifier::create).expectNext(4L).verifyComplete();
	}

	@Test // DATACASS-335
	void insertEntityShouldInsertEntity() {

		User person = new User("36", "Homer", "Simpson");

		repository.insert(person).as(StepVerifier::create).expectNext(person).verifyComplete();

		repository.findAll().as(StepVerifier::create).expectNextCount(1L).verifyComplete();
	}

	@Test // DATACASS-335
	void insertShouldDeferredWrite() {

		User person = new User("36", "Homer", "Simpson");

		repository.insert(person);

		repository.findAll().as(StepVerifier::create).expectNextCount(0L).verifyComplete();
	}

	@Test // DATACASS-335
	void insertIterableOfEntitiesShouldInsertEntity() {

		repository.insert(Arrays.asList(dave, oliver, boyd)).as(StepVerifier::create).expectNextCount(3L).verifyComplete();

		repository.findAll().as(StepVerifier::create).expectNextCount(3L).verifyComplete();
	}

	@Test // DATACASS-335
	void insertPublisherOfEntitiesShouldInsertEntity() {

		repository.insert(Flux.just(dave, oliver, boyd)).as(StepVerifier::create).expectNextCount(3L).verifyComplete();

		repository.findAll().as(StepVerifier::create).expectNextCount(3L).verifyComplete();
	}

	@Test // DATACASS-335
	void saveEntityShouldUpdateExistingEntity() {

		dave.setFirstname("Hello, Dave");
		dave.setLastname("Bowman");

		repository.save(dave).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		repository.findById(dave.getId()).as(StepVerifier::create).consumeNextWith(actual -> {

			assertThat(actual.getFirstname()).isEqualTo(dave.getFirstname());
			assertThat(actual.getLastname()).isEqualTo(dave.getLastname());
		}).verifyComplete();
	}

	@Test // DATACASS-335
	void saveEntityShouldInsertNewEntity() {

		User person = new User("36", "Homer", "Simpson");

		repository.save(person).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		repository.findById(person.getId()).as(StepVerifier::create).expectNext(person).verifyComplete();
	}

	@Test // DATACASS-335
	void saveIterableOfNewEntitiesShouldInsertEntity() {

		repository.saveAll(Arrays.asList(dave, oliver, boyd)).as(StepVerifier::create).expectNextCount(3).verifyComplete();

		repository.findAll().as(StepVerifier::create).expectNextCount(3L).verifyComplete();
	}

	@Test // DATACASS-335
	void saveIterableOfMixedEntitiesShouldInsertEntity() {

		User person = new User("36", "Homer", "Simpson");

		dave.setFirstname("Hello, Dave");
		dave.setLastname("Bowman");

		repository.saveAll(Arrays.asList(person, dave)).as(StepVerifier::create).expectNextCount(2).verifyComplete();

		repository.findById(dave.getId()).as(StepVerifier::create).expectNext(dave).verifyComplete();

		repository.findById(person.getId()).as(StepVerifier::create).expectNext(person).verifyComplete();
	}

	@Test // DATACASS-335
	void savePublisherOfEntitiesShouldInsertEntity() {

		repository.saveAll(Flux.just(dave, oliver, boyd)).as(StepVerifier::create).expectNextCount(3).verifyComplete();

		repository.findAll().as(StepVerifier::create).expectNextCount(3L).verifyComplete();
	}

	@Test // DATACASS-335
	void deleteAllShouldRemoveEntities() {

		insertTestData();

		repository.deleteAll().as(StepVerifier::create).verifyComplete();

		repository.findAll().as(StepVerifier::create).verifyComplete();
	}

	@Test // DATACASS-335
	void deleteByIdShouldRemoveEntity() {

		insertTestData();

		repository.deleteById(dave.getId()).as(StepVerifier::create).verifyComplete();

		repository.findById(dave.getId()).as(StepVerifier::create).expectNextCount(0).verifyComplete();
	}

	@Test // DATACASS-462
	void deleteByIdUsingMonoShouldRemoveEntity() {

		insertTestData();

		repository.deleteById(Mono.just(dave.getId())).as(StepVerifier::create).verifyComplete();

		repository.existsById(dave.getId()).as(StepVerifier::create).expectNext(false).verifyComplete();
	}

	@Test // DATACASS-825
	void deleteAllByIdRemovesEntities() {

		insertTestData();

		repository.deleteAllById(Arrays.asList(dave.getId(), carter.getId())).as(StepVerifier::create).verifyComplete();

		repository.existsById(dave.getId()).as(StepVerifier::create).expectNext(false).verifyComplete();
		repository.existsById(carter.getId()).as(StepVerifier::create).expectNext(false).verifyComplete();
	}

	@Test // DATACASS-462
	void deleteByIdUsingFluxShouldRemoveFirstEntity() {

		insertTestData();

		repository.deleteById(Flux.just(dave.getId(), oliver.getId())).as(StepVerifier::create).verifyComplete();

		repository.existsById(dave.getId()).as(StepVerifier::create).expectNext(false).verifyComplete();
		repository.existsById(oliver.getId()).as(StepVerifier::create).expectNext(true).verifyComplete();
	}

	@Test // DATACASS-335
	void deleteShouldRemoveEntity() {

		insertTestData();

		repository.delete(dave).as(StepVerifier::create).verifyComplete();

		repository.findById(dave.getId()).as(StepVerifier::create).expectNextCount(0).verifyComplete();
	}

	@Test // DATACASS-335
	void deleteIterableOfEntitiesShouldRemoveEntities() {

		insertTestData();

		repository.deleteAll(Arrays.asList(dave, boyd)).as(StepVerifier::create).verifyComplete();

		repository.findById(boyd.getId()).as(StepVerifier::create).expectNextCount(0).verifyComplete();
	}

	@Test // DATACASS-335
	void deletePublisherOfEntitiesShouldRemoveEntities() {

		insertTestData();

		repository.deleteAll(Flux.just(dave, boyd)).as(StepVerifier::create).verifyComplete();

		repository.findById(boyd.getId()).as(StepVerifier::create).expectNextCount(0).verifyComplete();
	}

	interface UserRepostitory extends ReactiveCassandraRepository<User, String> {}

	interface UserTokenRepostitory extends ReactiveCassandraRepository<UserToken, UUID> {
		Mono<Slice<UserToken>> findAllByUserId(UUID id, Pageable pageRequest);
	}

}
