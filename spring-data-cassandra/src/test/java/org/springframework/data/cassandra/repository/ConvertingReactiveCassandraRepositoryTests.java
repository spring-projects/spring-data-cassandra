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
package org.springframework.data.cassandra.repository;

import static org.assertj.core.api.Assertions.*;

import io.reactivex.rxjava3.core.Single;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.domain.User;
import org.springframework.data.cassandra.repository.config.EnableReactiveCassandraRepositories;
import org.springframework.data.cassandra.repository.support.AbstractSpringDataEmbeddedCassandraIntegrationTest;
import org.springframework.data.cassandra.repository.support.IntegrationTestConfig;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;

/**
 * Test for {@link ReactiveCassandraRepository} using reactive wrapper type conversion.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
@SpringJUnitConfig(classes = ConvertingReactiveCassandraRepositoryTests.Config.class)
class ConvertingReactiveCassandraRepositoryTests extends AbstractSpringDataEmbeddedCassandraIntegrationTest {

	@EnableReactiveCassandraRepositories(includeFilters = @Filter(value = Repository.class),
			considerNestedRepositories = true)
	@Configuration
	public static class Config extends IntegrationTestConfig {

		@Override
		public String[] getEntityBasePackages() {
			return new String[] { User.class.getPackage().getName() };
		}
	}

	@Autowired CqlSession session;
	@Autowired MixedUserRepository reactiveRepository;
	@Autowired UserRepostitory reactiveUserRepostitory;
	@Autowired RxJava3UserRepository rxJava3UserRepository;

	private User dave;
	private User oliver;
	private User carter;
	private User boyd;

	@BeforeEach
	void setUp() throws Exception {

		TableMetadata users = session.getKeyspace().flatMap(it -> session.getMetadata().getKeyspace(it))
				.flatMap(it -> it.getTable(CqlIdentifier.fromCql("users"))).get();

		if (users.getIndexes().containsKey(CqlIdentifier.fromCql("IX_lastname"))) {
			session.execute("CREATE INDEX IX_lastname ON users (lastname);");
			Thread.sleep(500);
		}

		reactiveRepository.deleteAll().as(StepVerifier::create).verifyComplete();

		dave = new User("42", "Dave", "Matthews");
		oliver = new User("4", "Oliver August", "Matthews");
		carter = new User("49", "Carter", "Beauford");
		boyd = new User("45", "Boyd", "Tinsley");

		reactiveRepository.saveAll(Arrays.asList(oliver, dave, carter, boyd)).as(StepVerifier::create).expectNextCount(4)
				.verifyComplete();
	}

	@Test // DATACASS-335
	void reactiveStreamsMethodsShouldWork() {
		reactiveUserRepostitory.existsById(dave.getId()).as(StepVerifier::create).expectNext(true).verifyComplete();
	}

	@Test // DATACASS-335
	void reactiveStreamsQueryMethodsShouldWork() {
		StepVerifier.create(reactiveUserRepostitory.findByLastname(boyd.getLastname())).expectNext(boyd).verifyComplete();
	}

	@Test // DATACASS-360
	void dtoProjectionShouldWork() {

		reactiveUserRepostitory.findProjectedByLastname(boyd.getLastname()).as(StepVerifier::create)
				.consumeNextWith(actual -> {

					assertThat(actual.firstname).isEqualTo(boyd.getFirstname());
					assertThat(actual.lastname).isEqualTo(boyd.getLastname());
				}).verifyComplete();
	}

	@Test // DATACASS-335
	void simpleRxJava3MethodsShouldWork() throws InterruptedException {

		rxJava3UserRepository.existsById(dave.getId()) //
				.test() //
				.await() //
				.assertResult(true) //
				.assertComplete() //
				.assertNoErrors();
	}

	@Test // DATACASS-335
	void existsWithSingleRxJava3IdMethodsShouldWork() throws InterruptedException {

		rxJava3UserRepository.existsById(Single.just(dave.getId())) //
				.test() //
				.await() //
				.assertResult(true) //
				.assertComplete() //
				.assertNoErrors();
	}

	@Test // DATACASS-335
	void singleRxJava3QueryMethodShouldWork() throws InterruptedException {

		rxJava3UserRepository.findManyByLastname(dave.getLastname()) //
				.test() //
				.await() //
				.assertValueCount(2) //
				.assertNoErrors() //
				.assertComplete();
	}

	@Test // DATACASS-335
	void singleProjectedRxJava3QueryMethodShouldWork() throws InterruptedException {

		List<ProjectedUser> values = rxJava3UserRepository.findProjectedByLastname(carter.getLastname()) //
				.test() //
				.await() //
				.assertValueCount(1) //
				.assertComplete() //
				.assertNoErrors() //
				.values();

		ProjectedUser projectedUser = values.get(0);
		assertThat(projectedUser.getFirstname()).isEqualTo(carter.getFirstname());
	}

	@Test // DATACASS-335
	void observableRxJava3QueryMethodShouldWork() throws InterruptedException {

		rxJava3UserRepository.findByLastname(boyd.getLastname()) //
				.test() //
				.await() //
				.assertValue(boyd) //
				.assertNoErrors() //
				.assertComplete();
	}

	@Test // DATACASS-335
	void mixedRepositoryShouldWork() throws InterruptedException {

		reactiveRepository.findByLastname(boyd.getLastname()) //
				.test() //
				.await() //
				.assertValue(boyd) //
				.assertComplete() //
				.assertNoErrors();
	}

	@Test // DATACASS-335
	void shouldFindByIdByPublisherOfLastName() {

		reactiveRepository.findByLastname(Single.just(this.carter.getLastname())).as(StepVerifier::create) //
				.expectNext(carter) //
				.verifyComplete();
	}

	@Repository
	interface UserRepostitory extends ReactiveCrudRepository<User, String> {

		Publisher<User> findByLastname(String lastname);

		Flux<UserDto> findProjectedByLastname(String lastname);
	}

	@Repository
	interface RxJava3UserRepository extends org.springframework.data.repository.Repository<User, String> {

		io.reactivex.rxjava3.core.Observable<User> findManyByLastname(String lastname);

		io.reactivex.rxjava3.core.Single<User> findByLastname(String lastname);

		io.reactivex.rxjava3.core.Single<ProjectedUser> findProjectedByLastname(String lastname);

		io.reactivex.rxjava3.core.Single<Boolean> existsById(String id);

		io.reactivex.rxjava3.core.Single<Boolean> existsById(io.reactivex.rxjava3.core.Single<String> id);
	}

	@Repository
	interface MixedUserRepository extends ReactiveCassandraRepository<User, String> {

		Single<User> findByLastname(String lastname);

		Mono<User> findByLastname(Single<String> lastname);
	}

	private interface ProjectedUser {

		String getId();

		String getFirstname();
	}

	private static class UserDto {

		private String firstname;
		private String lastname;

		public UserDto(String firstname, String lastname) {

			this.firstname = firstname;
			this.lastname = lastname;
		}
	}
}
