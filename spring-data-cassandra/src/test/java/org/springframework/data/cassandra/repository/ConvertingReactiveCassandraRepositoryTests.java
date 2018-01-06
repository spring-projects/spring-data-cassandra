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
package org.springframework.data.cassandra.repository;

import static org.assertj.core.api.Assertions.*;

import io.reactivex.Flowable;
import io.reactivex.Maybe;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import rx.Observable;
import rx.Single;

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.domain.User;
import org.springframework.data.cassandra.repository.config.EnableReactiveCassandraRepositories;
import org.springframework.data.cassandra.repository.support.IntegrationTestConfig;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTest;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.data.repository.reactive.RxJava2CrudRepository;
import org.springframework.stereotype.Repository;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;

/**
 * Test for {@link ReactiveCassandraRepository} using reactive wrapper type conversion.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = ConvertingReactiveCassandraRepositoryTests.Config.class)
public class ConvertingReactiveCassandraRepositoryTests extends AbstractKeyspaceCreatingIntegrationTest {

	@EnableReactiveCassandraRepositories(includeFilters = @Filter(value = Repository.class),
			considerNestedRepositories = true)
	@Configuration
	public static class Config extends IntegrationTestConfig {

		@Override
		public String[] getEntityBasePackages() {
			return new String[] { User.class.getPackage().getName() };
		}
	}

	@Autowired Session session;
	@Autowired MixedUserRepository reactiveRepository;
	@Autowired UserRepostitory reactiveUserRepostitory;
	@Autowired RxJava1UserRepository rxJava1UserRepository;
	@Autowired RxJava2UserRepository rxJava2UserRepository;

	User dave, oliver, carter, boyd;

	@Before
	public void setUp() throws Exception {

		KeyspaceMetadata keyspace = session.getCluster().getMetadata().getKeyspace(session.getLoggedKeyspace());
		TableMetadata person = keyspace.getTable("person");

		if (person.getIndex("IX_person_lastname") == null) {
			session.execute("CREATE INDEX IX_person_lastname ON person (lastname);");
			Thread.sleep(500);
		}

		StepVerifier.create(reactiveRepository.deleteAll()).verifyComplete();

		dave = new User("42", "Dave", "Matthews");
		oliver = new User("4", "Oliver August", "Matthews");
		carter = new User("49", "Carter", "Beauford");
		boyd = new User("45", "Boyd", "Tinsley");

		StepVerifier.create(reactiveRepository.saveAll(Arrays.asList(oliver, dave, carter, boyd))).expectNextCount(4)
				.verifyComplete();
	}

	@Test // DATACASS-335
	public void reactiveStreamsMethodsShouldWork() {
		StepVerifier.create(reactiveUserRepostitory.existsById(dave.getId())).expectNext(true).verifyComplete();
	}

	@Test // DATACASS-335
	public void reactiveStreamsQueryMethodsShouldWork() {
		StepVerifier.create(reactiveUserRepostitory.findByLastname(boyd.getLastname())).expectNext(boyd).verifyComplete();
	}

	@Test // DATACASS-360
	public void dtoProjectionShouldWork() {

		StepVerifier.create(reactiveUserRepostitory.findProjectedByLastname(boyd.getLastname())).consumeNextWith(actual -> {

			assertThat(actual.firstname).isEqualTo(boyd.getFirstname());
			assertThat(actual.lastname).isEqualTo(boyd.getLastname());
		}).verifyComplete();
	}

	@Test // DATACASS-335
	public void simpleRxJava1MethodsShouldWork() {

		rxJava1UserRepository.existsById(dave.getId()) //
				.test() //
				.awaitTerminalEvent() //
				.assertResult(true) //
				.assertCompleted() //
				.assertNoErrors();
	}

	@Test // DATACASS-335
	public void existsWithSingleRxJava1IdMethodsShouldWork() {

		rxJava1UserRepository.existsById(Single.just(dave.getId())) //
				.test() //
				.awaitTerminalEvent() //
				.assertResult(true) //
				.assertCompleted() //
				.assertNoErrors();
	}

	@Test // DATACASS-335
	public void singleRxJava1QueryMethodShouldWork() {

		rxJava1UserRepository.findManyByLastname(dave.getLastname()) //
				.test() //
				.awaitTerminalEvent() //
				.assertValueCount(2) //
				.assertNoErrors() //
				.assertCompleted();
	}

	@Test // DATACASS-335
	public void singleProjectedRxJava1QueryMethodShouldWork() {

		List<ProjectedUser> values = rxJava1UserRepository.findProjectedByLastname(carter.getLastname()) //
				.test() //
				.awaitTerminalEvent() //
				.assertValueCount(1) //
				.assertCompleted() //
				.assertNoErrors() //
				.getOnNextEvents();

		ProjectedUser projectedUser = values.get(0);
		assertThat(projectedUser.getFirstname()).isEqualTo(carter.getFirstname());
	}

	@Test // DATACASS-335
	public void observableRxJava1QueryMethodShouldWork() {

		rxJava1UserRepository.findByLastname(boyd.getLastname()) //
				.test() //
				.awaitTerminalEvent() //
				.assertValue(boyd) //
				.assertNoErrors() //
				.assertCompleted();
	}

	@Test // DATACASS-398
	public void simpleRxJava2MethodsShouldWork() {

		rxJava2UserRepository.existsById(dave.getId()) //
				.test()//
				.assertValue(true) //
				.assertNoErrors() //
				.assertComplete() //
				.awaitTerminalEvent();
	}

	@Test // DATACASS-398
	public void existsWithSingleRxJava2IdMethodsShouldWork() {

		rxJava2UserRepository.existsById(io.reactivex.Single.just(dave.getId())).test() //
				.assertValue(true) //
				.assertNoErrors() //
				.assertComplete() //
				.awaitTerminalEvent();
	}

	@Test // DATACASS-398
	public void flowableRxJava2QueryMethodShouldWork() {

		rxJava2UserRepository.findManyByLastname(dave.getLastname()) //
				.test() //
				.assertValueCount(2) //
				.assertNoErrors() //
				.assertComplete() //
				.awaitTerminalEvent();
	}

	@Test // DATACASS-398
	public void singleProjectedRxJava2QueryMethodShouldWork() {

		rxJava2UserRepository.findProjectedByLastname(Maybe.just(carter.getLastname())) //
				.test() //
				.assertValue(actual -> {
					assertThat(actual.getFirstname()).isEqualTo(carter.getFirstname());
					return true;
				}) //
				.assertComplete() //
				.assertNoErrors() //
				.awaitTerminalEvent();
	}

	@Test // DATACASS-398
	public void observableProjectedRxJava2QueryMethodShouldWork() {

		rxJava2UserRepository.findProjectedByLastname(Single.just(carter.getLastname())) //
				.test() //
				.assertValue(actual -> {
					assertThat(actual.getFirstname()).isEqualTo(carter.getFirstname());
					return true;
				}) //
				.assertComplete() //
				.assertNoErrors() //
				.awaitTerminalEvent();
	}

	@Test // DATACASS-398
	public void maybeRxJava2QueryMethodShouldWork() {

		rxJava2UserRepository.findByLastname(boyd.getLastname()) //
				.test() //
				.assertValue(boyd) //
				.assertNoErrors() //
				.assertComplete() //
				.awaitTerminalEvent();
	}

	@Test // DATACASS-335
	public void mixedRepositoryShouldWork() {

		reactiveRepository.findByLastname(boyd.getLastname()) //
				.test() //
				.awaitTerminalEvent() //
				.assertValue(boyd) //
				.assertCompleted() //
				.assertNoErrors();
	}

	@Test // DATACASS-335
	public void shouldFindByIdByPublisherOfLastName() {

		StepVerifier.create(reactiveRepository.findByLastname(Single.just(this.carter.getLastname()))) //
				.expectNext(carter) //
				.verifyComplete();
	}

	@Repository
	interface UserRepostitory extends ReactiveCrudRepository<User, String> {

		Publisher<User> findByLastname(String lastname);

		Flux<UserDto> findProjectedByLastname(String lastname);
	}

	@Repository
	interface RxJava1UserRepository extends org.springframework.data.repository.Repository<User, String> {

		Observable<User> findManyByLastname(String lastname);

		Single<User> findByLastname(String lastname);

		Single<ProjectedUser> findProjectedByLastname(String lastname);

		Single<Boolean> existsById(String id);

		Single<Boolean> existsById(Single<String> id);
	}

	@Repository
	interface RxJava2UserRepository extends RxJava2CrudRepository<User, String> {

		Flowable<User> findManyByLastname(String lastname);

		Maybe<User> findByLastname(String lastname);

		io.reactivex.Single<ProjectedUser> findProjectedByLastname(Maybe<String> lastname);

		io.reactivex.Observable<ProjectedUser> findProjectedByLastname(Single<String> lastname);
	}

	@Repository
	interface MixedUserRepository extends ReactiveCassandraRepository<User, String> {

		Single<User> findByLastname(String lastname);

		Mono<User> findByLastname(Single<String> lastname);
	}

	interface ProjectedUser {

		String getId();

		String getFirstname();
	}

	static class UserDto {

		public String firstname, lastname;

		public UserDto(String firstname, String lastname) {

			this.firstname = firstname;
			this.lastname = lastname;
		}
	}
}
