/*
 * Copyright 2016 the original author or authors.
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

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cassandra.test.integration.AbstractKeyspaceCreatingIntegrationTest;
import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.core.ReactiveCassandraTemplate;
import org.springframework.data.cassandra.domain.Person;
import org.springframework.data.cassandra.repository.config.EnableReactiveCassandraRepositories;
import org.springframework.data.cassandra.test.integration.support.IntegrationTestConfig;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.data.repository.reactive.RxJavaCrudRepository;
import org.springframework.stereotype.Repository;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;

import reactor.core.publisher.Mono;
import reactor.test.TestSubscriber;
import rx.Observable;
import rx.Single;

/**
 * Test for {@link ReactiveCassandraRepository} using reactive wrapper type conversion.
 *
 * @author Mark Paluch
 * @soundtrack Dj Marc - Euromix 97 Part 1
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
			return new String[] { Person.class.getPackage().getName() };
		}
	}

	@Autowired Session session;
	@Autowired ReactiveCassandraTemplate template;
	@Autowired MixedPersonRepostitory reactiveRepository;
	@Autowired PersonRepostitory reactivePersonRepostitory;
	@Autowired RxJavaPersonRepostitory rxJavaPersonRepostitory;

	Person dave, oliver, carter, boyd;

	@Before
	public void setUp() throws Exception {

		KeyspaceMetadata keyspace = session.getCluster().getMetadata().getKeyspace(session.getLoggedKeyspace());
		TableMetadata person = keyspace.getTable("person");

		if (person.getIndex("IX_person_lastname") == null) {

			session.execute("CREATE INDEX IX_person_lastname ON person (lastname);");
			Thread.sleep(500);
		}

		reactiveRepository.deleteAll().block();

		dave = new Person("42", "Dave", "Matthews");
		oliver = new Person("4", "Oliver August", "Matthews");
		carter = new Person("49", "Carter", "Beauford");
		boyd = new Person("45", "Boyd", "Tinsley");

		TestSubscriber<Person> subscriber = TestSubscriber.create();
		reactiveRepository.save(Arrays.asList(oliver, dave, carter, boyd)).subscribe(subscriber);

		subscriber.await().assertComplete().assertNoError();
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void reactiveStreamsMethodsShouldWork() throws InterruptedException {

		TestSubscriber<Boolean> subscriber = TestSubscriber.subscribe(reactivePersonRepostitory.exists(dave.getId()));

		subscriber.awaitAndAssertNextValueCount(1).assertNoError().assertValues(true);
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void reactiveStreamsQueryMethodsShouldWork() {

		TestSubscriber<Person> subscriber = TestSubscriber
				.subscribe(reactivePersonRepostitory.findByLastname(boyd.getLastname()));

		subscriber.awaitAndAssertNextValueCount(1).assertValues(boyd);
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void simpleRxJavaMethodsShouldWork() {

		rx.observers.TestSubscriber<Boolean> subscriber = new rx.observers.TestSubscriber<>();
		rxJavaPersonRepostitory.exists(dave.getId()).subscribe(subscriber);

		subscriber.awaitTerminalEvent();
		subscriber.assertCompleted();
		subscriber.assertNoErrors();
		subscriber.assertValue(true);
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void existsWithSingleRxJavaIdMethodsShouldWork() {

		rx.observers.TestSubscriber<Boolean> subscriber = new rx.observers.TestSubscriber<>();
		rxJavaPersonRepostitory.exists(Single.just(dave.getId())).subscribe(subscriber);

		subscriber.awaitTerminalEvent();
		subscriber.assertCompleted();
		subscriber.assertNoErrors();
		subscriber.assertValue(true);
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void singleRxJavaQueryMethodShouldWork() {

		rx.observers.TestSubscriber<Person> subscriber = new rx.observers.TestSubscriber<>();
		rxJavaPersonRepostitory.findManyByLastname(dave.getLastname()).subscribe(subscriber);

		subscriber.awaitTerminalEvent();
		subscriber.assertNoErrors();
		subscriber.assertCompleted();
		subscriber.assertValueCount(2);
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void singleProjectedRxJavaQueryMethodShouldWork() {

		rx.observers.TestSubscriber<ProjectedPerson> subscriber = new rx.observers.TestSubscriber<>();
		rxJavaPersonRepostitory.findProjectedByLastname(carter.getLastname()).subscribe(subscriber);

		subscriber.awaitTerminalEvent();
		subscriber.assertCompleted();
		subscriber.assertNoErrors();

		ProjectedPerson projectedPerson = subscriber.getOnNextEvents().get(0);
		assertThat(projectedPerson.getFirstname()).isEqualTo(carter.getFirstname());
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void observableRxJavaQueryMethodShouldWork() {

		rx.observers.TestSubscriber<Person> subscriber = new rx.observers.TestSubscriber<>();
		rxJavaPersonRepostitory.findByLastname(boyd.getLastname()).subscribe(subscriber);

		subscriber.awaitTerminalEvent();
		subscriber.assertCompleted();
		subscriber.assertNoErrors();
		subscriber.assertValue(boyd);
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void mixedRepositoryShouldWork() {

		Person value = reactiveRepository.findByLastname(boyd.getLastname()).toBlocking().value();

		assertThat(value).isEqualTo(boyd);
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void shouldFindOneByPublisherOfLastName() {

		Person carter = reactiveRepository.findByLastname(Single.just(this.carter.getLastname())).block();

		assertThat(carter.getFirstname()).isEqualTo(this.carter.getFirstname());
	}

	@Repository
	interface PersonRepostitory extends ReactiveCrudRepository<Person, String> {

		Publisher<Person> findByLastname(String lastname);
	}

	@Repository
	interface RxJavaPersonRepostitory extends RxJavaCrudRepository<Person, String> {

		Observable<Person> findManyByLastname(String lastname);

		Single<Person> findByLastname(String lastname);

		Single<ProjectedPerson> findProjectedByLastname(String lastname);
	}

	@Repository
	interface MixedPersonRepostitory extends ReactiveCassandraRepository<Person, String> {

		Single<Person> findByLastname(String lastname);

		Mono<Person> findByLastname(Single<String> lastname);
	}

	interface ProjectedPerson {

		String getId();

		String getFirstname();
	}
}
