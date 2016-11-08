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
package org.springframework.data.cassandra.repository.support;

import static org.assertj.core.api.Assertions.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cassandra.test.integration.AbstractKeyspaceCreatingIntegrationTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.core.ReactiveCassandraOperations;
import org.springframework.data.cassandra.domain.Person;
import org.springframework.data.cassandra.repository.ReactiveCassandraRepository;
import org.springframework.data.cassandra.test.integration.support.IntegrationTestConfig;
import org.springframework.data.repository.query.DefaultEvaluationContextProvider;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.TestSubscriber;

/**
 * Integration tests for {@link SimpleReactiveCassandraRepository}.
 * 
 * @author Mark Paluch
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class SimpleReactiveCassandraRepositoryIntegrationTests extends AbstractKeyspaceCreatingIntegrationTest
		implements BeanClassLoaderAware, BeanFactoryAware {

	@Configuration
	public static class Config extends IntegrationTestConfig {

		@Override
		public String[] getEntityBasePackages() {
			return new String[] { Person.class.getPackage().getName() };
		}
	}

	@Autowired private ReactiveCassandraOperations operations;

	ReactiveCassandraRepositoryFactory factory;
	ClassLoader classLoader;
	BeanFactory beanFactory;
	PersonRepostitory repository;

	Person dave, oliver, carter, boyd;

	@Override
	public void setBeanClassLoader(ClassLoader classLoader) {
		this.classLoader = classLoader == null ? org.springframework.util.ClassUtils.getDefaultClassLoader() : classLoader;
	}

	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.beanFactory = beanFactory;
	}

	@Before
	public void setUp() {

		factory = new ReactiveCassandraRepositoryFactory(operations);
		factory.setRepositoryBaseClass(SimpleReactiveCassandraRepository.class);
		factory.setBeanClassLoader(classLoader);
		factory.setBeanFactory(beanFactory);
		factory.setEvaluationContextProvider(DefaultEvaluationContextProvider.INSTANCE);

		repository = factory.getRepository(PersonRepostitory.class);

		repository.deleteAll().block();

		dave = new Person("42", "Dave", "Matthews");
		oliver = new Person("4", "Oliver August", "Matthews");
		carter = new Person("49", "Carter", "Beauford");
		boyd = new Person("45", "Boyd", "Tinsley");

		repository.save(Arrays.asList(oliver, dave, carter, boyd)).last().block();
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void existsByIdShouldReturnTrueForExistingObject() {

		Boolean exists = repository.exists(dave.getId()).block();

		assertThat(exists).isTrue();
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void existsByIdShouldReturnFalseForAbsentObject() {

		TestSubscriber<Boolean> testSubscriber = TestSubscriber.subscribe(repository.exists("unknown"));

		testSubscriber.await().assertComplete().assertValues(false).assertNoError();
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void existsByMonoOfIdShouldReturnTrueForExistingObject() {

		Boolean exists = repository.exists(Mono.just(dave.getId())).block();
		assertThat(exists).isTrue();
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void existsByEmptyMonoOfIdShouldReturnEmptyMono() {

		TestSubscriber<Boolean> testSubscriber = TestSubscriber.subscribe(repository.exists(Mono.empty()));

		testSubscriber.await().assertComplete().assertNoValues().assertNoError();
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void findOneShouldReturnObject() {

		Person person = repository.findOne(dave.getId()).block();

		assertThat(person).isEqualTo(dave);
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void findOneShouldCompleteWithoutValueForAbsentObject() {

		TestSubscriber<Person> testSubscriber = TestSubscriber.subscribe(repository.findOne("unknown"));

		testSubscriber.await().assertComplete().assertNoValues().assertNoError();
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void findOneByMonoOfIdShouldReturnTrueForExistingObject() {

		Person person = repository.findOne(Mono.just(dave.getId())).block();

		assertThat(person).isEqualTo(dave);
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void findOneByEmptyMonoOfIdShouldReturnEmptyMono() {

		TestSubscriber<Person> testSubscriber = TestSubscriber.subscribe(repository.findOne(Mono.empty()));

		testSubscriber.await().assertComplete().assertNoValues().assertNoError();
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void findAllShouldReturnAllResults() {

		List<Person> persons = repository.findAll().collectList().block();

		assertThat(persons).hasSize(4);
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void findAllByIterableOfIdShouldReturnResults() {

		List<Person> persons = repository.findAll(Arrays.asList(dave.getId(), boyd.getId())).collectList().block();

		assertThat(persons).hasSize(2);
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void findAllByPublisherOfIdShouldReturnResults() {

		List<Person> persons = repository.findAll(Flux.just(dave.getId(), boyd.getId())).collectList().block();

		assertThat(persons).hasSize(2);
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void findAllByEmptyPublisherOfIdShouldReturnResults() {

		TestSubscriber<Person> testSubscriber = TestSubscriber.subscribe(repository.findAll(Flux.empty()));

		testSubscriber.await().assertComplete().assertNoValues().assertNoError();
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void countShouldReturnNumberOfRecords() {

		TestSubscriber<Long> testSubscriber = TestSubscriber.subscribe(repository.count());

		testSubscriber.await().assertComplete().assertValueCount(1).assertValues(4L).assertNoError();
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void insertEntityShouldInsertEntity() {

		repository.deleteAll().block();

		Person person = new Person("36", "Homer", "Simpson");

		TestSubscriber<Person> testSubscriber = TestSubscriber.subscribe(repository.insert(person));

		testSubscriber.await().assertComplete().assertValueCount(1).assertValues(person);
		repository.findAll().count().subscribeWith(TestSubscriber.create()).awaitAndAssertNextValues(1L);
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void insertShouldDeferredWrite() {

		repository.deleteAll().block();

		Person person = new Person("36", "Homer", "Simpson");

		repository.insert(person);

		repository.findAll().count().subscribeWith(TestSubscriber.create()).awaitAndAssertNextValues(0L);
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void insertIterableOfEntitiesShouldInsertEntity() {

		repository.deleteAll().block();

		TestSubscriber<Person> testSubscriber = TestSubscriber
				.subscribe(repository.insert(Arrays.asList(dave, oliver, boyd)));

		testSubscriber.await().assertComplete().assertValueCount(3);

		repository.findAll().count().subscribeWith(TestSubscriber.create()).awaitAndAssertNextValues(3L);
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void insertPublisherOfEntitiesShouldInsertEntity() {

		repository.deleteAll().block();

		TestSubscriber<Person> testSubscriber = TestSubscriber.subscribe(repository.insert(Flux.just(dave, oliver, boyd)));

		testSubscriber.await().assertComplete().assertValueCount(3);
		repository.findAll().count().subscribeWith(TestSubscriber.create()).awaitAndAssertNextValues(3L);
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void saveEntityShouldUpdateExistingEntity() {

		dave.setFirstname("Hello, Dave");
		dave.setLastname("Bowman");

		TestSubscriber<Person> testSubscriber = TestSubscriber.subscribe(repository.save(dave));

		testSubscriber.await().assertComplete().assertValueCount(1).assertValues(dave);

		Person loaded = repository.findOne(dave.getId()).block();

		assertThat(loaded.getFirstname()).isEqualTo(dave.getFirstname());
		assertThat(loaded.getLastname()).isEqualTo(dave.getLastname());
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void saveEntityShouldInsertNewEntity() {

		Person person = new Person("36", "Homer", "Simpson");

		TestSubscriber<Person> testSubscriber = TestSubscriber.subscribe(repository.save(person));

		testSubscriber.await().assertComplete().assertValueCount(1).assertValues(person);

		Person loaded = repository.findOne(person.getId()).block();

		assertThat(loaded).isEqualTo(person);
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void saveIterableOfNewEntitiesShouldInsertEntity() {

		repository.deleteAll().block();

		TestSubscriber<Person> testSubscriber = TestSubscriber
				.subscribe(repository.save(Arrays.asList(dave, oliver, boyd)));

		testSubscriber.await().assertComplete().assertValueCount(3);

		repository.findAll().count().subscribeWith(TestSubscriber.create()).awaitAndAssertNextValues(3L);
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void saveIterableOfMixedEntitiesShouldInsertEntity() {

		Person person = new Person("36", "Homer", "Simpson");

		dave.setFirstname("Hello, Dave");
		dave.setLastname("Bowman");

		TestSubscriber<Person> testSubscriber = TestSubscriber.subscribe(repository.save(Arrays.asList(person, dave)));

		testSubscriber.await().assertComplete().assertValueCount(2);

		Person persistentDave = repository.findOne(dave.getId()).block();
		assertThat(persistentDave).isEqualTo(dave);

		Person persistentHomer = repository.findOne(person.getId()).block();
		assertThat(persistentHomer).isEqualTo(person);
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void savePublisherOfEntitiesShouldInsertEntity() {

		repository.deleteAll().block();

		TestSubscriber<Person> testSubscriber = TestSubscriber.subscribe(repository.save(Flux.just(dave, oliver, boyd)));

		testSubscriber.await().assertComplete().assertValueCount(3);
		repository.findAll().count().subscribeWith(TestSubscriber.create()).awaitAndAssertNextValues(3L);
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void deleteAllShouldRemoveEntities() {

		repository.deleteAll().block();

		TestSubscriber<Person> testSubscriber = TestSubscriber.subscribe(repository.findAll());

		testSubscriber.await().assertComplete().assertValueCount(0);
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void deleteByIdShouldRemoveEntity() {

		TestSubscriber<Void> testSubscriber = TestSubscriber.subscribe(repository.delete(dave.getId()));

		testSubscriber.await().assertComplete().assertNoValues();

		TestSubscriber<Person> verificationSubscriber = TestSubscriber.subscribe(repository.findOne(dave.getId()));

		verificationSubscriber.await().assertComplete().assertNoValues();
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void deleteShouldRemoveEntity() {

		TestSubscriber<Void> testSubscriber = TestSubscriber.subscribe(repository.delete(dave));

		testSubscriber.await().assertComplete().assertNoValues();

		TestSubscriber<Person> verificationSubscriber = TestSubscriber.subscribe(repository.findOne(dave.getId()));

		verificationSubscriber.await().assertComplete().assertNoValues();
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void deleteIterableOfEntitiesShouldRemoveEntities() {

		TestSubscriber<Void> testSubscriber = TestSubscriber.subscribe(repository.delete(Arrays.asList(dave, boyd)));

		testSubscriber.await().assertComplete().assertNoValues();

		TestSubscriber<Person> verificationSubscriber = TestSubscriber.subscribe(repository.findOne(boyd.getId()));
		verificationSubscriber.await().assertComplete().assertNoValues();
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void deletePublisherOfEntitiesShouldRemoveEntities() {

		TestSubscriber<Void> testSubscriber = TestSubscriber.subscribe(repository.delete(Flux.just(dave, boyd)));

		testSubscriber.await().assertComplete().assertNoValues();

		TestSubscriber<Person> verificationSubscriber = TestSubscriber.subscribe(repository.findOne(boyd.getId()));
		verificationSubscriber.await().assertComplete().assertNoValues();
	}

	static interface PersonRepostitory extends ReactiveCassandraRepository<Person, String> {}
}
