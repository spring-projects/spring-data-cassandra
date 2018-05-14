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
package org.springframework.data.cassandra.repository.support;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.core.ReactiveCassandraOperations;
import org.springframework.data.cassandra.domain.User;
import org.springframework.data.cassandra.repository.ReactiveCassandraRepository;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTest;
import org.springframework.data.repository.query.ExtensionAwareQueryMethodEvaluationContextProvider;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * Integration tests for {@link SimpleReactiveCassandraRepository}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
@RunWith(SpringRunner.class)
@ContextConfiguration
public class SimpleReactiveCassandraRepositoryIntegrationTests extends AbstractKeyspaceCreatingIntegrationTest
		implements BeanClassLoaderAware, BeanFactoryAware {

	@Configuration
	public static class Config extends IntegrationTestConfig {

		@Override
		public String[] getEntityBasePackages() {
			return new String[] { User.class.getPackage().getName() };
		}
	}

	@Autowired
	private ReactiveCassandraOperations operations;

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

	@Before
	public void setUp() {

		factory = new ReactiveCassandraRepositoryFactory(operations);
		factory.setRepositoryBaseClass(SimpleReactiveCassandraRepository.class);
		factory.setBeanClassLoader(classLoader);
		factory.setBeanFactory(beanFactory);
		factory.setEvaluationContextProvider(ExtensionAwareQueryMethodEvaluationContextProvider.DEFAULT);

		repository = factory.getRepository(UserRepostitory.class);

		deleteAll();

		dave = new User("42", "Dave", "Matthews");
		oliver = new User("4", "Oliver August", "Matthews");
		carter = new User("49", "Carter", "Beauford");
		boyd = new User("45", "Boyd", "Tinsley");
	}

	private void insertTestData() {
		StepVerifier.create(repository.saveAll(Arrays.asList(oliver, dave, carter, boyd))).expectNextCount(4)
				.verifyComplete();
	}

	private void deleteAll() {
		StepVerifier.create(repository.deleteAll()).verifyComplete();
	}

	@Test // DATACASS-335
	public void existsByIdShouldReturnTrueForExistingObject() {

		insertTestData();

		StepVerifier.create(repository.existsById(dave.getId())).expectNext(true).verifyComplete();
	}

	@Test // DATACASS-335
	public void existsByIdShouldReturnFalseForAbsentObject() {
		StepVerifier.create(repository.existsById("unknown")).expectNext(false).verifyComplete();
	}

	@Test // DATACASS-335
	public void existsByMonoOfIdShouldReturnTrueForExistingObject() {

		insertTestData();

		StepVerifier.create(repository.existsById(Mono.just(dave.getId()))).expectNext(true).verifyComplete();
	}

	@Test // DATACASS-462
	public void existsByIdUsingFluxShouldReturnTrueForExistingObject() {

		insertTestData();

		StepVerifier.create(repository.existsById(Flux.just(dave.getId(), oliver.getId()))).expectNext(true)
				.verifyComplete();
	}

	@Test // DATACASS-335
	public void existsByEmptyMonoOfIdShouldReturnEmptyMono() {
		StepVerifier.create(repository.existsById(Mono.empty())).verifyComplete();
	}

	@Test // DATACASS-335
	public void findByIdShouldReturnObject() {

		insertTestData();

		StepVerifier.create(repository.findById(dave.getId())).expectNext(dave).verifyComplete();
	}

	@Test // DATACASS-335
	public void findByIdShouldCompleteWithoutValueForAbsentObject() {
		StepVerifier.create(repository.findById("unknown")).verifyComplete();
	}

	@Test // DATACASS-335
	public void findByIdByMonoOfIdShouldReturnTrueForExistingObject() {

		insertTestData();

		StepVerifier.create(repository.findById(Mono.just(dave.getId()))).expectNext(dave).verifyComplete();
	}

	@Test // DATACASS-462
	public void findByIdUsingFluxShouldReturnTrueForExistingObject() {

		insertTestData();

		StepVerifier.create(repository.findById(Flux.just(dave.getId(), oliver.getId()))).expectNext(dave).verifyComplete();
	}

	@Test // DATACASS-335
	public void findByIdByEmptyMonoOfIdShouldReturnEmptyMono() {
		StepVerifier.create(repository.findById(Mono.empty())).verifyComplete();
	}

	@Test // DATACASS-335
	public void findAllShouldReturnAllResults() {

		insertTestData();

		StepVerifier.create(repository.findAll()).expectNextCount(4).verifyComplete();
	}

	@Test // DATACASS-335
	public void findAllByIterableOfIdShouldReturnResults() {

		insertTestData();

		StepVerifier.create(repository.findAllById(Arrays.asList(dave.getId(), boyd.getId()))) //
				.expectNextCount(2) //
				.verifyComplete();
	}

	@Test // DATACASS-335
	public void findAllByPublisherOfIdShouldReturnResults() {

		insertTestData();

		StepVerifier.create(repository.findAllById(Flux.just(dave.getId(), boyd.getId()))) //
				.expectNextCount(2) //
				.verifyComplete();
	}

	@Test // DATACASS-335
	public void findAllByEmptyPublisherOfIdShouldReturnResults() {
		StepVerifier.create(repository.findAllById(Flux.empty())).verifyComplete();
	}

	@Test // DATACASS-335
	public void countShouldReturnNumberOfRecords() {

		insertTestData();

		StepVerifier.create(repository.count()).expectNext(4L).verifyComplete();
	}

	@Test // DATACASS-335
	public void insertEntityShouldInsertEntity() {

		User person = new User("36", "Homer", "Simpson");

		StepVerifier.create(repository.insert(person)).expectNext(person).verifyComplete();

		StepVerifier.create(repository.findAll()).expectNextCount(1L).verifyComplete();
	}

	@Test // DATACASS-335
	public void insertShouldDeferredWrite() {

		User person = new User("36", "Homer", "Simpson");

		repository.insert(person);

		StepVerifier.create(repository.findAll()).expectNextCount(0L).verifyComplete();
	}

	@Test // DATACASS-335
	public void insertIterableOfEntitiesShouldInsertEntity() {

		StepVerifier.create(repository.insert(Arrays.asList(dave, oliver, boyd))).expectNextCount(3L).verifyComplete();

		StepVerifier.create(repository.findAll()).expectNextCount(3L).verifyComplete();
	}

	@Test // DATACASS-335
	public void insertPublisherOfEntitiesShouldInsertEntity() {

		StepVerifier.create(repository.insert(Flux.just(dave, oliver, boyd))).expectNextCount(3L).verifyComplete();

		StepVerifier.create(repository.findAll()).expectNextCount(3L).verifyComplete();
	}

	@Test // DATACASS-335
	public void saveEntityShouldUpdateExistingEntity() {

		dave.setFirstname("Hello, Dave");
		dave.setLastname("Bowman");

		StepVerifier.create(repository.save(dave)).expectNextCount(1).verifyComplete();

		StepVerifier.create(repository.findById(dave.getId())).consumeNextWith(actual -> {

			assertThat(actual.getFirstname()).isEqualTo(dave.getFirstname());
			assertThat(actual.getLastname()).isEqualTo(dave.getLastname());
		}).verifyComplete();
	}

	@Test // DATACASS-335
	public void saveEntityShouldInsertNewEntity() {

		User person = new User("36", "Homer", "Simpson");

		StepVerifier.create(repository.save(person)).expectNextCount(1).verifyComplete();

		StepVerifier.create(repository.findById(person.getId())).expectNext(person).verifyComplete();
	}

	@Test // DATACASS-335
	public void saveIterableOfNewEntitiesShouldInsertEntity() {

		StepVerifier.create(repository.saveAll(Arrays.asList(dave, oliver, boyd))).expectNextCount(3).verifyComplete();

		StepVerifier.create(repository.findAll()).expectNextCount(3L).verifyComplete();
	}

	@Test // DATACASS-335
	public void saveIterableOfMixedEntitiesShouldInsertEntity() {

		User person = new User("36", "Homer", "Simpson");

		dave.setFirstname("Hello, Dave");
		dave.setLastname("Bowman");

		StepVerifier.create(repository.saveAll(Arrays.asList(person, dave))).expectNextCount(2).verifyComplete();

		StepVerifier.create(repository.findById(dave.getId())).expectNext(dave).verifyComplete();

		StepVerifier.create(repository.findById(person.getId())).expectNext(person).verifyComplete();
	}

	@Test // DATACASS-335
	public void savePublisherOfEntitiesShouldInsertEntity() {

		StepVerifier.create(repository.saveAll(Flux.just(dave, oliver, boyd))).expectNextCount(3).verifyComplete();

		StepVerifier.create(repository.findAll()).expectNextCount(3L).verifyComplete();
	}

	@Test // DATACASS-335
	public void deleteAllShouldRemoveEntities() {

		insertTestData();

		StepVerifier.create(repository.deleteAll()).verifyComplete();

		StepVerifier.create(repository.findAll()).verifyComplete();
	}

	@Test // DATACASS-335
	public void deleteByIdShouldRemoveEntity() {

		insertTestData();

		StepVerifier.create(repository.deleteById(dave.getId())).verifyComplete();

		StepVerifier.create(repository.findById(dave.getId())).expectNextCount(0).verifyComplete();
	}

	@Test // DATACASS-462
	public void deleteByIdUsingMonoShouldRemoveEntity() {

		insertTestData();

		StepVerifier.create(repository.deleteById(Mono.just(dave.getId()))).verifyComplete();

		StepVerifier.create(repository.existsById(dave.getId())).expectNext(false).verifyComplete();
	}

	@Test // DATACASS-462
	public void deleteByIdUsingFluxShouldRemoveFirstEntity() {

		insertTestData();

		StepVerifier.create(repository.deleteById(Flux.just(dave.getId(), oliver.getId()))).verifyComplete();

		StepVerifier.create(repository.existsById(dave.getId())).expectNext(false).verifyComplete();
		StepVerifier.create(repository.existsById(oliver.getId())).expectNext(true).verifyComplete();
	}

	@Test // DATACASS-335
	public void deleteShouldRemoveEntity() {

		insertTestData();

		StepVerifier.create(repository.delete(dave)).verifyComplete();

		StepVerifier.create(repository.findById(dave.getId())).expectNextCount(0).verifyComplete();
	}

	@Test // DATACASS-335
	public void deleteIterableOfEntitiesShouldRemoveEntities() {

		insertTestData();

		StepVerifier.create(repository.deleteAll(Arrays.asList(dave, boyd))).verifyComplete();

		StepVerifier.create(repository.findById(boyd.getId())).expectNextCount(0).verifyComplete();
	}

	@Test // DATACASS-335
	public void deletePublisherOfEntitiesShouldRemoveEntities() {

		insertTestData();

		StepVerifier.create(repository.deleteAll(Flux.just(dave, boyd))).verifyComplete();

		StepVerifier.create(repository.findById(boyd.getId())).expectNextCount(0).verifyComplete();
	}

	interface UserRepostitory extends ReactiveCassandraRepository<User, String> { }

}
