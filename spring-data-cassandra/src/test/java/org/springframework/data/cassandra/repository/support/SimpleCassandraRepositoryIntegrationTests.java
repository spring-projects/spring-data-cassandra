/*
 * Copyright 2017 the original author or authors.
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
import java.util.Optional;

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
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.domain.Person;
import org.springframework.data.cassandra.repository.TypedIdCassandraRepository;
import org.springframework.data.cassandra.test.integration.support.IntegrationTestConfig;
import org.springframework.data.repository.query.DefaultEvaluationContextProvider;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Integration tests for {@link SimpleCassandraRepository}.
 *
 * @author Mark Paluch
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class SimpleCassandraRepositoryIntegrationTests extends AbstractKeyspaceCreatingIntegrationTest
		implements BeanClassLoaderAware, BeanFactoryAware {

	@Configuration
	public static class Config extends IntegrationTestConfig {

		@Override
		public String[] getEntityBasePackages() {
			return new String[] { Person.class.getPackage().getName() };
		}
	}

	@Autowired private CassandraOperations operations;

	CassandraRepositoryFactory factory;
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

		factory = new CassandraRepositoryFactory(operations);
		factory.setRepositoryBaseClass(SimpleCassandraRepository.class);
		factory.setBeanClassLoader(classLoader);
		factory.setBeanFactory(beanFactory);
		factory.setEvaluationContextProvider(DefaultEvaluationContextProvider.INSTANCE);

		repository = factory.getRepository(PersonRepostitory.class);

		repository.deleteAll();

		dave = new Person("42", "Dave", "Matthews");
		oliver = new Person("4", "Oliver August", "Matthews");
		carter = new Person("49", "Carter", "Beauford");
		boyd = new Person("45", "Boyd", "Tinsley");

		repository.saveAll(Arrays.asList(oliver, dave, carter, boyd));
	}

	@Test // DATACASS-396
	public void existsByIdShouldReturnTrueForExistingObject() {

		Boolean exists = repository.existsById(dave.getId());

		assertThat(exists).isTrue();
	}

	@Test // DATACASS-396
	public void existsByIdShouldReturnFalseForAbsentObject() {

		boolean exists = repository.existsById("unknown");

		assertThat(exists).isFalse();
	}

	@Test // DATACASS-396
	public void existsByMonoOfIdShouldReturnTrueForExistingObject() {

		boolean exists = repository.existsById(dave.getId());

		assertThat(exists).isTrue();
	}

	@Test // DATACASS-396
	public void findByIdShouldReturnObject() {

		Optional<Person> person = repository.findById(dave.getId());

		assertThat(person).contains(dave);
	}

	@Test // DATACASS-396
	public void findByIdShouldCompleteWithoutValueForAbsentObject() {

		Optional<Person> person = repository.findById("unknown");

		assertThat(person).isEmpty();
	}

	@Test // DATACASS-396, DATACASS-416
	public void findAllShouldReturnAllResults() {

		List<Person> persons = repository.findAll();

		assertThat(persons).hasSize(4);
	}

	@Test // DATACASS-396, DATACASS-416
	public void findAllByIterableOfIdShouldReturnResults() {

		List<Person> persons = repository.findAllById(Arrays.asList(dave.getId(), boyd.getId()));

		assertThat(persons).hasSize(2);
	}

	@Test // DATACASS-396
	public void countShouldReturnNumberOfRecords() {

		long count = repository.count();

		assertThat(count).isEqualTo(4);
	}

	@Test // DATACASS-415
	public void insertEntityShouldInsertEntity() {

		repository.deleteAll();

		Person person = new Person("36", "Homer", "Simpson");

		repository.insert(person);

		assertThat(repository.count()).isEqualTo(1);
	}

	@Test // DATACASS-415
	public void insertIterableOfEntitiesShouldInsertEntity() {

		repository.deleteAll();

		repository.insert(Arrays.asList(dave, oliver, boyd));

		assertThat(repository.count()).isEqualTo(3);
	}

	@Test // DATACASS-396
	public void saveEntityShouldUpdateExistingEntity() {

		dave.setFirstname("Hello, Dave");
		dave.setLastname("Bowman");

		Person saved = repository.save(dave);

		assertThat(saved).isEqualTo(saved);

		Optional<Person> loaded = repository.findById(dave.getId());

		assertThat(loaded).isPresent();

		loaded.ifPresent(actual -> {
			assertThat(actual.getFirstname()).isEqualTo(dave.getFirstname());
			assertThat(actual.getLastname()).isEqualTo(dave.getLastname());
		});
	}

	@Test // DATACASS-396
	public void saveEntityShouldInsertNewEntity() {

		Person person = new Person("36", "Homer", "Simpson");

		Person saved = repository.save(person);

		assertThat(saved).isEqualTo(person);

		Optional<Person> loaded = repository.findById(person.getId());

		assertThat(loaded).contains(person);
	}

	@Test // DATACASS-396, DATACASS-416
	public void saveIterableOfNewEntitiesShouldInsertEntity() {

		repository.deleteAll();

		List<Person> saved = repository.saveAll(Arrays.asList(dave, oliver, boyd));

		assertThat(saved).hasSize(3);

		assertThat(repository.count()).isEqualTo(3);
	}

	@Test // DATACASS-396, DATACASS-416
	public void saveIterableOfMixedEntitiesShouldInsertEntity() {

		Person person = new Person("36", "Homer", "Simpson");

		dave.setFirstname("Hello, Dave");
		dave.setLastname("Bowman");

		List<Person> saved = repository.saveAll(Arrays.asList(person, dave));

		assertThat(saved).hasSize(2);

		Optional<Person> persistentDave = repository.findById(dave.getId());
		assertThat(persistentDave).contains(dave);

		Optional<Person> persistentHomer = repository.findById(person.getId());
		assertThat(persistentHomer).contains(person);
	}

	@Test // DATACASS-396, DATACASS-416
	public void deleteAllShouldRemoveEntities() {

		repository.deleteAll();

		List<Person> result = repository.findAll();

		assertThat(result).isEmpty();
	}

	@Test // DATACASS-396
	public void deleteByIdShouldRemoveEntity() {

		repository.deleteById(dave.getId());

		Optional<Person> loaded = repository.findById(dave.getId());

		assertThat(loaded).isEmpty();
	}

	@Test // DATACASS-396
	public void deleteShouldRemoveEntity() {

		repository.delete(dave);

		Optional<Person> loaded = repository.findById(dave.getId());

		assertThat(loaded).isEmpty();
	}

	@Test // DATACASS-396
	public void deleteIterableOfEntitiesShouldRemoveEntities() {

		repository.deleteAll(Arrays.asList(dave, boyd));

		Optional<Person> loaded = repository.findById(boyd.getId());

		assertThat(loaded).isEmpty();
	}

	interface PersonRepostitory extends TypedIdCassandraRepository<Person, String> {}
}
