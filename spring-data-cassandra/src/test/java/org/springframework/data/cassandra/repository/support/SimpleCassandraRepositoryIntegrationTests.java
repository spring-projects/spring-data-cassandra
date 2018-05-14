/*
 * Copyright 2017-2018 the original author or authors.
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
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.query.CassandraPageRequest;
import org.springframework.data.cassandra.domain.User;
import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTest;
import org.springframework.data.domain.Slice;
import org.springframework.data.repository.query.ExtensionAwareQueryMethodEvaluationContextProvider;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.ClassUtils;

/**
 * Integration tests for {@link SimpleCassandraRepository}.
 *
 * @author Mark Paluch
 */
@RunWith(SpringRunner.class)
@ContextConfiguration
public class SimpleCassandraRepositoryIntegrationTests extends AbstractKeyspaceCreatingIntegrationTest
		implements BeanClassLoaderAware, BeanFactoryAware {

	@Configuration
	public static class Config extends IntegrationTestConfig {

		@Override
		public String[] getEntityBasePackages() {
			return new String[] { User.class.getPackage().getName() };
		}
	}

	@Autowired private CassandraOperations operations;

	private BeanFactory beanFactory;
	private CassandraRepositoryFactory factory;
	private ClassLoader classLoader;
	private UserRepostitory repository;

	private User dave, oliver, carter, boyd;

	@Override
	public void setBeanClassLoader(ClassLoader classLoader) {
		this.classLoader = classLoader != null ? classLoader : ClassUtils.getDefaultClassLoader();
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
		factory.setEvaluationContextProvider(ExtensionAwareQueryMethodEvaluationContextProvider.DEFAULT);

		repository = factory.getRepository(UserRepostitory.class);

		repository.deleteAll();

		dave = new User("42", "Dave", "Matthews");
		oliver = new User("4", "Oliver August", "Matthews");
		carter = new User("49", "Carter", "Beauford");
		boyd = new User("45", "Boyd", "Tinsley");

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

		Optional<User> User = repository.findById(dave.getId());

		assertThat(User).contains(dave);
	}

	@Test // DATACASS-396
	public void findByIdShouldCompleteWithoutValueForAbsentObject() {

		Optional<User> User = repository.findById("unknown");

		assertThat(User).isEmpty();
	}

	@Test // DATACASS-396, DATACASS-416
	public void findAllShouldReturnAllResults() {

		List<User> Users = repository.findAll();

		assertThat(Users).hasSize(4);
	}

	@Test // DATACASS-396, DATACASS-416
	public void findAllByIterableOfIdShouldReturnResults() {

		List<User> Users = repository.findAllById(Arrays.asList(dave.getId(), boyd.getId()));

		assertThat(Users).hasSize(2);
	}

	@Test // DATACASS-56
	public void findAllWithPaging() {

		Slice<User> slice = repository.findAll(CassandraPageRequest.first(2));

		assertThat(slice).hasSize(2);

		assertThat(repository.findAll(slice.nextPageable())).hasSize(2);
	}

	@Test // DATACASS-396
	public void countShouldReturnNumberOfRecords() {

		long count = repository.count();

		assertThat(count).isEqualTo(4);
	}

	@Test // DATACASS-415
	public void insertEntityShouldInsertEntity() {

		repository.deleteAll();

		User User = new User("36", "Homer", "Simpson");

		repository.insert(User);

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

		User saved = repository.save(dave);

		assertThat(saved).isEqualTo(saved);

		Optional<User> loaded = repository.findById(dave.getId());

		assertThat(loaded).isPresent();

		loaded.ifPresent(actual -> {
			assertThat(actual.getFirstname()).isEqualTo(dave.getFirstname());
			assertThat(actual.getLastname()).isEqualTo(dave.getLastname());
		});
	}

	@Test // DATACASS-396
	public void saveEntityShouldInsertNewEntity() {

		User User = new User("36", "Homer", "Simpson");

		User saved = repository.save(User);

		assertThat(saved).isEqualTo(User);

		Optional<User> loaded = repository.findById(User.getId());

		assertThat(loaded).contains(User);
	}

	@Test // DATACASS-396, DATACASS-416
	public void saveIterableOfNewEntitiesShouldInsertEntity() {

		repository.deleteAll();

		List<User> saved = repository.saveAll(Arrays.asList(dave, oliver, boyd));

		assertThat(saved).hasSize(3);

		assertThat(repository.count()).isEqualTo(3);
	}

	@Test // DATACASS-396, DATACASS-416
	public void saveIterableOfMixedEntitiesShouldInsertEntity() {

		User User = new User("36", "Homer", "Simpson");

		dave.setFirstname("Hello, Dave");
		dave.setLastname("Bowman");

		List<User> saved = repository.saveAll(Arrays.asList(User, dave));

		assertThat(saved).hasSize(2);

		Optional<User> persistentDave = repository.findById(dave.getId());
		assertThat(persistentDave).contains(dave);

		Optional<User> persistentHomer = repository.findById(User.getId());
		assertThat(persistentHomer).contains(User);
	}

	@Test // DATACASS-396, DATACASS-416
	public void deleteAllShouldRemoveEntities() {

		repository.deleteAll();

		List<User> result = repository.findAll();

		assertThat(result).isEmpty();
	}

	@Test // DATACASS-396
	public void deleteByIdShouldRemoveEntity() {

		repository.deleteById(dave.getId());

		Optional<User> loaded = repository.findById(dave.getId());

		assertThat(loaded).isEmpty();
	}

	@Test // DATACASS-396
	public void deleteShouldRemoveEntity() {

		repository.delete(dave);

		Optional<User> loaded = repository.findById(dave.getId());

		assertThat(loaded).isEmpty();
	}

	@Test // DATACASS-396
	public void deleteIterableOfEntitiesShouldRemoveEntities() {

		repository.deleteAll(Arrays.asList(dave, boyd));

		Optional<User> loaded = repository.findById(boyd.getId());

		assertThat(loaded).isEmpty();
	}

	interface UserRepostitory extends CassandraRepository<User, String> { }

}
