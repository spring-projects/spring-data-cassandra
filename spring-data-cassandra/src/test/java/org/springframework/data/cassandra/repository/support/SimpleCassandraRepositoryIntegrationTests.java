/*
 * Copyright 2017-2021 the original author or authors.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.mapping.event.AbstractCassandraEventListener;
import org.springframework.data.cassandra.core.mapping.event.AfterSaveEvent;
import org.springframework.data.cassandra.core.mapping.event.BeforeSaveEvent;
import org.springframework.data.cassandra.core.mapping.event.CassandraMappingEvent;
import org.springframework.data.cassandra.core.query.CassandraPageRequest;
import org.springframework.data.cassandra.domain.User;
import org.springframework.data.cassandra.domain.UserToken;
import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.data.cassandra.support.CassandraVersion;
import org.springframework.data.cassandra.test.util.IntegrationTestsSupport;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;
import org.springframework.data.repository.query.ExtensionAwareQueryMethodEvaluationContextProvider;
import org.springframework.data.util.Version;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.util.ClassUtils;

import com.datastax.oss.driver.api.core.CqlSession;

/**
 * Integration tests for {@link SimpleCassandraRepository}.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 */
@SpringJUnitConfig
public class SimpleCassandraRepositoryIntegrationTests extends IntegrationTestsSupport
		implements BeanClassLoaderAware, BeanFactoryAware {

	private static final Version CASSANDRA_3 = Version.parse("3.0");

	@Configuration
	public static class Config extends IntegrationTestConfig {

		@Override
		public String[] getEntityBasePackages() {
			return new String[] { User.class.getPackage().getName() };
		}

		@Bean
		CaptureEventListener eventListener() {
			return new CaptureEventListener();
		}
	}

	@Autowired private CqlSession session;
	@Autowired private CassandraOperations operations;
	@Autowired private CaptureEventListener eventListener;

	private Version cassandraVersion;
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

	@BeforeEach
	void setUp() {

		factory = new CassandraRepositoryFactory(operations);
		factory.setRepositoryBaseClass(SimpleCassandraRepository.class);
		factory.setBeanClassLoader(classLoader);
		factory.setBeanFactory(beanFactory);
		factory.setEvaluationContextProvider(ExtensionAwareQueryMethodEvaluationContextProvider.DEFAULT);

		repository = factory.getRepository(UserRepostitory.class);

		cassandraVersion = CassandraVersion.get(session);

		repository.deleteAll();

		dave = new User("42", "Dave", "Matthews");
		oliver = new User("4", "Oliver August", "Matthews");
		carter = new User("49", "Carter", "Beauford");
		boyd = new User("45", "Boyd", "Tinsley");

		repository.saveAll(Arrays.asList(oliver, dave, carter, boyd));

		eventListener.clear();
	}

	@Test // DATACASS-396
	void existsByIdShouldReturnTrueForExistingObject() {

		Boolean exists = repository.existsById(dave.getId());

		assertThat(exists).isTrue();
	}

	@Test // DATACASS-396
	void existsByIdShouldReturnFalseForAbsentObject() {

		boolean exists = repository.existsById("unknown");

		assertThat(exists).isFalse();
	}

	@Test // DATACASS-396
	void existsByMonoOfIdShouldReturnTrueForExistingObject() {

		boolean exists = repository.existsById(dave.getId());

		assertThat(exists).isTrue();
	}

	@Test // DATACASS-396
	void findByIdShouldReturnObject() {

		Optional<User> User = repository.findById(dave.getId());

		assertThat(User).contains(dave);
	}

	@Test // DATACASS-396
	void findByIdShouldCompleteWithoutValueForAbsentObject() {

		Optional<User> User = repository.findById("unknown");

		assertThat(User).isEmpty();
	}

	@Test // DATACASS-396, DATACASS-416
	void findAllShouldReturnAllResults() {

		List<User> Users = repository.findAll();

		assertThat(Users).hasSize(4);
	}

	@Test // DATACASS-396, DATACASS-416
	void findAllByIterableOfIdShouldReturnResults() {

		List<User> Users = repository.findAllById(Arrays.asList(dave.getId(), boyd.getId()));

		assertThat(Users).hasSize(2);
	}

	@Test // DATACASS-56
	void findAllWithPaging() {

		Slice<User> slice = repository.findAll(CassandraPageRequest.first(2));

		assertThat(slice).hasSize(2);

		assertThat(repository.findAll(slice.nextPageable())).hasSize(2);
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

		repository.saveAll(users);

		List<UserToken> result = new ArrayList<>();
		Slice<UserToken> slice = repository.findAllByUserId(id, CassandraPageRequest.first(10, Sort.by("token")));

		while (!slice.isEmpty() || slice.hasNext()) {
			result.addAll(slice.getContent());

			slice = repository.findAllByUserId(id, slice.nextPageable());
		}

		assertThat(result).hasSize(100);
	}

	@Test // DATACASS-396
	void countShouldReturnNumberOfRecords() {

		long count = repository.count();

		assertThat(count).isEqualTo(4);
	}

	@Test // DATACASS-415
	void insertEntityShouldInsertEntity() {

		repository.deleteAll();

		User User = new User("36", "Homer", "Simpson");

		repository.insert(User);

		assertThat(repository.count()).isEqualTo(1);
	}

	@Test // DATACASS-415
	void insertIterableOfEntitiesShouldInsertEntity() {

		repository.deleteAll();

		repository.insert(Arrays.asList(dave, oliver, boyd));

		assertThat(repository.count()).isEqualTo(3);
	}

	@Test // DATACASS-396, DATACASS-573
	void saveEntityShouldUpdateExistingEntity() {

		dave.setFirstname("Hello, Dave");
		dave.setLastname("Bowman");

		User saved = repository.save(dave);

		assertThat(saved).isSameAs(saved);

		Optional<User> loaded = repository.findById(dave.getId());

		assertThat(loaded).isPresent();

		loaded.ifPresent(actual -> {
			assertThat(actual.getFirstname()).isEqualTo(dave.getFirstname());
			assertThat(actual.getLastname()).isEqualTo(dave.getLastname());
		});
	}

	@Test // DATACASS-560
	void saveShouldEmitEvents() {

		dave.setFirstname("Hello, Dave");
		dave.setLastname("Bowman");

		repository.save(dave);

		assertThat(eventListener.getBeforeSave()).hasSize(1);
		assertThat(eventListener.getAfterSave()).hasSize(1);
	}

	@Test // DATACASS-396
	void saveEntityShouldInsertNewEntity() {

		User User = new User("36", "Homer", "Simpson");

		User saved = repository.save(User);

		assertThat(saved).isEqualTo(User);

		Optional<User> loaded = repository.findById(User.getId());

		assertThat(loaded).contains(User);
	}

	@Test // DATACASS-396, DATACASS-416, DATACASS-573
	void saveIterableOfNewEntitiesShouldInsertEntity() {

		repository.deleteAll();

		List<User> saved = repository.saveAll(Arrays.asList(dave, oliver, boyd));

		assertThat(saved).hasSize(3).contains(dave, oliver, boyd);

		assertThat(repository.count()).isEqualTo(3);
	}

	@Test // DATACASS-396, DATACASS-416
	void saveIterableOfMixedEntitiesShouldInsertEntity() {

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
	void deleteAllShouldRemoveEntities() {

		repository.deleteAll();

		List<User> result = repository.findAll();

		assertThat(result).isEmpty();
	}

	@Test // DATACASS-396
	void deleteByIdShouldRemoveEntity() {

		repository.deleteById(dave.getId());

		Optional<User> loaded = repository.findById(dave.getId());

		assertThat(loaded).isEmpty();
	}

	@Test // DATACASS-825
	void deleteAllByIdShouldRemoveEntity() {

		repository.deleteAllById(Collections.singletonList(dave.getId()));

		Optional<User> loaded = repository.findById(dave.getId());

		assertThat(loaded).isEmpty();
	}

	@Test // DATACASS-396
	void deleteShouldRemoveEntity() {

		repository.delete(dave);

		Optional<User> loaded = repository.findById(dave.getId());

		assertThat(loaded).isEmpty();
	}

	@Test // DATACASS-396
	void deleteIterableOfEntitiesShouldRemoveEntities() {

		repository.deleteAll(Arrays.asList(dave, boyd));

		Optional<User> loaded = repository.findById(boyd.getId());

		assertThat(loaded).isEmpty();
	}

	interface UserRepostitory extends CassandraRepository<User, String> {}

	interface UserTokenRepostitory extends CassandraRepository<UserToken, UUID> {
		Slice<UserToken> findAllByUserId(UUID id, Pageable pageRequest);
	}

	static class CaptureEventListener extends AbstractCassandraEventListener<User> {

		private final List<CassandraMappingEvent<?>> events = new CopyOnWriteArrayList<>();

		@Override
		public void onBeforeSave(BeforeSaveEvent<User> event) {
			events.add(event);
		}

		@Override
		public void onAfterSave(AfterSaveEvent<User> event) {
			events.add(event);
		}

		private void clear() {
			events.clear();
		}

		private List<BeforeSaveEvent<User>> getBeforeSave() {
			return filter(BeforeSaveEvent.class);
		}

		private List<AfterSaveEvent<User>> getAfterSave() {
			return filter(AfterSaveEvent.class);
		}

		@SuppressWarnings("unchecked")
		private <T> List<T> filter(Class<? super T> targetType) {
			return (List) events.stream().filter(targetType::isInstance).map(targetType::cast).collect(Collectors.toList());
		}
	}
}
