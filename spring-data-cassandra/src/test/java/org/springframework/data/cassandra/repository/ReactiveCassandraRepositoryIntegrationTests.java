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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.reactivestreams.Publisher;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.data.cassandra.core.ReactiveCassandraOperations;
import org.springframework.data.cassandra.domain.Group;
import org.springframework.data.cassandra.domain.GroupKey;
import org.springframework.data.cassandra.domain.User;
import org.springframework.data.cassandra.repository.support.IntegrationTestConfig;
import org.springframework.data.cassandra.repository.support.ReactiveCassandraRepositoryFactory;
import org.springframework.data.cassandra.repository.support.SimpleReactiveCassandraRepository;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTest;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;

/**
 * Test for {@link ReactiveCassandraRepository} query methods.
 *
 * @author Mark Paluch
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class ReactiveCassandraRepositoryIntegrationTests extends AbstractKeyspaceCreatingIntegrationTest
		implements BeanClassLoaderAware, BeanFactoryAware {

	@Configuration
	public static class Config extends IntegrationTestConfig {

		@Override
		protected Set<Class<?>> getInitialEntitySet() {
			return new HashSet<>(Arrays.asList(Group.class, User.class));
		}
	}

	@Autowired ReactiveCassandraOperations operations;
	@Autowired Session session;

	ReactiveCassandraRepositoryFactory factory;
	ClassLoader classLoader;
	BeanFactory beanFactory;
	UserRepository repository;
	GroupRepository groupRepostitory;

	User dave, oliver, carter, boyd;

	@Override
	public void setBeanClassLoader(ClassLoader classLoader) {
		this.classLoader = classLoader == null ? org.springframework.util.ClassUtils.getDefaultClassLoader() : classLoader;
	}

	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.beanFactory = beanFactory;
	}

	@Before
	public void setUp() throws Exception {

		KeyspaceMetadata keyspace = session.getCluster().getMetadata().getKeyspace(session.getLoggedKeyspace());
		TableMetadata users = keyspace.getTable("users");

		if (users.getIndex("IX_lastname") == null) {
			session.execute("CREATE INDEX IX_lastname ON users (lastname);");
			Thread.sleep(500);
		}

		factory = new ReactiveCassandraRepositoryFactory(operations);
		factory.setRepositoryBaseClass(SimpleReactiveCassandraRepository.class);
		factory.setBeanClassLoader(classLoader);
		factory.setBeanFactory(beanFactory);
		factory.setEvaluationContextProvider(QueryMethodEvaluationContextProvider.DEFAULT);

		repository = factory.getRepository(UserRepository.class);
		groupRepostitory = factory.getRepository(GroupRepository.class);

		StepVerifier.create(repository.deleteAll().concatWith(groupRepostitory.deleteAll())).verifyComplete();

		dave = new User("42", "Dave", "Matthews");
		oliver = new User("4", "Oliver August", "Matthews");
		carter = new User("49", "Carter", "Beauford");
		boyd = new User("45", "Boyd", "Tinsley");

		StepVerifier.create(repository.saveAll(Arrays.asList(oliver, dave, carter, boyd))).expectNextCount(4)
				.verifyComplete();
	}

	@Test // DATACASS-335
	public void shouldFindByLastName() {
		StepVerifier.create(repository.findByLastname(dave.getLastname())).expectNextCount(2).verifyComplete();
	}

	@Test // DATACASS-525
	public void findOneWithManyResultsShouldFail() {
		StepVerifier.create(repository.findOneByLastname(dave.getLastname()))
				.expectError(IncorrectResultSizeDataAccessException.class).verify();
	}

	@Test // DATACASS-525
	public void findOneWithNoResultsShouldNotEmitItem() {
		StepVerifier.create(repository.findByLastname("foo")).verifyComplete();
	}

	@Test // DATACASS-525
	public void findFirstWithManyResultsShouldEmitFirstItem() {
		StepVerifier.create(repository.findFirstByLastname(dave.getLastname())).expectNextCount(1).verifyComplete();
	}

	@Test // DATACASS-335
	public void shouldFindByIdByLastName() {
		StepVerifier.create(repository.findOneByLastname(carter.getLastname())).expectNext(carter).verifyComplete();
	}

	@Test // DATACASS-335
	public void shouldFindByIdByPublisherOfLastName() {
		StepVerifier.create(repository.findByLastname(Mono.just(carter.getLastname()))).expectNext(carter).verifyComplete();
	}

	@Test // DATACASS-335
	public void shouldFindUsingPublishersInStringQuery() {
		StepVerifier.create(repository.findStringQuery(Mono.just(dave.getLastname()))).expectNextCount(2).verifyComplete();
	}

	@Test // DATACASS-335
	public void shouldFindByLastNameAndSort() {

		GroupKey key1 = new GroupKey("Simpsons", "hash", "Bart");
		GroupKey key2 = new GroupKey("Simpsons", "hash", "Homer");

		StepVerifier.create(groupRepostitory.saveAll(Flux.just(new Group(key1), new Group(key2)))).expectNextCount(2)
				.verifyComplete();

		StepVerifier
				.create(groupRepostitory.findByIdGroupnameAndIdHashPrefix("Simpsons", "hash",
						new Sort(Direction.ASC, "id.username"))) //
				.expectNext(new Group(key1), new Group(key2)) //
				.verifyComplete();

		StepVerifier
				.create(groupRepostitory.findByIdGroupnameAndIdHashPrefix("Simpsons", "hash",
						new Sort(Direction.DESC, "id.username"))) //
				.expectNext(new Group(key2), new Group(key1)) //
				.verifyComplete();
	}

	@Test // DATACASS-512
	public void shouldCountRecords() {

		StepVerifier.create(repository.countByLastname("Matthews")).expectNext(2L).verifyComplete();
		StepVerifier.create(repository.countByLastname("None")).expectNext(0L).verifyComplete();

		StepVerifier.create(repository.countQueryByLastname("Matthews")).expectNext(2L).verifyComplete();
		StepVerifier.create(repository.countQueryByLastname("None")).expectNext(0L).verifyComplete();
	}

	@Test // DATACASS-512
	public void shouldApplyExistsProjection() {

		StepVerifier.create(repository.existsByLastname("Matthews")).expectNext(true).verifyComplete();
		StepVerifier.create(repository.existsByLastname("None")).expectNext(false).verifyComplete();

		StepVerifier.create(repository.existsQueryByLastname("Matthews")).expectNext(true).verifyComplete();
		StepVerifier.create(repository.existsQueryByLastname("None")).expectNext(false).verifyComplete();
	}

	interface UserRepository extends ReactiveCassandraRepository<User, String> {

		Flux<User> findByLastname(String lastname);

		Mono<User> findFirstByLastname(String lastname);

		Mono<User> findOneByLastname(String lastname);

		Mono<User> findByLastname(Publisher<String> lastname);

		Mono<Long> countByLastname(String lastname);

		Mono<Boolean> existsByLastname(String lastname);

		@Query("SELECT * FROM users WHERE lastname = ?0")
		Flux<User> findStringQuery(Mono<String> lastname);

		@CountQuery("SELECT COUNT(*) from users WHERE lastname = ?0")
		Mono<Long> countQueryByLastname(String lastname);

		@ExistsQuery("SELECT * from users WHERE lastname = ?0")
		Mono<Boolean> existsQueryByLastname(String lastname);
	}

	interface GroupRepository extends ReactiveCassandraRepository<Group, GroupKey> {

		Flux<Group> findByIdGroupnameAndIdHashPrefix(String groupname, String hashPrefix, Sort sort);

	}
}
