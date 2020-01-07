/*
 * Copyright 2016-2020 the original author or authors.
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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

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
import org.springframework.data.cassandra.core.query.CassandraPageRequest;
import org.springframework.data.cassandra.domain.Group;
import org.springframework.data.cassandra.domain.GroupKey;
import org.springframework.data.cassandra.domain.User;
import org.springframework.data.cassandra.repository.support.IntegrationTestConfig;
import org.springframework.data.cassandra.repository.support.ReactiveCassandraRepositoryFactory;
import org.springframework.data.cassandra.repository.support.SimpleReactiveCassandraRepository;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.data.util.Streamable;
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

		repository.deleteAll().concatWith(groupRepostitory.deleteAll()).as(StepVerifier::create).verifyComplete();

		dave = new User("42", "Dave", "Matthews");
		oliver = new User("4", "Oliver August", "Matthews");
		carter = new User("49", "Carter", "Beauford");
		boyd = new User("45", "Boyd", "Tinsley");

		repository.saveAll(Arrays.asList(oliver, dave, carter, boyd)).as(StepVerifier::create).expectNextCount(4)
				.verifyComplete();
	}

	@Test // DATACASS-335
	public void shouldFindByLastName() {
		repository.findByLastname(dave.getLastname()).as(StepVerifier::create).expectNextCount(2).verifyComplete();
	}

	@Test // DATACASS-529
	public void shouldFindSliceByLastName() {
		repository.findByLastname(carter.getLastname(), CassandraPageRequest.first(1)).as(StepVerifier::create)
				.expectNextMatches(users -> users.getSize() == 1 && users.hasNext()).verifyComplete();
	}

	@Test // DATACASS-529
	public void shouldFindEmpptySliceByLastName() {
		repository.findByLastname("foo", CassandraPageRequest.first(1)).as(StepVerifier::create)
				.expectNextMatches(Streamable::isEmpty).verifyComplete();
	}

	@Test // DATACASS-525
	public void findOneWithManyResultsShouldFail() {
		repository.findOneByLastname(dave.getLastname()).as(StepVerifier::create)
				.expectError(IncorrectResultSizeDataAccessException.class).verify();
	}

	@Test // DATACASS-525
	public void findOneWithNoResultsShouldNotEmitItem() {
		repository.findByLastname("foo").as(StepVerifier::create).verifyComplete();
	}

	@Test // DATACASS-525
	public void findFirstWithManyResultsShouldEmitFirstItem() {
		repository.findFirstByLastname(dave.getLastname()).as(StepVerifier::create).expectNextCount(1).verifyComplete();
	}

	@Test // DATACASS-335
	public void shouldFindByIdByLastName() {
		repository.findOneByLastname(carter.getLastname()).as(StepVerifier::create).expectNext(carter).verifyComplete();
	}

	@Test // DATACASS-335
	public void shouldFindByIdByPublisherOfLastName() {
		repository.findByLastname(Mono.just(carter.getLastname())).as(StepVerifier::create).expectNext(carter)
				.verifyComplete();
	}

	@Test // DATACASS-335
	public void shouldFindUsingPublishersInStringQuery() {
		repository.findStringQuery(Mono.just(dave.getLastname())).as(StepVerifier::create).expectNextCount(2)
				.verifyComplete();
	}

	@Test // DATACASS-335
	public void shouldFindByLastNameAndSort() {

		GroupKey key1 = new GroupKey("Simpsons", "hash", "Bart");
		GroupKey key2 = new GroupKey("Simpsons", "hash", "Homer");

		groupRepostitory.saveAll(Flux.just(new Group(key1), new Group(key2))).as(StepVerifier::create).expectNextCount(2)
				.verifyComplete();

		groupRepostitory.findByIdGroupnameAndIdHashPrefix("Simpsons", "hash", Sort.by("id.username").ascending())
				.as(StepVerifier::create) //
				.expectNext(new Group(key1), new Group(key2)) //
				.verifyComplete();

		groupRepostitory.findByIdGroupnameAndIdHashPrefix("Simpsons", "hash", Sort.by("id.username").descending())
				.as(StepVerifier::create).expectNext(new Group(key2), new Group(key1)) //
				.verifyComplete();
	}

	@Test // DATACASS-512
	public void shouldCountRecords() {

		repository.countByLastname("Matthews").as(StepVerifier::create).expectNext(2L).verifyComplete();
		repository.countByLastname("None").as(StepVerifier::create).expectNext(0L).verifyComplete();

		repository.countQueryByLastname("Matthews").as(StepVerifier::create).expectNext(2L).verifyComplete();
		repository.countQueryByLastname("None").as(StepVerifier::create).expectNext(0L).verifyComplete();
	}

	@Test // DATACASS-611
	public void shouldDeleteRecords() {

		repository.deleteVoidById(dave.getId()).as(StepVerifier::create).verifyComplete();

		repository.countByLastname("Matthews").as(StepVerifier::create).expectNext(1L).verifyComplete();
	}

	@Test // DATACASS-611
	public void shouldDeleteRecordsReturingWasApplied() {

		repository.deleteAllById(dave.getId()).as(StepVerifier::create).expectNext(true).verifyComplete();

		repository.countByLastname("Matthews").as(StepVerifier::create).expectNext(1L).verifyComplete();
	}

	@Test // DATACASS-512
	public void shouldApplyExistsProjection() {

		repository.existsByLastname("Matthews").as(StepVerifier::create).expectNext(true).verifyComplete();
		repository.existsByLastname("None").as(StepVerifier::create).expectNext(false).verifyComplete();

		repository.existsQueryByLastname("Matthews").as(StepVerifier::create).expectNext(true).verifyComplete();
		repository.existsQueryByLastname("None").as(StepVerifier::create).expectNext(false).verifyComplete();
	}

	interface UserRepository extends ReactiveCassandraRepository<User, String> {

		Flux<User> findByLastname(String lastname);

		Mono<Slice<User>> findByLastname(String firstname, Pageable pageable);

		Mono<User> findFirstByLastname(String lastname);

		Mono<User> findOneByLastname(String lastname);

		Mono<User> findByLastname(Publisher<String> lastname);

		Mono<Long> countByLastname(String lastname);

		Mono<Boolean> deleteAllById(String lastname);

		Mono<Void> deleteVoidById(String lastname);

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
