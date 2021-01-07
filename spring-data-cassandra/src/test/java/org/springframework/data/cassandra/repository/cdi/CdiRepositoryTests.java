/*
 * Copyright 2014-2021 the original author or authors.
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
package org.springframework.data.cassandra.repository.cdi;

import static org.assertj.core.api.Assertions.*;

import java.util.Optional;

import javax.enterprise.inject.se.SeContainer;
import javax.enterprise.inject.se.SeContainerInitializer;

import org.junit.AfterClass;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.data.cassandra.domain.User;
import org.springframework.data.cassandra.test.util.IntegrationTestsSupport;

/**
 * @author Mohsin Husen
 * @author Mark Paluch
 */
class CdiRepositoryTests extends IntegrationTestsSupport {

	private static SeContainer cdiContainer;

	private CdiUserRepository repository;
	private SamplePersonRepository personRepository;
	private QualifiedUserRepository qualifiedUserRepository;

	@BeforeAll
	static void init() throws Exception {

		// CDI container is booted before the @Rule can be triggered.
		// Ensure that we have a usable Cassandra instance otherwise the container won't boot
		// because it needs a CassandraOperations with a working Session/Cluster

		cdiContainer = SeContainerInitializer.newInstance() //
				.disableDiscovery() //
				.addPackages(CdiRepositoryClient.class) //
				.initialize();
	}

	@AfterClass
	static void shutdown() throws Exception {
		cdiContainer.close();
	}

	@BeforeEach
	void setUp() {

		CdiRepositoryClient client = cdiContainer.select(CdiRepositoryClient.class).get();
		repository = client.getRepository();
		personRepository = client.getSamplePersonRepository();
		qualifiedUserRepository = client.getQualifiedUserRepository();
	}

	@Test // DATACASS-149, DATACASS-495
	void testCdiRepository() {

		assertThat(repository).isNotNull();

		repository.deleteAll();

		User bean = new User();
		bean.setId("username");
		bean.setFirstname("first");
		bean.setLastname("last");

		repository.save(bean);

		assertThat(repository.existsById(bean.getId())).isTrue();

		Optional<User> retrieved = repository.findById(bean.getId());

		assertThat(retrieved).hasValueSatisfying(actual -> {
			assertThat(actual.getId()).isEqualTo(bean.getId());
			assertThat(actual.getFirstname()).isEqualTo(bean.getFirstname());
			assertThat(actual.getLastname()).isEqualTo(bean.getLastname());
		});

		assertThat(repository.count()).isEqualTo(1);
		assertThat(repository.existsById(bean.getId())).isTrue();

		repository.delete(bean);

		assertThat(repository.count()).isEqualTo(0);
		assertThat(repository.findById(bean.getId())).isNotPresent();
	}

	@Test // DATACASS-249, DATACASS-495
	void testQualifiedCdiRepository() {

		assertThat(qualifiedUserRepository).isNotNull();
		qualifiedUserRepository.deleteAll();

		User bean = new User();
		bean.setId("username");
		bean.setFirstname("first");
		bean.setLastname("last");

		qualifiedUserRepository.save(bean);

		assertThat(qualifiedUserRepository.existsById(bean.getId())).isTrue();
	}

	@Test // DATACASS-149, DATACASS-495
	void returnOneFromCustomImpl() {
		assertThat(personRepository.returnOne()).isEqualTo(1);
	}
}
