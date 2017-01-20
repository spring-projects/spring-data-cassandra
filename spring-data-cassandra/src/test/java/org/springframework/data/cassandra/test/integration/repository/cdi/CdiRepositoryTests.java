/*
 * Copyright 2014-2017 the original author or authors.
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
package org.springframework.data.cassandra.test.integration.repository.cdi;

import static org.assertj.core.api.Assertions.*;

import java.util.Optional;

import org.apache.webbeans.cditest.CdiTestContainer;
import org.apache.webbeans.cditest.CdiTestContainerLoader;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.cassandra.test.integration.AbstractEmbeddedCassandraIntegrationTest;
import org.springframework.data.cassandra.test.integration.repository.simple.User;

/**
 * @author Mohsin Husen
 * @author Mark Paluch
 */
public class CdiRepositoryTests extends AbstractEmbeddedCassandraIntegrationTest {

	private static CdiTestContainer cdiContainer;
	private CdiUserRepository repository;
	private SamplePersonRepository personRepository;
	private QualifiedUserRepository qualifiedUserRepository;

	@BeforeClass
	public static void init() throws Exception {
		// CDI container is booted before the @Rule can be triggered.
		// Ensure that we have a usable Cassandra instance otherwise the container won't boot
		// because it needs a CassandraOperations with a working Session/Cluster

		cdiContainer = CdiTestContainerLoader.getCdiContainer();
		cdiContainer.startApplicationScope();
		cdiContainer.startContexts();
		cdiContainer.bootContainer();
	}

	@AfterClass
	public static void shutdown() throws Exception {

		cdiContainer.stopContexts();
		cdiContainer.shutdownContainer();
	}

	@Before
	public void setUp() {

		CdiRepositoryClient client = cdiContainer.getInstance(CdiRepositoryClient.class);
		repository = client.getRepository();
		personRepository = client.getSamplePersonRepository();
		qualifiedUserRepository = client.getQualifiedUserRepository();
	}

	@Test // DATACASS-149
	public void testCdiRepository() {

		assertThat(repository).isNotNull();

		repository.deleteAll();

		User bean = new User();
		bean.setUsername("username");
		bean.setFirstName("first");
		bean.setLastName("last");

		repository.save(bean);

		assertThat(repository.exists(bean.getUsername())).isTrue();

		Optional<User> retrieved = repository.findOne(bean.getUsername());

		assertThat(retrieved).hasValueSatisfying(actual -> {
			assertThat(actual.getUsername()).isEqualTo(bean.getUsername());
			assertThat(actual.getFirstName()).isEqualTo(bean.getFirstName());
			assertThat(actual.getLastName()).isEqualTo(bean.getLastName());
		});

		assertThat(repository.count()).isEqualTo(1);
		assertThat(repository.exists(bean.getUsername())).isTrue();

		repository.delete(bean);

		assertThat(repository.count()).isEqualTo(0);
		assertThat(repository.findOne(bean.getUsername())).isNotPresent();
	}

	@Test // DATACASS-249
	public void testQualifiedCdiRepository() {

		assertThat(qualifiedUserRepository).isNotNull();
		qualifiedUserRepository.deleteAll();

		User bean = new User();
		bean.setUsername("username");
		bean.setFirstName("first");
		bean.setLastName("last");

		qualifiedUserRepository.save(bean);

		assertThat(qualifiedUserRepository.exists(bean.getUsername())).isTrue();
	}

	@Test // DATACASS-149
	public void returnOneFromCustomImpl() {

		assertThat(personRepository.returnOne()).isEqualTo(1);
	}
}
