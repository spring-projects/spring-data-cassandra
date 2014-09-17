/*
 * Copyright 2014 the original author or authors.
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

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import org.apache.webbeans.cditest.CdiTestContainer;
import org.apache.webbeans.cditest.CdiTestContainerLoader;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.cassandra.test.integration.AbstractEmbeddedCassandraIntegrationTest;
import org.springframework.data.cassandra.test.integration.repository.User;

/**
 * @author Mohsin Husen
 * @author Mark Paluch
 */

public class CdiRepositoryTests extends AbstractEmbeddedCassandraIntegrationTest {

	private static CdiTestContainer cdiContainer;
	private CdiUserRepository repository;
	private SamplePersonRepository personRepository;

	@BeforeClass
	public static void init() throws Exception {
		startCassandra();
		cdiContainer = CdiTestContainerLoader.getCdiContainer();
		cdiContainer.startApplicationScope();
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
	}

	@Test
	public void testCdiRepository() {
		assertNotNull(repository);

		User bean = new User();
		bean.setUsername("username");
		bean.setFirstName("first");
		bean.setLastName("last");

		repository.save(bean);

		assertTrue(repository.exists(bean.getUsername()));

		User retrieved = repository.findOne(bean.getUsername());
		assertNotNull(retrieved);
		assertEquals(bean.getUsername(), retrieved.getUsername());
		assertEquals(bean.getFirstName(), retrieved.getFirstName());
		assertEquals(bean.getLastName(), retrieved.getLastName());

		assertEquals(1, repository.count());

		assertTrue(repository.exists(bean.getUsername()));

		repository.delete(bean);

		assertEquals(0, repository.count());
		retrieved = repository.findOne(bean.getUsername());
		assertNull(retrieved);
	}

	/**
	 * @see DATACASS-149
	 */
	@Test
	public void returnOneFromCustomImpl() {

		assertThat(personRepository.returnOne(), is(1));
	}
}
