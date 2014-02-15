/*
 * Copyright 2013-2014 the original author or authors
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.test.integration.repository;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.test.integration.support.AbstractSpringDataEmbeddedCassandraIntegrationTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Base class for xml config tests for {@link UserRepository}.
 * 
 * @author Matthew T. Adams
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class UserRepositoryXmlConfigIntegrationTests extends AbstractSpringDataEmbeddedCassandraIntegrationTest {

	@Autowired
	protected UserRepository repository;

	@Autowired
	protected CassandraOperations template;

	UserRepositoryIntegrationTests tests;

	@Before
	public void setUp() throws InterruptedException {
		tests = new UserRepositoryIntegrationTests(repository, template);
		tests.setUp();
	}

	@After
	public void after() {
		tests.after();
	}

	@Test
	public void findsUserById() throws Exception {
		tests.findsUserById();
	}

	@Test
	public void findsAll() throws Exception {
		tests.findsAll();
	}

	@Test
	public void findsAllWithGivenIds() {
		tests.findsAllWithGivenIds();
	}

	@Test
	public void deletesUserCorrectly() throws Exception {
		tests.deletesUserCorrectly();
	}

	@Test
	public void deletesUserByIdCorrectly() {
		tests.deletesUserByIdCorrectly();
	}
}
