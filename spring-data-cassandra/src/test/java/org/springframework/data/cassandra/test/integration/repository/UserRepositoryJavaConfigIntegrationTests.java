/*
 * Copyright 2011-2013 the original author or authors.
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
package org.springframework.data.cassandra.test.integration.repository;

import static org.springframework.cassandra.core.keyspace.CreateKeyspaceSpecification.createKeyspace;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cassandra.core.keyspace.CreateKeyspaceSpecification;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.config.SchemaAction;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;
import org.springframework.data.cassandra.test.integration.AbstractSpringDataEmbeddedCassandraIntegrationTest;
import org.springframework.data.cassandra.test.integration.support.AbstractDataTestJavaConfig;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Base class for Java config tests for {@link UserRepository}.
 * 
 * @author Matthew T. Adams
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class UserRepositoryJavaConfigIntegrationTests extends AbstractSpringDataEmbeddedCassandraIntegrationTest {

	@Configuration
	@EnableCassandraRepositories(basePackageClasses = UserRepository.class)
	public static class Config extends AbstractDataTestJavaConfig {

		@Override
		protected String getKeyspaceName() {
			return UserRepositoryJavaConfigIntegrationTests.class.getSimpleName();
		}

		@Override
		protected List<CreateKeyspaceSpecification> getKeyspaceCreations() {
			List<CreateKeyspaceSpecification> creates = new ArrayList<CreateKeyspaceSpecification>();

			creates.add(createKeyspace().name(getKeyspaceName()).withSimpleReplication());

			return creates;
		}

		@Override
		public SchemaAction getSchemaAction() {
			return SchemaAction.RECREATE;
		}

		@Override
		public String getEntityBasePackage() {
			return User.class.getPackage().getName();
		}
	}

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
