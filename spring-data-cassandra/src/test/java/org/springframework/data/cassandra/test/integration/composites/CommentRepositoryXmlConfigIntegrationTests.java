/*
 * Copyright 2013-2017 the original author or authors
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
package org.springframework.data.cassandra.test.integration.composites;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.test.integration.repository.simple.UserRepository;
import org.springframework.data.cassandra.test.integration.support.AbstractSpringDataEmbeddedCassandraIntegrationTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Base class for xml config tests for {@link UserRepository}.
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class CommentRepositoryXmlConfigIntegrationTests extends AbstractSpringDataEmbeddedCassandraIntegrationTest {

	@Autowired private CommentRepository repository;

	@Autowired private CassandraOperations template;

	private CommentRepositoryIntegrationTests tests;

	@Before
	public void setUp() throws InterruptedException {
		tests = new CommentRepositoryIntegrationTests(repository, template);
		tests.before();
	}

	@Test
	public void testInsert() {
		tests.testInsert();
	}

	@Test
	public void testDelete() {
		tests.testDelete();
	}

	@Test
	public void testUpdateNonKeyField() {
		tests.testUpdateNonKeyField();
	}
}
