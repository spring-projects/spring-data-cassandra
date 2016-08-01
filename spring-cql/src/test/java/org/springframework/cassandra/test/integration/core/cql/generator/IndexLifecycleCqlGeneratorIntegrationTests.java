/*
 * Copyright 2013-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.cassandra.test.integration.core.cql.generator;

import static org.springframework.cassandra.test.integration.core.cql.generator.CqlIndexSpecificationAssertions.*;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cassandra.core.cql.generator.CreateIndexCqlGeneratorUnitTests;
import org.springframework.cassandra.core.cql.generator.DropIndexCqlGeneratorUnitTests;
import org.springframework.cassandra.test.integration.AbstractKeyspaceCreatingIntegrationTest;

/**
 * Integration tests that reuse unit tests.
 *
 * @author Matthew T. Adams
 * @author Oliver Gierke
 * @author Mark Paluch
 */
public class IndexLifecycleCqlGeneratorIntegrationTests extends AbstractKeyspaceCreatingIntegrationTest {

	private final static Logger log = LoggerFactory.getLogger(IndexLifecycleCqlGeneratorIntegrationTests.class);

	@Before
	public void setUp() throws Exception {
		execute("integration/cql/generator/CreateIndexCqlGeneratorIntegrationTests-BasicTest.cql", this.keyspace);
	}

	@Test
	public void lifecycleTest() {

		CreateIndexCqlGeneratorUnitTests.BasicTest createTest = new CreateIndexCqlGeneratorUnitTests.BasicTest();
		DropIndexCqlGeneratorUnitTests.BasicTest dropTest = new DropIndexCqlGeneratorUnitTests.BasicTest();
		DropIndexCqlGeneratorUnitTests.IfExistsTest dropIfExists = new DropIndexCqlGeneratorUnitTests.IfExistsTest();

		createTest.prepare();
		dropTest.prepare();
		dropIfExists.prepare();

		log.info(createTest.cql);
		session.execute(createTest.cql);

		assertIndex(createTest.specification, keyspace, session);

		log.info(dropTest.cql);
		session.execute(dropTest.cql);

		assertNoIndex(createTest.specification, keyspace, session);
	}
}
