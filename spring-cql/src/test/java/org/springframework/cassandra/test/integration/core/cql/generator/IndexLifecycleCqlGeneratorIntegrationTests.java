/*
 * Copyright 2013-2015 the original author or authors.
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

import static org.springframework.cassandra.test.integration.core.cql.generator.CqlIndexSpecificationAssertions.assertIndex;
import static org.springframework.cassandra.test.integration.core.cql.generator.CqlIndexSpecificationAssertions.assertNoIndex;

import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cassandra.test.integration.AbstractKeyspaceCreatingIntegrationTest;
import org.springframework.cassandra.test.unit.core.cql.generator.CreateIndexCqlGeneratorTests;
import org.springframework.cassandra.test.unit.core.cql.generator.DropIndexCqlGeneratorTests;

/**
 * Integration tests that reuse unit tests.
 * 
 * @author Matthew T. Adams
 * @author Oliver Gierke
 */
public class IndexLifecycleCqlGeneratorIntegrationTests extends AbstractKeyspaceCreatingIntegrationTest {

	Logger log = LoggerFactory.getLogger(IndexLifecycleCqlGeneratorIntegrationTests.class);

	/**
	 * This loads any test specific Cassandra objects
	 */
	@Rule
	public CassandraCQLUnit cassandraCQLUnit = new CassandraCQLUnit(new ClassPathCQLDataSet(
			"integration/cql/generator/CreateIndexCqlGeneratorIntegrationTests-BasicTest.cql", this.keyspace),
			CASSANDRA_CONFIG);

	@Test
	public void lifecycleTest() {

		CreateIndexCqlGeneratorTests.BasicTest createTest = new CreateIndexCqlGeneratorTests.BasicTest();
		DropIndexCqlGeneratorTests.BasicTest dropTest = new DropIndexCqlGeneratorTests.BasicTest();
		DropIndexCqlGeneratorTests.IfExistsTest dropIfExists = new DropIndexCqlGeneratorTests.IfExistsTest();

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
