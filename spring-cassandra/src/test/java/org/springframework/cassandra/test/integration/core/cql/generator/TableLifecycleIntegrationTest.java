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
package org.springframework.cassandra.test.integration.core.cql.generator;

import static org.springframework.cassandra.test.integration.core.cql.generator.CqlTableSpecificationAssertions.assertNoTable;
import static org.springframework.cassandra.test.integration.core.cql.generator.CqlTableSpecificationAssertions.assertTable;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cassandra.core.cql.generator.DropTableCqlGenerator;
import org.springframework.cassandra.core.keyspace.DropTableSpecification;
import org.springframework.cassandra.test.integration.AbstractEmbeddedCassandraIntegrationTest;
import org.springframework.cassandra.test.unit.core.cql.generator.AlterTableCqlGeneratorTests;
import org.springframework.cassandra.test.unit.core.cql.generator.CreateTableCqlGeneratorTests;
import org.springframework.cassandra.test.unit.core.cql.generator.DropTableCqlGeneratorTests;

/**
 * Test CREATE TABLE / ALTER TABLE / DROP TABLE
 * 
 * @author David Webb
 */
public class TableLifecycleIntegrationTest extends AbstractEmbeddedCassandraIntegrationTest {

	private final static Logger log = LoggerFactory.getLogger(TableLifecycleIntegrationTest.class);

	CreateTableCqlGeneratorTests.MultipleOptionsTest createTableTest = new CreateTableCqlGeneratorTests.MultipleOptionsTest();

	@Test
	public void testDrop() {

		createTableTest.prepare();

		log.info(createTableTest.cql);

		session.execute(createTableTest.cql);

		assertTable(createTableTest.specification, keyspace, session);

		DropTableTest dropTest = new DropTableTest();
		dropTest.prepare();

		log.info(dropTest.cql);

		session.execute(dropTest.cql);

		assertNoTable(dropTest.specification, keyspace, session);
	}

	@Test
	public void testAlter() {

		createTableTest.prepare();

		log.info(createTableTest.cql);

		session.execute(createTableTest.cql);

		assertTable(createTableTest.specification, keyspace, session);

		AlterTableCqlGeneratorTests.MultipleOptionsTest alterTest = new AlterTableCqlGeneratorTests.MultipleOptionsTest();
		alterTest.prepare();

		log.info(alterTest.cql);

		session.execute(alterTest.cql);

		// assertTable(alterTest.specification, keyspace, session);

	}

	public class DropTableTest extends DropTableCqlGeneratorTests.DropTableTest {

		/* (non-Javadoc)
		 * @see org.springframework.cassandra.test.unit.core.cql.generator.TableOperationCqlGeneratorTest#specification()
		 */
		@Override
		public DropTableSpecification specification() {
			return DropTableSpecification.dropTable().name(createTableTest.specification.getName());
		}

		/* (non-Javadoc)
		 * @see org.springframework.cassandra.test.unit.core.cql.generator.TableOperationCqlGeneratorTest#generator()
		 */
		@Override
		public DropTableCqlGenerator generator() {
			return new DropTableCqlGenerator(specification);
		}

	}

}