/*
 * Copyright 2013-2015 the original author or authors.
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

import org.junit.Test;
import org.springframework.cassandra.core.cql.generator.CreateTableCqlGenerator;
import org.springframework.cassandra.core.keyspace.CreateTableSpecification;
import org.springframework.cassandra.test.integration.AbstractKeyspaceCreatingIntegrationTest;
import org.springframework.cassandra.test.unit.core.cql.generator.CreateTableCqlGeneratorTests.FunkyTableNameTest;

import com.datastax.driver.core.DataType;

/**
 * @author Matthew T. Adams
 * @author Oliver Gierke
 */
public class FunkyIdentifierIntegrationTest extends AbstractKeyspaceCreatingIntegrationTest {

	public FunkyIdentifierIntegrationTest() {
		super(randomKeyspaceName());
	}

	@Test
	public void testFunkyTableName() {
		for (String name : FunkyTableNameTest.FUNKY_LEGAL_NAMES) {
			session.execute(new CreateTableCqlGenerator(CreateTableSpecification.createTable().name(name)
					.partitionKeyColumn("key", DataType.text())).toCql());
		}
	}

	@Test
	public void testFunkyColumnName() {
		String table = "funky";
		int i = 0;
		for (String name : FunkyTableNameTest.FUNKY_LEGAL_NAMES) {
			session.execute(new CreateTableCqlGenerator(CreateTableSpecification.createTable().name(table + i++)
					.partitionKeyColumn(name, DataType.text())).toCql());
		}
	}

	@Test
	public void testFunkyTableAndColumnName() {
		for (String name : FunkyTableNameTest.FUNKY_LEGAL_NAMES) {
			session.execute(new CreateTableCqlGenerator(CreateTableSpecification.createTable().name(name)
					.partitionKeyColumn(name, DataType.text())).toCql());
		}
	}
}
