/*
 * Copyright 2013-2016 the original author or authors.
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

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.cassandra.core.cql.generator.CreateTableCqlGenerator;
import org.springframework.cassandra.core.keyspace.CreateTableSpecification;
import org.springframework.cassandra.test.integration.AbstractKeyspaceCreatingIntegrationTest;
import org.springframework.cassandra.test.unit.core.cql.generator.CreateTableCqlGeneratorTests.FunkyTableNameTest;

import com.datastax.driver.core.DataType;

/**
 * @author Matthew T. Adams
 * @author Oliver Gierke
 * @author Mark Paluch
 */
@RunWith(Parameterized.class)
public class FunkyIdentifierIntegrationTest extends AbstractKeyspaceCreatingIntegrationTest {

	private String cql;

	public FunkyIdentifierIntegrationTest(String cql) {
		super(FunkyIdentifierIntegrationTest.class.getSimpleName());
		this.cql = cql;
	}

	@Parameters(name = "{0}")
	public static List<Object[]> parameters() {

		List<Object[]> parameters = new ArrayList<Object[]>();
		List<CreateTableCqlGenerator> tableCqlGenerators = new ArrayList<CreateTableCqlGenerator>();

		String table = "funky";
		int i = 0;
		for (String name : FunkyTableNameTest.FUNKY_LEGAL_NAMES) {
			tableCqlGenerators.add(new CreateTableCqlGenerator(
					CreateTableSpecification.createTable().name(table + i++).partitionKeyColumn(name, DataType.text())));
		}

		for (String name : FunkyTableNameTest.FUNKY_LEGAL_NAMES) {
			tableCqlGenerators.add(new CreateTableCqlGenerator(
					CreateTableSpecification.createTable().name(name).partitionKeyColumn(name, DataType.text())));
		}

		for (CreateTableCqlGenerator tableCqlGenerator : tableCqlGenerators) {
			parameters.add(new Object[] { tableCqlGenerator.toCql() });
		}

		return parameters;
	}

	@Test
	public void execute() throws Exception {
		session.execute(cql);
	}
}
