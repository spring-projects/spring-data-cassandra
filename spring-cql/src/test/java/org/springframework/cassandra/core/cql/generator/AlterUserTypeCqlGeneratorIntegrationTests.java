/*
 * Copyright 2016 the original author or authors.
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
package org.springframework.cassandra.core.cql.generator;

import static org.springframework.cassandra.core.cql.generator.AlterUserTypeCqlGenerator.*;

import org.junit.Before;
import org.junit.Test;
import org.springframework.cassandra.core.keyspace.AlterUserTypeSpecification;
import org.springframework.cassandra.test.integration.AbstractKeyspaceCreatingIntegrationTest;

import com.datastax.driver.core.DataType;

/**
 * Integration tests for {@link AlterUserTypeCqlGenerator}.
 * 
 * @author Mark Paluch
 */
public class AlterUserTypeCqlGeneratorIntegrationTests extends AbstractKeyspaceCreatingIntegrationTest {

	@Before
	public void setUp() throws Exception {

		session.execute("DROP TYPE IF EXISTS address;");
		session.execute("CREATE TYPE address (zip text, state text);");
	}

	/**
	 * @see DATACASS-172
	 */
	@Test
	public void alterTypeShouldAddField() {

		AlterUserTypeSpecification spec = AlterUserTypeSpecification.alterType("address")//
				.add("street", DataType.varchar());

		session.execute(toCql(spec));
	}

	/**
	 * @see DATACASS-172
	 */
	@Test
	public void alterTypeShouldAlterField() {

		AlterUserTypeSpecification spec = AlterUserTypeSpecification.alterType("address")//
				.alter("zip", DataType.varchar());

		session.execute(toCql(spec));
	}

	/**
	 * @see DATACASS-172
	 */
	@Test
	public void alterTypeShouldRenameField() {

		AlterUserTypeSpecification spec = AlterUserTypeSpecification.alterType("address")//
				.rename("zip", "zap");

		session.execute(toCql(spec));
	}

	/**
	 * @see DATACASS-172
	 */
	@Test
	public void alterTypeShouldRenameFields() {

		AlterUserTypeSpecification spec = AlterUserTypeSpecification.alterType("address")//
				.rename("zip", "zap") //
				.rename("state", "county");

		session.execute(toCql(spec));
	}

	/**
	 * @see DATACASS-172
	 */
	@Test(expected = IllegalArgumentException.class)
	public void generationFailsIfNameIsNotSet() {
		toCql(AlterUserTypeSpecification.alterType());
	}

	/**
	 * @see DATACASS-172
	 */
	@Test(expected = IllegalArgumentException.class)
	public void generationFailsWithoutFields() {
		toCql(AlterUserTypeSpecification.alterType().name("hello"));
	}

}
