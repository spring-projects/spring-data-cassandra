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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.springframework.cassandra.core.cql.generator.CreateUserTypeCqlGenerator.*;

import org.junit.Test;
import org.springframework.cassandra.core.keyspace.CreateUserTypeSpecification;

import com.datastax.driver.core.DataType;

/**
 * Unit tests for {@link CreateUserTypeCqlGenerator}.
 * 
 * @author Mark Paluch
 */
public class CreateUserTypeCqlGeneratorUnitTests {

	/**
	 * @see DATACASS-172
	 */
	@Test
	public void createUserType() {

		CreateUserTypeSpecification spec = CreateUserTypeSpecification //
				.createType("address") //
				.field("city", DataType.varchar());

		assertThat(toCql(spec), is(equalTo("CREATE TYPE address (city varchar);")));
	}

	/**
	 * @see DATACASS-172
	 */
	@Test
	public void createMultiFieldUserType() {

		CreateUserTypeSpecification spec = CreateUserTypeSpecification //
				.createType("address") //
				.field("zip", DataType.ascii()) //
				.field("city", DataType.varchar());

		assertThat(toCql(spec), is(equalTo("CREATE TYPE address (zip ascii, city varchar);")));
	}

	/**
	 * @see DATACASS-172
	 */
	@Test
	public void createUserTypeIfNotExists() {

		CreateUserTypeSpecification spec = CreateUserTypeSpecification //
				.createType() //
				.name("address").ifNotExists().field("zip", DataType.ascii()) //
				.field("city", DataType.varchar());

		assertThat(toCql(spec), is(equalTo("CREATE TYPE IF NOT EXISTS address (zip ascii, city varchar);")));
	}

	/**
	 * @see DATACASS-172
	 */
	@Test(expected = IllegalArgumentException.class)
	public void generationFailsIfNameIsNotSet() {
		toCql(CreateUserTypeSpecification.createType());
	}

	/**
	 * @see DATACASS-172
	 */
	@Test(expected = IllegalArgumentException.class)
	public void generationFailsWithoutFields() {
		toCql(CreateUserTypeSpecification.createType().name("hello"));
	}
}
