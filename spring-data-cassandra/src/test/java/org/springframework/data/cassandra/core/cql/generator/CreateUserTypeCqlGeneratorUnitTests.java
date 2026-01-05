/*
 * Copyright 2016-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core.cql.generator;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;

import org.springframework.data.cassandra.core.cql.keyspace.CreateUserTypeSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.SpecificationBuilder;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataTypes;

/**
 * Unit tests for {@link CreateUserTypeCqlGenerator}.
 *
 * @author Mark Paluch
 */
class CreateUserTypeCqlGeneratorUnitTests {

	@Test // DATACASS-172
	void createUserType() {

		CreateUserTypeSpecification spec = SpecificationBuilder //
				.createType("address") //
				.field("city", DataTypes.TEXT);

		assertThat(CqlGenerator.toCql(spec)).isEqualTo("CREATE TYPE address (city text);");
	}

	@Test // DATACASS-921
	void shouldConsiderKeyspace() {

		CreateUserTypeSpecification spec = CreateUserTypeSpecification //
				.createType(CqlIdentifier.fromCql("myks"), CqlIdentifier.fromCql("address")) //
				.field("city", DataTypes.TEXT);

		assertThat(CqlGenerator.toCql(spec)).isEqualTo("CREATE TYPE myks.address (city text);");
	}

	@Test // DATACASS-172
	void createMultiFieldUserType() {

		CreateUserTypeSpecification spec = CreateUserTypeSpecification //
				.createType("address") //
				.field("zip", DataTypes.ASCII) //
				.field("city", DataTypes.TEXT);

		assertThat(CqlGenerator.toCql(spec)).isEqualTo("CREATE TYPE address (zip ascii, city text);");
	}

	@Test // DATACASS-172
	void createUserTypeIfNotExists() {

		CreateUserTypeSpecification spec = CreateUserTypeSpecification //
				.createType("address").ifNotExists().field("zip", DataTypes.ASCII) //
				.field("city", DataTypes.TEXT);

		assertThat(CqlGenerator.toCql(spec)).isEqualTo("CREATE TYPE IF NOT EXISTS address (zip ascii, city text);");
	}

	@Test // DATACASS-172
	void generationFailsWithoutFields() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> CqlGenerator.toCql(CreateUserTypeSpecification.createType("hello")));
	}
}
