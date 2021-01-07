/*
 * Copyright 2016-2021 the original author or authors.
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
import static org.springframework.data.cassandra.core.cql.generator.CreateUserTypeCqlGenerator.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.data.cassandra.core.cql.keyspace.CreateUserTypeSpecification;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTests;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;

/**
 * Integration tests for {@link CreateUserTypeCqlGenerator}.
 *
 * @author Mark Paluch
 */
class CreateUserTypeCqlGeneratorIntegrationTests extends AbstractKeyspaceCreatingIntegrationTests {

	@BeforeEach
	void setUp() throws Exception {

		session.execute("DROP TYPE IF EXISTS person;");
		session.execute("DROP TYPE IF EXISTS address;");
	}

	@Test // DATACASS-172
	void createUserType() {

		CreateUserTypeSpecification spec = CreateUserTypeSpecification //
				.createType("address") //
				.field("zip", DataTypes.ASCII) //
				.field("city", DataTypes.TEXT);

		session.execute(toCql(spec));

		KeyspaceMetadata keyspace = session.getMetadata().getKeyspace(session.getKeyspace().get()).get();
		UserDefinedType address = keyspace.getUserDefinedType("address").get();
		assertThat(address.getFieldNames()).contains(CqlIdentifier.fromCql("zip"), CqlIdentifier.fromCql("city"));
	}

	@Test // DATACASS-172
	void createUserTypeIfNotExists() {

		CreateUserTypeSpecification spec = CreateUserTypeSpecification //
				.createType("address").ifNotExists().field("zip", DataTypes.ASCII) //
				.field("city", DataTypes.TEXT);

		session.execute(toCql(spec));

		KeyspaceMetadata keyspace = session.getMetadata().getKeyspace(session.getKeyspace().get()).get();
		UserDefinedType address = keyspace.getUserDefinedType("address").get();
		assertThat(address.getFieldNames()).contains(CqlIdentifier.fromCql("zip"), CqlIdentifier.fromCql("city"));
	}

	@Test // DATACASS-172, DATACASS-424
	void createNestedUserType() {

		CreateUserTypeSpecification addressSpec = CreateUserTypeSpecification //
				.createType("address").ifNotExists().field("zip", DataTypes.ASCII) //
				.field("city", DataTypes.TEXT);

		session.execute(toCql(addressSpec));

		KeyspaceMetadata keyspace = session.getMetadata().getKeyspace(session.getKeyspace().get()).get();
		UserDefinedType address = keyspace.getUserDefinedType("address").get();

		CreateUserTypeSpecification personSpec = CreateUserTypeSpecification //
				.createType("person").ifNotExists().field("address", address.copy(true)) //
				.field("city", DataTypes.TEXT);

		session.execute(toCql(personSpec));
	}
}
