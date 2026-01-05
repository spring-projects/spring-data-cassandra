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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.data.cassandra.core.cql.keyspace.CreateTableSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.CreateUserTypeSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.SpecificationBuilder;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTests;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.metadata.schema.ShallowUserDefinedType;

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

		session.execute("DROP KEYSPACE IF EXISTS CreateUserTypeCqlGenerator_it;");
		session.execute(
				"CREATE KEYSPACE CreateUserTypeCqlGenerator_it WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
	}

	@Test // DATACASS-172
	void createUserType() {

		CreateUserTypeSpecification spec = SpecificationBuilder //
				.createType("address") //
				.field("zip", DataTypes.ASCII) //
				.field("city", DataTypes.TEXT);

		session.execute(CqlGenerator.toCql(spec));

		KeyspaceMetadata keyspace = session.getMetadata().getKeyspace(session.getKeyspace().get()).get();
		UserDefinedType address = keyspace.getUserDefinedType("address").get();
		assertThat(address.getFieldNames()).contains(CqlIdentifier.fromCql("zip"), CqlIdentifier.fromCql("city"));
	}

	@Test // DATACASS-172
	void createUserTypeIfNotExists() {

		CreateUserTypeSpecification spec = SpecificationBuilder //
				.createType("address").ifNotExists().field("zip", DataTypes.ASCII) //
				.field("city", DataTypes.TEXT);

		session.execute(CqlGenerator.toCql(spec));

		KeyspaceMetadata keyspace = session.getMetadata().getKeyspace(session.getKeyspace().get()).get();
		UserDefinedType address = keyspace.getUserDefinedType("address").get();
		assertThat(address.getFieldNames()).contains(CqlIdentifier.fromCql("zip"), CqlIdentifier.fromCql("city"));
	}

	@Test // DATACASS-172, DATACASS-424
	void createNestedUserType() {

		CreateUserTypeSpecification addressSpec = SpecificationBuilder //
				.createType("address").ifNotExists().field("zip", DataTypes.ASCII) //
				.field("city", DataTypes.TEXT);

		session.execute(CqlGenerator.toCql(addressSpec));

		KeyspaceMetadata keyspace = session.getMetadata().getKeyspace(session.getKeyspace().get()).get();
		UserDefinedType address = keyspace.getUserDefinedType("address").get();

		CreateUserTypeSpecification personSpec = CreateUserTypeSpecification //
				.createType("person").ifNotExists().field("address", address.copy(true)) //
				.field("city", DataTypes.TEXT);

		session.execute(CqlGenerator.toCql(personSpec));
	}

	@Test // DATACASS-172
	void shouldGenerateTypeAndTableInOtherKeyspace() {

		CreateUserTypeSpecification spec = SpecificationBuilder //
				.createType(CqlIdentifier.fromCql("CreateUserTypeCqlGenerator_it"), CqlIdentifier.fromCql("address"))
				.ifNotExists().field("zip", DataTypes.ASCII) //
				.field("city", DataTypes.TEXT);

		session.execute(CqlGenerator.toCql(spec));

		CreateTableSpecification table = SpecificationBuilder
				.createTable(CqlIdentifier.fromCql("CreateUserTypeCqlGenerator_it"), CqlIdentifier.fromCql("person"))
				.partitionKeyColumn("id", DataTypes.ASCII)//
				.column("udtref", new ShallowUserDefinedType(CqlIdentifier.fromCql("CreateUserTypeCqlGenerator_it"),
						CqlIdentifier.fromCql("address"), true));

		session.execute(CqlGenerator.toCql(table));

		KeyspaceMetadata keyspace = session.getMetadata().getKeyspace("CreateUserTypeCqlGenerator_it").get();
		UserDefinedType address = keyspace.getUserDefinedType("address").get();
		assertThat(address.getFieldNames()).contains(CqlIdentifier.fromCql("zip"), CqlIdentifier.fromCql("city"));
	}
}
