/*
 * Copyright 2016-2018 the original author or authors.
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
package org.springframework.data.cassandra.core.cql.generator;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.cassandra.core.cql.generator.CreateUserTypeCqlGenerator.*;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.cassandra.core.cql.keyspace.CreateUserTypeSpecification;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTest;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.UserType;

/**
 * Integration tests for {@link CreateUserTypeCqlGenerator}.
 *
 * @author Mark Paluch
 */
public class CreateUserTypeCqlGeneratorIntegrationTests extends AbstractKeyspaceCreatingIntegrationTest {

	@Before
	public void setUp() throws Exception {

		session.execute("DROP TYPE IF EXISTS person;");
		session.execute("DROP TYPE IF EXISTS address;");
	}

	@Test // DATACASS-172
	public void createUserType() {

		CreateUserTypeSpecification spec = CreateUserTypeSpecification //
				.createType("address") //
				.field("zip", DataType.ascii()) //
				.field("city", DataType.varchar());

		session.execute(toCql(spec));

		KeyspaceMetadata keyspace = session.getCluster().getMetadata().getKeyspace(session.getLoggedKeyspace());
		UserType address = keyspace.getUserType("address");
		assertThat(address.getFieldNames()).contains("zip", "city");
	}

	@Test // DATACASS-172
	public void createUserTypeIfNotExists() {

		CreateUserTypeSpecification spec = CreateUserTypeSpecification //
				.createType("address").ifNotExists().field("zip", DataType.ascii()) //
				.field("city", DataType.varchar());

		session.execute(toCql(spec));

		KeyspaceMetadata keyspace = session.getCluster().getMetadata().getKeyspace(session.getLoggedKeyspace());
		UserType address = keyspace.getUserType("address");
		assertThat(address.getFieldNames()).contains("zip", "city");
	}

	@Test // DATACASS-172, DATACASS-424
	public void createNestedUserType() {

		CreateUserTypeSpecification addressSpec = CreateUserTypeSpecification //
				.createType("address").ifNotExists().field("zip", DataType.ascii()) //
				.field("city", DataType.varchar());

		session.execute(toCql(addressSpec));

		KeyspaceMetadata keyspace = session.getCluster().getMetadata().getKeyspace(session.getLoggedKeyspace());
		UserType address = keyspace.getUserType("address");

		CreateUserTypeSpecification personSpec = CreateUserTypeSpecification //
				.createType("person").ifNotExists().field("address", address.copy(true)) //
				.field("city", DataType.varchar());

		session.execute(toCql(personSpec));
	}
}
