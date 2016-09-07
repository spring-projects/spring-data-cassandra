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
package org.springframework.data.cassandra.core;

import static org.mockito.Mockito.*;

import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.cassandra.core.cql.CqlIdentifier;
import org.springframework.data.cassandra.mapping.BasicCassandraMappingContext;
import org.springframework.data.cassandra.mapping.UserDefinedType;
import org.springframework.data.cassandra.mapping.UserTypeResolver;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.UserType;

import lombok.Data;

/**
 * Unit tests for {@link CassandraPersistentEntitySchemaCreator}.
 * 
 * @author Mark Paluch.
 */
@RunWith(MockitoJUnitRunner.class)
public class CassandraPersistentEntitySchemaCreatorUnitTests {

	@Mock CassandraAdminOperations operations;
	@Mock KeyspaceMetadata metadata;
	@Mock UserType universetype;
	@Mock UserType moontype;
	@Mock UserType manufacturertype;
	@Mock UserType biketype;
	@Mock UserType tiretype;

	BasicCassandraMappingContext context = new BasicCassandraMappingContext();

	@Before
	public void setUp() throws Exception {

		context.setUserTypeResolver(new UserTypeResolver() {
			@Override
			public UserType resolveType(CqlIdentifier typeName) {
				return metadata.getUserType(typeName.toCql());
			}
		});
	}

	@Test
	public void shouldCreateTypesInOrder() throws Exception {

		context.getPersistentEntity(MoonType.class);
		context.getPersistentEntity(PlanetType.class);
		context.getPersistentEntity(UniverseType.class);

		when(metadata.getUserType("universetype")).thenReturn(universetype);
		when(metadata.getUserType("moontype")).thenReturn(moontype);

		CassandraPersistentEntitySchemaCreator schemaCreator = new CassandraPersistentEntitySchemaCreator(context,
				operations);

		schemaCreator.createUserTypes(false, false, false);

		verify(operations).execute(Mockito.contains("CREATE TYPE universetype"));
		verify(operations).execute(Mockito.contains("CREATE TYPE moontype"));
		verify(operations).execute(Mockito.contains("CREATE TYPE planettype"));

		InOrder inOrder = Mockito.inOrder(operations);

		inOrder.verify(operations).execute(Mockito.contains("CREATE TYPE universetype"));
		inOrder.verify(operations).execute(Mockito.contains("CREATE TYPE moontype"));
		inOrder.verify(operations).execute(Mockito.contains("CREATE TYPE planettype"));
	}

	/**
	 * @author Mark Paluch
	 */
	@UserDefinedType
	@Data
	static class UniverseType {
		String name;
	}

	/**
	 * @author Mark Paluch
	 */
	@UserDefinedType
	static class MoonType {

		UniverseType universeType;
	}

	/**
	 * @author Mark Paluch
	 */
	@UserDefinedType
	static class PlanetType {

		Set<MoonType> moons;
		UniverseType universeType;
	}
}
