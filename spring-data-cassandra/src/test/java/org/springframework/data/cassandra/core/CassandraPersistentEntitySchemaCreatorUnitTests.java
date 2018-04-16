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
package org.springframework.data.cassandra.core;

import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.cassandra.core.cql.CqlOperations;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;

/**
 * Unit tests for {@link CassandraPersistentEntitySchemaCreator}.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 */
@RunWith(MockitoJUnitRunner.class)
public class CassandraPersistentEntitySchemaCreatorUnitTests extends CassandraPersistentEntitySchemaTestSupport {

	@Mock CassandraAdminOperations adminOperations;
	@Mock CqlOperations operations;

	CassandraMappingContext context = new CassandraMappingContext();

	@Before
	public void setUp() {

		context.setUserTypeResolver(typeName -> {
			// make sure that calls to this method pop up. Calling UserTypeResolver while resolving
			// to be created user types isn't a good idea because they do not exist at resolution time.
			throw new IllegalArgumentException(String.format("Type %s not found", typeName));
		});

		when(adminOperations.getCqlOperations()).thenReturn(operations);
	}

	@Test // DATACASS-172, DATACASS-406
	public void createsCorrectTypeForSimpleTypes() {

		context.getPersistentEntity(MoonType.class);
		context.getPersistentEntity(PlanetType.class);

		CassandraPersistentEntitySchemaCreator schemaCreator =
				new CassandraPersistentEntitySchemaCreator(context, adminOperations);

		schemaCreator.createUserTypes(false);

		verifyTypesGetCreatedInOrderFor("universetype", "moontype", "planettype");
	}

	@Test // DATACASS-406
	public void createsCorrectTypeForSets() {

		context.getPersistentEntity(PlanetType.class);

		CassandraPersistentEntitySchemaCreator schemaCreator =
				new CassandraPersistentEntitySchemaCreator(context, adminOperations);

		schemaCreator.createUserTypes(false);

		verify(operations).execute(matches("CREATE TYPE planettype .* set<.*moontype>.*"));

		verifyTypesGetCreatedInOrderFor("universetype", "moontype", "planettype");
	}

	@Test // DATACASS-406
	public void createsCorrectTypeForLists() {

		context.getPersistentEntity(SpaceAgencyType.class);

		CassandraPersistentEntitySchemaCreator schemaCreator =
				new CassandraPersistentEntitySchemaCreator(context, adminOperations);

		schemaCreator.createUserTypes(false);

		verify(operations).execute(matches("CREATE TYPE spaceagencytype .* list<.*astronauttype>.*"));

		verifyTypesGetCreatedInOrderFor("astronauttype", "spaceagencytype");
	}

	@Test // DATACASS-406
	public void createsCorrectTypesForNestedTypes() {

		context.getPersistentEntity(PlanetType.class);

		CassandraPersistentEntitySchemaCreator schemaCreator =
				new CassandraPersistentEntitySchemaCreator(context, adminOperations);

		schemaCreator.createUserTypes(false);

		verifyTypesGetCreatedInOrderFor("universetype", "moontype", "planettype");
	}

	@Test // DATACASS-213
	public void createsIndexes() {

		context.getPersistentEntity(IndexedEntity.class);

		CassandraPersistentEntitySchemaCreator schemaCreator =
				new CassandraPersistentEntitySchemaCreator(context, adminOperations);

		schemaCreator.createIndexes(false);

		verify(operations).execute("CREATE INDEX ON indexedentity (firstname);");
	}

	private void verifyTypesGetCreatedInOrderFor(String... typenames) {

		InOrder inOrder = Mockito.inOrder(operations);

		for (String typename : typenames) {
			inOrder.verify(operations).execute(Mockito.contains("CREATE TYPE " + typename));
		}
	}
}
