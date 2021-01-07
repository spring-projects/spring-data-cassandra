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
package org.springframework.data.cassandra.core;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import org.springframework.data.cassandra.core.convert.SchemaFactory;
import org.springframework.data.cassandra.core.cql.CqlOperations;
import org.springframework.data.cassandra.core.cql.keyspace.CreateUserTypeSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.UserTypeNameSpecification;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.UserDefinedType;
import org.springframework.data.convert.CustomConversions;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;

/**
 * Unit tests for {@link CassandraPersistentEntitySchemaCreator}.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class CassandraPersistentEntitySchemaCreatorUnitTests extends CassandraPersistentEntitySchemaTestSupport {

	@Mock CassandraAdminOperations adminOperations;
	@Mock CqlOperations operations;

	private CassandraMappingContext context = new CassandraMappingContext();

	@BeforeEach
	void setUp() {

		when(adminOperations.getCqlOperations()).thenReturn(operations);
		when(adminOperations.getSchemaFactory()).thenReturn(new SchemaFactory(context,
				new CustomConversions(CustomConversions.StoreConversions.NONE, Collections.emptyList()),
				CodecRegistry.DEFAULT));
	}

	@Test // DATACASS-687
	void shouldConsiderProperUdtOrdering() {

		List<Class<?>> ordered = new ArrayList<>(Arrays.asList(Udt2.class, Udt1.class, RequiredByAll.class));

		context = new CassandraMappingContext() {
			@Override
			public Collection<CassandraPersistentEntity<?>> getUserDefinedTypeEntities() {
				return ordered.stream().map(this::getRequiredPersistentEntity).collect(Collectors.toList());
			}
		};

		context.setUserTypeResolver(typeName -> {
			// make sure that calls to this method pop up. Calling UserTypeResolver while resolving
			// to be created user types isn't a good idea because they do not exist at resolution time.
			throw new IllegalArgumentException(String.format("Type %s not found", typeName));
		});

		CassandraPersistentEntitySchemaCreator schemaCreator = new CassandraPersistentEntitySchemaCreator(context,
				adminOperations);

		List<CreateUserTypeSpecification> userTypeSpecifications = schemaCreator.createUserTypeSpecifications(false);

		List<CqlIdentifier> collect = userTypeSpecifications.stream().map(UserTypeNameSpecification::getName)
				.collect(Collectors.toList());

		assertThat(collect).hasSize(3).startsWith(CqlIdentifier.fromCql("requiredbyall"));
	}

	@Test // DATACASS-172, DATACASS-406
	void createsCorrectTypeForSimpleTypes() {

		context.getPersistentEntity(MoonType.class);
		context.getPersistentEntity(PlanetType.class);

		CassandraPersistentEntitySchemaCreator schemaCreator = new CassandraPersistentEntitySchemaCreator(context,
				adminOperations);

		schemaCreator.createUserTypes(false);

		verifyTypesGetCreatedInOrderFor("universetype", "moontype", "planettype");
	}

	@Test // DATACASS-406
	void createsCorrectTypeForSets() {

		List<Class<?>> ordered = new ArrayList<>(Arrays.asList(UniverseType.class, PlanetType.class, MoonType.class));

		Collections.shuffle(ordered); // catch ordering issues
		context = new CassandraMappingContext() {
			@Override
			public Collection<CassandraPersistentEntity<?>> getUserDefinedTypeEntities() {
				return ordered.stream().map(this::getRequiredPersistentEntity).collect(Collectors.toList());
			}
		};
		context.setUserTypeResolver(typeName -> {
			// make sure that calls to this method pop up. Calling UserTypeResolver while resolving
			// to be created user types isn't a good idea because they do not exist at resolution time.
			throw new IllegalArgumentException(String.format("Type %s not found", typeName));
		});

		CassandraPersistentEntitySchemaCreator schemaCreator = new CassandraPersistentEntitySchemaCreator(context,
				adminOperations);

		schemaCreator.createUserTypes(false);

		verify(operations).execute(matches("CREATE TYPE planettype .* set<.*moontype>.*"));

		verifyTypesGetCreatedInOrderFor("universetype", "moontype", "planettype");
	}

	@Test // DATACASS-406
	void createsCorrectTypeForLists() {

		context.getPersistentEntity(SpaceAgencyType.class);

		CassandraPersistentEntitySchemaCreator schemaCreator = new CassandraPersistentEntitySchemaCreator(context,
				adminOperations);

		schemaCreator.createUserTypes(false);

		verify(operations).execute(matches("CREATE TYPE spaceagencytype .* list<.*astronauttype>.*"));

		verifyTypesGetCreatedInOrderFor("astronauttype", "spaceagencytype");
	}

	@Test // DATACASS-406
	void createsCorrectTypesForNestedTypes() {

		context.getPersistentEntity(PlanetType.class);

		CassandraPersistentEntitySchemaCreator schemaCreator = new CassandraPersistentEntitySchemaCreator(context,
				adminOperations);

		schemaCreator.createUserTypes(false);

		verifyTypesGetCreatedInOrderFor("universetype", "moontype", "planettype");
	}

	@Test // DATACASS-213
	void createsIndexes() {

		context.getPersistentEntity(IndexedEntity.class);

		CassandraPersistentEntitySchemaCreator schemaCreator = new CassandraPersistentEntitySchemaCreator(context,
				adminOperations);

		schemaCreator.createIndexes(false);

		verify(operations).execute("CREATE INDEX ON indexedentity (firstname);");
	}

	private void verifyTypesGetCreatedInOrderFor(String... typenames) {

		ArgumentCaptor<String> cql = ArgumentCaptor.forClass(String.class);

		verify(operations, atLeast(typenames.length)).execute(cql.capture());

		List<String> allValues = cql.getAllValues();

		for (int i = 0; i < typenames.length; i++) {
			assertThat(allValues.get(i)).describedAs("Actual: " + allValues + ", expected: " + Arrays.toString(typenames))
					.contains("CREATE TYPE " + typenames[i]);
		}
	}

	abstract static class AbstractModel {
		private RequiredByAll attachments;
	}

	@UserDefinedType
	private static class RequiredByAll {
		private String name;
	}

	@UserDefinedType
	private static class Udt1 {

		private RequiredByAll attachment;
	}

	@UserDefinedType
	private static class Udt2 extends AbstractModel {

		private Udt1 u1;
	}
}
