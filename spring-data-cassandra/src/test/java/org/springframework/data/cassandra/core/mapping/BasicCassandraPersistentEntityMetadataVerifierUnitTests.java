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
package org.springframework.data.cassandra.core.mapping;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.cql.Ordering;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.mapping.MappingException;

/**
 * Unit tests for {@link org.springframework.data.cassandra.core.mapping.BasicCassandraPersistentEntityMetadataVerifier}
 * through {@link CassandraMappingContext}
 *
 * @author David Webb
 * @author Mark Paluch
 */
class BasicCassandraPersistentEntityMetadataVerifierUnitTests {

	private BasicCassandraPersistentEntityMetadataVerifier verifier = new BasicCassandraPersistentEntityMetadataVerifier();

	private CassandraMappingContext context = new CassandraMappingContext();

	@BeforeEach
	void setUp() throws Exception {
		context.setVerifier(new NoOpVerifier());
	}

	@Test // DATACASS-258
	void shouldAllowInterfaceTypes() {
		verifier.verify(context.getRequiredPersistentEntity(MyInterface.class));
	}

	@Test // DATACASS-258
	void testPrimaryKeyClass() {
		verifier.verify(context.getRequiredPersistentEntity(Animal.class));
	}

	@Test // DATACASS-258
	void testNonPrimaryKeyClass() {
		verifier.verify(context.getRequiredPersistentEntity(Person.class));
	}

	@Test // DATACASS-258
	void testNonPersistentType() {
		verifier.verify(context.getRequiredPersistentEntity(NonPersistentClass.class));
	}

	@Test // DATACASS-258
	void shouldFailWithPersistentAndPrimaryKeyClassAnnotations() {

		try {
			verifier.verify(context.getRequiredPersistentEntity(TooManyAnnotations.class));
			fail("Missing MappingException");
		} catch (MappingException e) {
			assertThat(e).hasMessageContaining("Entity cannot be of type @Table and @PrimaryKeyClass");
		}
	}

	@Test // DATACASS-258
	void shouldFailWithoutPartitionKey() {

		try {
			verifier.verify(context.getRequiredPersistentEntity(NoPartitionKey.class));
			fail("Missing MappingException");
		} catch (MappingException e) {
			assertThat(e)
					.hasMessageContaining("At least one of the @PrimaryKeyColumn annotations must have a type of PARTITIONED");
		}
	}

	@Test // DATACASS-258
	void shouldFailWithoutPrimaryKey() {

		try {
			verifier.verify(context.getRequiredPersistentEntity(NoPrimaryKey.class));
			fail("Missing MappingException");
		} catch (MappingException e) {
			assertThat(e).hasMessageContaining("@Table types must have only one primary attribute, if any; Found 0");
		}
	}

	@Test // DATACASS-258
	void testPkAndPkc() {

		try {
			verifier.verify(context.getRequiredPersistentEntity(PrimaryKeyAndPrimaryKeyColumn.class));
			fail("Missing MappingException");
		} catch (MappingException e) {
			assertThat(e).hasMessageContaining("@Table types must not define both @Id and @PrimaryKeyColumn properties");
		}
	}

	@Test // DATACASS-213
	void shouldFailOnIndexedEntity() {

		try {
			verifier.verify(context.getRequiredPersistentEntity(InvalidIndexedPerson.class));
			fail("Missing MappingException");
		} catch (MappingException e) {
			assertThat(e).hasMessageContaining("@Indexed cannot be used on entity classes");
		}
	}

	private interface MyInterface {}

	private static class NonPersistentClass {

		@Id String id;

		String foo;
		String bar;
	}

	@Table
	static class Person {

		@Id String id;

		String firstName;
		String lastName;
	}

	@Table
	@Indexed
	private static class InvalidIndexedPerson {

		@Id String id;

		String firstName;
	}

	@Table
	private static class Animal {

		@PrimaryKey AnimalPK key;
		String name;
	}

	@PrimaryKeyClass
	static class AnimalPK {

		@PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED) String species;
		@PrimaryKeyColumn(ordinal = 1, type = PrimaryKeyType.PARTITIONED) String breed;
		@PrimaryKeyColumn(ordinal = 2, type = PrimaryKeyType.CLUSTERED, ordering = Ordering.DESCENDING) String color;
	}

	@Table
	private static class EntityWithComplexTypePrimaryKey {

		@PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED) Object species;
	}

	@Table
	@PrimaryKeyClass
	private static class TooManyAnnotations {}

	@Table
	private static class NoPartitionKey {

		@PrimaryKeyColumn(ordinal = 0) String key;
	}

	@Table
	private static class NoPrimaryKey {}

	@Table
	private static class PrimaryKeyAndPrimaryKeyColumn {

		@PrimaryKey String primaryKey;
		@PrimaryKeyColumn(ordinal = 0) String primaryKeyColumn;
	}

	@Table
	private static class OnePrimaryKeyColumn {

		@PrimaryKeyColumn(type = PrimaryKeyType.PARTITIONED, ordinal = 0) String pk;
	}

	@Table
	private static class MultiplePrimaryKeyColumns {

		@PrimaryKeyColumn(type = PrimaryKeyType.PARTITIONED, ordinal = 0) String pk0;
		@PrimaryKeyColumn(ordinal = 1) String pk1;
	}

	private static class NoOpVerifier implements CassandraPersistentEntityMetadataVerifier {

		@Override
		public void verify(CassandraPersistentEntity<?> entity) throws MappingException {}
	}
}
