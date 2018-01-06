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
package org.springframework.data.cassandra.core.mapping;

import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.Fail.fail;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.cql.Ordering;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.mapping.MappingException;

/**
 * Unit tests for {@link PrimaryKeyClassEntityMetadataVerifier}.
 *
 * @author Mark Paluch
 */
public class PrimaryKeyClassEntityMetadataVerifierUnitTests {

	private PrimaryKeyClassEntityMetadataVerifier verifier = new PrimaryKeyClassEntityMetadataVerifier();
	private CassandraMappingContext context = new CassandraMappingContext();

	@Before
	public void setUp() throws Exception {
		context.setVerifier(new NoOpVerifier());
	}

	@Test // DATACASS-258
	public void shouldAllowNonPersistentClasses() {
		verifier.verify(getEntity(NonPersistentClass.class));
	}

	@Test // DATACASS-258
	public void shouldAllowInterfaceTypes() {
		verifier.verify(getEntity(MyInterface.class));
	}

	@Test // DATACASS-258
	public void shouldAllowTableClass() {
		verifier.verify(getEntity(Person.class));
	}

	@Test // DATACASS-258
	public void shouldVerifyPrimaryKeyClass() {
		verifier.verify(getEntity(AnimalPK.class));
	}

	@Test // DATACASS-258
	public void shouldFailWithPersistentAndPrimaryKeyClassAnnotations() {

		try {
			verifier.verify(getEntity(TooManyAnnotations.class));
			fail("Missing MappingException");
		} catch (MappingException e) {
			assertThat(e).hasMessageContaining("Entity cannot be of type @Table and @PrimaryKeyClass");
		}
	}

	@Test // DATACASS-258
	public void shouldFailWithoutPartitionKey() {

		try {
			verifier.verify(getEntity(NoPartitionKey.class));
			fail("Missing MappingException");
		} catch (MappingException e) {
			assertThat(e)
					.hasMessageContaining("At least one of the @PrimaryKeyColumn annotations must have a type of PARTITIONED");
		}
	}

	@Test // DATACASS-258
	public void shouldFailWithoutPrimaryKey() {

		try {
			verifier.verify(getEntity(NoPrimaryKey.class));
			fail("Missing MappingException");
		} catch (MappingException e) {
			assertThat(e)
					.hasMessageContaining("At least one of the @PrimaryKeyColumn annotations must have a type of PARTITIONED");
		}
	}

	@Test // DATACASS-258
	public void shouldFailOnPrimaryKeyCycles() {

		try {
			verifier.verify(getEntity(TypeCycle.class));
			fail("Missing MappingException");
		} catch (MappingException e) {
			assertThat(e)
					.hasMessageContaining("Composite primary keys are not allowed inside of composite primary key classes");
		}
	}

	@Test // DATACASS-258
	public void shouldFailWithNestedPrimaryKeyClassReference() {

		try {
			verifier.verify(getEntity(PKClassWithNestedCompositeKey.class));
			fail("Missing MappingException");
		} catch (MappingException e) {
			assertThat(e)
					.hasMessageContaining("Composite primary keys are not allowed inside of composite primary key classes");
		}
	}

	@Test // DATACASS-258
	public void shouldFailWithPrimaryKeyClassAndPrimaryKeyAnnotations() {

		try {
			verifier.verify(getEntity(PrimaryKeyAndPrimaryKeyColumn.class));
			fail("Missing MappingException");
		} catch (MappingException e) {
			assertThat(e)
					.hasMessageContaining("Annotations @Id and @PrimaryKey are invalid for type annotated with @PrimaryKeyClass");
		}
	}

	@Test // DATACASS-258
	public void shouldFailForPrimaryKeyDerivedFromOtherThanObject() {

		try {
			verifier.verify(getEntity(SubclassPK.class));
			fail("Missing MappingException");
		} catch (MappingException e) {
			assertThat(e).hasMessageContaining("@PrimaryKeyClass must only extend Object");
		}
	}

	@Test // DATACASS-213
	public void shouldFailForIndexedPrimaryKey() {

		try {
			verifier.verify(getEntity(InvalidIndexedPrimaryKeyType.class));
			fail("Missing MappingException");
		} catch (MappingException e) {
			assertThat(e).hasMessageContaining("@Indexed cannot be used on primary key classes");
		}
	}

	private CassandraPersistentEntity<?> getEntity(Class<?> entityClass) {
		return context.getRequiredPersistentEntity(entityClass);
	}

	interface MyInterface {

	}

	static class NonPersistentClass {

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
	static class Animal {

		@PrimaryKey AnimalPK key;
		private String name;
	}

	@PrimaryKeyClass
	static class AnimalPK {

		@PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED) String species;
		@PrimaryKeyColumn(ordinal = 1, type = PrimaryKeyType.PARTITIONED) String breed;
		@PrimaryKeyColumn(ordinal = 2, type = PrimaryKeyType.CLUSTERED, ordering = Ordering.DESCENDING) String color;
	}

	@Table
	@PrimaryKeyClass
	static class TooManyAnnotations {

	}

	@PrimaryKeyClass
	static class NoPartitionKey {

		@PrimaryKeyColumn(ordinal = 0) String key;
	}

	@PrimaryKeyClass
	static class NoPrimaryKey {

	}

	@PrimaryKeyClass
	static class TypeCycle {

		@PrimaryKey TypeCycle typeCycle;
	}

	@PrimaryKeyClass
	static class PKClassWithNestedCompositeKey {

		@PrimaryKey OnePrimaryKeyColumn pkc;
	}

	@PrimaryKeyClass
	static class PrimaryKeyAndPrimaryKeyColumn {

		@PrimaryKey String primaryKey;
		@PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED) String primaryKeyColumn;
	}

	@PrimaryKeyClass
	static class OnePrimaryKeyColumn {

		@PrimaryKeyColumn(type = PrimaryKeyType.PARTITIONED, ordinal = 0) String pk;
	}

	static class MultiPrimaryKeyColumns {

		@PrimaryKeyColumn(type = PrimaryKeyType.PARTITIONED, ordinal = 0) String pk0;
		@PrimaryKeyColumn(ordinal = 1) String pk1;
	}

	@PrimaryKeyClass
	static class SubclassPK extends MultiPrimaryKeyColumns {

		@PrimaryKeyColumn(type = PrimaryKeyType.PARTITIONED, ordinal = 0) String pk;
	}

	@PrimaryKeyClass
	@Indexed
	static class InvalidIndexedPrimaryKeyType {

		@PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED) String species;
	}

	private static class NoOpVerifier implements CassandraPersistentEntityMetadataVerifier {

		@Override
		public void verify(CassandraPersistentEntity<?> entity) throws MappingException {}
	}
}
