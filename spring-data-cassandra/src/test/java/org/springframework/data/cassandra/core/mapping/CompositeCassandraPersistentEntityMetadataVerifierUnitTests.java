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
 * Unit tests for {@link CompositeCassandraPersistentEntityMetadataVerifier}.
 *
 * @author Mark Paluch
 */
public class CompositeCassandraPersistentEntityMetadataVerifierUnitTests {

	private CompositeCassandraPersistentEntityMetadataVerifier verifier = new CompositeCassandraPersistentEntityMetadataVerifier();
	private CassandraMappingContext context = new CassandraMappingContext();

	@Before
	public void setUp() throws Exception {
		context.setVerifier(verifier);
	}

	@Test // DATACASS-258
	public void shouldAllowInterfaceTypes() {
		verifier.verify(context.getRequiredPersistentEntity(MyInterface.class));
	}

	@Test // DATACASS-258
	public void testPrimaryKeyClass() {
		verifier.verify(context.getRequiredPersistentEntity(Animal.class));
	}

	@Test // DATACASS-258
	public void testNonPrimaryKeyClass() {
		verifier.verify(context.getRequiredPersistentEntity(Person.class));
	}

	@Test // DATACASS-258, DATACASS-359
	public void shouldNotFailWithNonPersistentClasses() {
		verifier.verify(context.getRequiredPersistentEntity(NonPersistentClass.class));
	}

	@Test // DATACASS-258
	public void shouldFailWithPersistentAndPrimaryKeyClassAnnotations() {

		try {
			verifier.verify(context.getRequiredPersistentEntity(TooManyAnnotations.class));
			fail("Missing MappingException");
		} catch (MappingException e) {
			assertThat(e).hasMessageContaining("Entity cannot be of type @Table and @PrimaryKeyClass");
		}
	}

	interface MyInterface {}

	static class NonPersistentClass {

		@Id String id;

		String foo;
		String bar;
	}

	@Table
	@PrimaryKeyClass
	static class TooManyAnnotations {}

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
}
