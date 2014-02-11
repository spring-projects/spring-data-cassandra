/*
 * Copyright 2011-2014 the original author or authors.
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
package org.springframework.data.cassandra.test.integration.mapping;

import java.io.Serializable;

import org.junit.Before;
import org.junit.Test;
import org.springframework.cassandra.core.Ordering;
import org.springframework.cassandra.core.PrimaryKeyType;
import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.mapping.DefaultCassandraMappingContext;
import org.springframework.data.cassandra.mapping.PrimaryKey;
import org.springframework.data.cassandra.mapping.PrimaryKeyClass;
import org.springframework.data.cassandra.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.mapping.Table;
import org.springframework.data.mapping.model.MappingException;

/**
 * @author dwebb
 * 
 */
public class BasicCassandraPersistentEntityVerifierIntegrationTest {

	CassandraMappingContext mappingContext;

	@Before
	public void init() {

		mappingContext = new DefaultCassandraMappingContext();

	}

	@Test(expected = MappingException.class)
	public void testNonPersistentType() {

		mappingContext.getPersistentEntity(NonPersistentClass.class);

	}

	@Test(expected = MappingException.class)
	public void testTooManyAnnotations() {

		mappingContext.getPersistentEntity(TooManyAnnotations.class);

	}

	@Test
	public void testNonPrimaryKeyClass() {

		mappingContext.getPersistentEntity(Person.class);

	}

	@Test(expected = MappingException.class)
	public void testPrimaryKeyClassNotFullyImplemented() {

		mappingContext.getPersistentEntity(AnimalPkNoOverrides.class);

	}

	@Test
	public void testPrimaryKeyClass() {

		mappingContext.getPersistentEntity(AnimalPK.class);

		mappingContext.getPersistentEntity(Animal.class);

	}

	static class NonPersistentClass {

		@Id
		private String id;

		private String foo;
		private String bar;

	}

	@Table
	static class Person {

		@Id
		private String id;

		private String firstName;
		private String lastName;

	}

	@Table
	static class Animal {

		@PrimaryKey
		private AnimalPK key;

		private String name;
	}

	@PrimaryKeyClass
	static class AnimalPK implements Serializable {

		@Override
		public int hashCode() {
			return super.hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			return super.equals(obj);
		}

		@PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED)
		private String species;

		@PrimaryKeyColumn(ordinal = 1, type = PrimaryKeyType.PARTITIONED)
		private String breed;

		@PrimaryKeyColumn(ordinal = 2, type = PrimaryKeyType.CLUSTERED, ordering = Ordering.DESCENDING)
		private String color;

	}

	@PrimaryKeyClass
	static class AnimalPkNoOverrides {

		@PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED)
		private String species;

		@PrimaryKeyColumn(ordinal = 1, type = PrimaryKeyType.PARTITIONED)
		private String breed;

		@PrimaryKeyColumn(ordinal = 2, type = PrimaryKeyType.CLUSTERED, ordering = Ordering.DESCENDING)
		private String color;

	}

	@Table
	@PrimaryKeyClass
	static class TooManyAnnotations {

	}

}
