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
package org.springframework.data.cassandra.mapping;

import static org.junit.Assert.*;

import java.io.Serializable;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;
import org.springframework.cassandra.core.Ordering;
import org.springframework.cassandra.core.PrimaryKeyType;
import org.springframework.data.annotation.Id;
import org.springframework.data.mapping.model.MappingException;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;

/**
 * Unit tests for {@link org.springframework.data.cassandra.mapping.BasicCassandraPersistentEntityMetadataVerifier}
 * through {@link CassandraMappingContext}
 *
 * @author David Webb
 * @author Mark Paluch
 */
public class BasicCassandraPersistentEntityMetadataVerifierUnitTests {

	private static LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
	private Logger logger = loggerContext.getLogger(BasicCassandraPersistentEntityMetadataVerifier.class);
	private CassandraMappingContext mappingContext;

	@Before
	public void setUp() {
		mappingContext = new BasicCassandraMappingContext();
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

	@Test(expected = MappingException.class)
	public void testNoPartitionKey() {
		mappingContext.getPersistentEntity(NoPartitionKey.class);
	}

	@Test(expected = MappingException.class)
	public void testPkAndPkc() {
		mappingContext.getPersistentEntity(PkAndPkc.class);
	}

	@Test
	public void testOnePkc() {

		CassandraPersistentEntity<?> entity = mappingContext.getPersistentEntity(OnePkc.class);

		assertNull(entity.getIdProperty());
	}

	@Test
	public void testMultiPkc() {

		CassandraPersistentEntity<?> entity = mappingContext.getPersistentEntity(MultiPkc.class);

		assertNull(entity.getIdProperty());
	}

	static class NonPersistentClass {

		@Id private String id;

		private String foo;
		private String bar;

	}

	@Table
	static class Person {

		@Id private String id;

		private String firstName;
		private String lastName;

	}

	@Table
	static class Animal {

		@PrimaryKey AnimalPK key;
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

		@PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED) String species;
		@PrimaryKeyColumn(ordinal = 1, type = PrimaryKeyType.PARTITIONED) String breed;
		@PrimaryKeyColumn(ordinal = 2, type = PrimaryKeyType.CLUSTERED, ordering = Ordering.DESCENDING) String color;

	}

	@PrimaryKeyClass
	static class AnimalPkNoOverrides {

		@PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED) String species;
		@PrimaryKeyColumn(ordinal = 1, type = PrimaryKeyType.PARTITIONED) String breed;
		@PrimaryKeyColumn(ordinal = 2, type = PrimaryKeyType.CLUSTERED, ordering = Ordering.DESCENDING) String color;
	}

	@Table
	@PrimaryKeyClass
	static class TooManyAnnotations {}

	@Table
	public static class NoPartitionKey {

		@PrimaryKeyColumn(ordinal = 0) String key;
	}

	@Table
	public static class PkAndPkc {

		@PrimaryKey String primaryKey;
		@PrimaryKeyColumn(ordinal = 0) String primaryKeyColumn;
	}

	@Table
	public static class OnePkc {

		@PrimaryKeyColumn(type = PrimaryKeyType.PARTITIONED, ordinal = 0) String pk;
	}

	@Table
	public static class MultiPkc {

		@PrimaryKeyColumn(type = PrimaryKeyType.PARTITIONED, ordinal = 0) String pk0;
		@PrimaryKeyColumn(ordinal = 1) String pk1;
	}
}
