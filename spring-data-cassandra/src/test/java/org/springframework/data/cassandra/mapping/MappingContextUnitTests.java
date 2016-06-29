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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.springframework.cassandra.core.PrimaryKeyType;
import org.springframework.cassandra.core.cql.CqlIdentifier;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.cassandra.convert.CustomConversions;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.util.ClassTypeInformation;

/**
 * Unit tests for {@link BasicCassandraMappingContext}.
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
public class MappingContextUnitTests {

	BasicCassandraMappingContext mappingContext = new BasicCassandraMappingContext();

	@Test(expected = MappingException.class)
	public void testGetPersistentEntityOfTransientType() {
		mappingContext.getPersistentEntity(Transient.class);
	}

	private static class Transient {}

	@Test
	public void testGetExistingPersistentEntityHappyPath() {

		mappingContext.getPersistentEntity(X.class);

		assertTrue(mappingContext.contains(X.class));
		assertNotNull(mappingContext.getExistingPersistentEntity(X.class));
		assertFalse(mappingContext.contains(Y.class));
	}

	@Table
	private static class X {
		@PrimaryKey String key;
	}

	@Table
	private static class Y {
		@PrimaryKey String key;
	}

	/**
	 * @see DATACASS-248
	 */
	@Test
	public void primaryKeyOnPropertyShouldWork() {

		CassandraPersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(PrimaryKeyOnProperty.class);

		CassandraPersistentProperty idProperty = persistentEntity.getIdProperty();

		assertThat(idProperty.getColumnName().toCql(), is(equalTo("foo")));

		List<CqlIdentifier> columnNames = idProperty.getColumnNames();

		assertThat(columnNames, hasSize(1));
		assertThat(columnNames.get(0).toCql(), is(equalTo("foo")));
	}

	@Table
	private static class PrimaryKeyOnProperty {

		String key;

		@PrimaryKey(value = "foo")
		public String getKey() {
			return key;
		}

		public void setKey(String key) {
			this.key = key;
		}
	}

	/**
	 * @see DATACASS-248
	 */
	@Test
	public void primaryKeyColumnsOnPropertyShouldWork() {

		CassandraPersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(PrimaryKeyColumnsOnProperty.class);

		assertThat(persistentEntity.isCompositePrimaryKey(), is(false));

		CassandraPersistentProperty firstname = persistentEntity.getPersistentProperty("firstname");

		assertThat(firstname.isCompositePrimaryKey(), is(false));
		assertThat(firstname.isPrimaryKeyColumn(), is(true));
		assertThat(firstname.isPartitionKeyColumn(), is(true));
		assertThat(firstname.getColumnName().toCql(), is(equalTo("firstname")));

		CassandraPersistentProperty lastname = persistentEntity.getPersistentProperty("lastname");

		assertThat(lastname.isPrimaryKeyColumn(), is(true));
		assertThat(lastname.isClusterKeyColumn(), is(true));
		assertThat(lastname.getColumnName().toCql(), is(equalTo("mylastname")));
	}

	@Table
	private static class PrimaryKeyColumnsOnProperty {

		String firstname;
		String lastname;

		@PrimaryKeyColumn(ordinal = 1, type = PrimaryKeyType.PARTITIONED)
		public String getFirstname() {
			return firstname;
		}

		public void setFirstname(String firstname) {
			this.firstname = firstname;
		}

		@PrimaryKeyColumn(name = "mylastname", ordinal = 2, type = PrimaryKeyType.CLUSTERED)
		public String getLastname() {
			return lastname;
		}

		public void setLastname(String lastname) {
			this.lastname = lastname;
		}
	}

	/**
	 * @see DATACASS-248
	 */
	@Test
	public void primaryKeyClassWithprimaryKeyColumnsOnPropertyShouldWork() {

		CassandraPersistentEntity<?> persistentEntity =
				mappingContext.getPersistentEntity(PrimaryKeyOnPropertyWithPrimaryKeyClass.class);

		CassandraPersistentEntity<?> primaryKeyClass =
				mappingContext.getPersistentEntity(CompositePrimaryKeyClassWithProperties.class);

		assertThat(persistentEntity.isCompositePrimaryKey(), is(false));
		assertThat(persistentEntity.getPersistentProperty("key").isCompositePrimaryKey(), is(true));

		assertThat(primaryKeyClass.isCompositePrimaryKey(), is(true));
		assertThat(primaryKeyClass.getCompositePrimaryKeyProperties(), hasSize(2));

		CassandraPersistentProperty firstname = primaryKeyClass.getPersistentProperty("firstname");

		assertThat(firstname.isPrimaryKeyColumn(), is(true));
		assertThat(firstname.isPartitionKeyColumn(), is(true));
		assertThat(firstname.isClusterKeyColumn(), is(false));
		assertThat(firstname.getColumnName().toCql(), is(equalTo("firstname")));

		CassandraPersistentProperty lastname = primaryKeyClass.getPersistentProperty("lastname");

		assertThat(lastname.isPrimaryKeyColumn(), is(true));
		assertThat(lastname.isPartitionKeyColumn(), is(false));
		assertThat(lastname.isClusterKeyColumn(), is(true));
		assertThat(lastname.getColumnName().toCql(), is(equalTo("mylastname")));
	}

	@Table
	private static class PrimaryKeyOnPropertyWithPrimaryKeyClass {

		CompositePrimaryKeyClassWithProperties key;

		@PrimaryKey
		public CompositePrimaryKeyClassWithProperties getKey() {
			return key;
		}

		public void setKey(CompositePrimaryKeyClassWithProperties key) {
			this.key = key;
		}
	}

	@PrimaryKeyClass
	private static class CompositePrimaryKeyClassWithProperties implements Serializable {

		String firstname;
		String lastname;

		@PrimaryKeyColumn(ordinal = 1, type = PrimaryKeyType.PARTITIONED)
		public String getFirstname() {
			return firstname;
		}

		public void setFirstname(String firstname) {
			this.firstname = firstname;
		}

		@PrimaryKeyColumn(name = "mylastname", ordinal = 2, type = PrimaryKeyType.CLUSTERED)
		public String getLastname() {
			return lastname;
		}

		public void setLastname(String lastname) {
			this.lastname = lastname;
		}
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void shouldCreatePersistentEntityIfNoConversionRegistered() {

		mappingContext.setCustomConversions(new CustomConversions(Collections.EMPTY_LIST));
		assertThat(mappingContext.shouldCreatePersistentEntityFor(ClassTypeInformation.from(Human.class)), is(true));
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void shouldNotCreateEntitiesForCustomConvertedTypes() {

		List<?> converters = Arrays.asList(new HumanToStringConverter());
		mappingContext.setCustomConversions(new CustomConversions(converters));

		assertThat(mappingContext.shouldCreatePersistentEntityFor(ClassTypeInformation.from(Human.class)), is(false));
	}

	private static class Human {
	}

	private static class HumanToStringConverter implements Converter<Human, String> {

		@Override
		public String convert(Human source) {
			return "hello";
		}
	}
}
