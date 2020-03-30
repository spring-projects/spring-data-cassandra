/*
 * Copyright 2016-2020 the original author or authors.
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
import static org.mockito.Mockito.*;
import static org.springframework.data.cassandra.core.mapping.CassandraType.*;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.junit.Before;
import org.junit.Test;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.convert.CassandraCustomConversions;
import org.springframework.data.cassandra.core.cql.Ordering;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.cql.keyspace.CreateIndexSpecification;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.util.ClassTypeInformation;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.UserDefinedType;

/**
 * Unit tests for {@link CassandraMappingContext}.
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
public class CassandraMappingContextUnitTests {

	CassandraMappingContext mappingContext = new CassandraMappingContext();

	@Before
	public void before() {
		this.mappingContext.setUserTypeResolver(typeName -> null);
	}

	@Test
	public void testGetRequiredPersistentEntityOfTransientType() {
		this.mappingContext.getRequiredPersistentEntity(Transient.class);
	}

	private static class Transient {}

	@Test // DATACASS-282, DATACASS-455
	public void testGetExistingPersistentEntityHappyPath() {

		TableMetadata tableMetadata = mock(TableMetadata.class);

		when(tableMetadata.getName()).thenReturn(CqlIdentifier.fromCql(X.class.getSimpleName().toLowerCase()));

		mappingContext.getRequiredPersistentEntity(X.class);

		assertThat(mappingContext.getUserDefinedTypeEntities()).isEmpty();
		assertThat(mappingContext.getTableEntities()).hasSize(1);
		assertThat(mappingContext.getPersistentEntities()).hasSize(1);
		assertThat(mappingContext.usesTable(tableMetadata.getName())).isTrue();
	}

	@Test // DATACASS-248
	public void primaryKeyOnPropertyShouldWork() {

		CassandraPersistentEntity<?> persistentEntity = mappingContext
				.getRequiredPersistentEntity(PrimaryKeyOnProperty.class);

		CassandraPersistentProperty idProperty = persistentEntity.getIdProperty();

		assertThat(idProperty).satisfies(actual -> {

			assertThat(actual.getColumnName()).hasToString("foo");
		});
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

	@Test // DATACASS-248
	public void primaryKeyColumnsOnPropertyShouldWork() {

		CassandraPersistentEntity<?> persistentEntity = mappingContext
				.getRequiredPersistentEntity(PrimaryKeyColumnsOnProperty.class);

		assertThat(persistentEntity.isCompositePrimaryKey()).isFalse();

		CassandraPersistentProperty firstname = persistentEntity.getRequiredPersistentProperty("firstname");

		assertThat(firstname.isCompositePrimaryKey()).isFalse();
		assertThat(firstname.isPrimaryKeyColumn()).isTrue();
		assertThat(firstname.isPartitionKeyColumn()).isTrue();
		assertThat(firstname.getColumnName().toString()).isEqualTo("firstname");

		CassandraPersistentProperty lastname = persistentEntity.getRequiredPersistentProperty("lastname");

		assertThat(lastname.isPrimaryKeyColumn()).isTrue();
		assertThat(lastname.isClusterKeyColumn()).isTrue();
		assertThat(lastname.getColumnName().toString()).isEqualTo("mylastname");
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

	@Test // DATACASS-248
	public void primaryKeyClassWithPrimaryKeyColumnsOnPropertyShouldWork() {

		CassandraPersistentEntity<?> persistentEntity = mappingContext
				.getRequiredPersistentEntity(PrimaryKeyOnPropertyWithPrimaryKeyClass.class);

		CassandraPersistentEntity<?> primaryKeyClass = mappingContext
				.getRequiredPersistentEntity(CompositePrimaryKeyClassWithProperties.class);

		assertThat(persistentEntity.isCompositePrimaryKey()).isFalse();
		assertThat(persistentEntity.getPersistentProperty("key").isCompositePrimaryKey()).isTrue();

		assertThat(primaryKeyClass.isCompositePrimaryKey()).isTrue();

		CassandraPersistentProperty firstname = primaryKeyClass.getRequiredPersistentProperty("firstname");

		assertThat(firstname.isPrimaryKeyColumn()).isTrue();
		assertThat(firstname.isPartitionKeyColumn()).isTrue();
		assertThat(firstname.isClusterKeyColumn()).isFalse();
		assertThat(firstname.getColumnName().toString()).isEqualTo("firstname");

		CassandraPersistentProperty lastname = primaryKeyClass.getRequiredPersistentProperty("lastname");

		assertThat(lastname.isPrimaryKeyColumn()).isTrue();
		assertThat(lastname.isPartitionKeyColumn()).isFalse();
		assertThat(lastname.isClusterKeyColumn()).isTrue();
		assertThat(lastname.getColumnName().toString()).isEqualTo("mylastname");
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

	@Table
	static class EntityWithOrderedClusteredColumns {

		@PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED) String species;
		@PrimaryKeyColumn(ordinal = 1, type = PrimaryKeyType.CLUSTERED, ordering = Ordering.ASCENDING) String breed;
		@PrimaryKeyColumn(ordinal = 2, type = PrimaryKeyType.CLUSTERED, ordering = Ordering.DESCENDING) String color;
		@PrimaryKeyColumn(ordinal = 3, type = PrimaryKeyType.CLUSTERED) String kind;
	}

	@PrimaryKeyClass
	static class PrimaryKeyWithOrderedClusteredColumns implements Serializable {

		@PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED) String species;
		@PrimaryKeyColumn(ordinal = 1, type = PrimaryKeyType.CLUSTERED, ordering = Ordering.ASCENDING) String breed;
		@PrimaryKeyColumn(ordinal = 2, type = PrimaryKeyType.CLUSTERED, ordering = Ordering.DESCENDING) String color;
		@PrimaryKeyColumn(ordinal = 3, type = PrimaryKeyType.CLUSTERED) String kind;
	}

	@Table
	private static class EntityWithPrimaryKeyWithOrderedClusteredColumns {

		@PrimaryKey PrimaryKeyWithOrderedClusteredColumns key;
	}

	private static CreateIndexSpecification getSpecificationFor(String column,
			List<CreateIndexSpecification> specifications) {

		return specifications.stream().filter(it -> it.getColumnName().equals(CqlIdentifier.fromCql(column))).findFirst()
				.orElseThrow(() -> new NoSuchElementException(column));
	}

	static class IndexedType {

		@PrimaryKeyColumn("first_name") @Indexed("my_index") String firstname;

		@Indexed List<String> phoneNumbers;
	}

	@PrimaryKeyClass
	static class CompositeKeyWithIndex {

		@PrimaryKeyColumn(value = "first_name", type = PrimaryKeyType.PARTITIONED) String firstname;
		@PrimaryKeyColumn("last_name") @Indexed("my_index") String lastname;
	}

	static class CompositeKeyEntity {

		@PrimaryKey CompositeKeyWithIndex key;
	}

	static class InvalidMapIndex {

		@Indexed Map<@Indexed String, String> mixed;
	}

	@Test // DATACASS-296
	public void shouldCreatePersistentEntityIfNoConversionRegistered() {

		mappingContext.setCustomConversions(new CassandraCustomConversions(Collections.emptyList()));
		assertThat(mappingContext.shouldCreatePersistentEntityFor(ClassTypeInformation.from(Human.class))).isTrue();
	}

	@Test // DATACASS-296
	public void shouldNotCreateEntitiesForCustomConvertedTypes() {

		mappingContext.setCustomConversions(
				new CassandraCustomConversions(Collections.singletonList(HumanToStringConverter.INSTANCE)));

		assertThat(mappingContext.shouldCreatePersistentEntityFor(ClassTypeInformation.from(Human.class))).isFalse();
	}

	@Test // DATACASS-172, DATACASS-455
	public void shouldRegisterUdtTypes() {

		CassandraPersistentEntity<?> persistentEntity = mappingContext.getRequiredPersistentEntity(MappedUdt.class);

		assertThat(persistentEntity.isUserDefinedType()).isTrue();

		assertThat(mappingContext.getUserDefinedTypeEntities()).hasSize(1);
		assertThat(mappingContext.getPersistentEntities()).hasSize(1);
		assertThat(mappingContext.getTableEntities()).hasSize(0);
	}

	@Test // DATACASS-523
	public void shouldCreateMappedTupleType() {

		CassandraPersistentEntity<?> persistentEntity = this.mappingContext.getRequiredPersistentEntity(MappedTuple.class);

		assertThat(persistentEntity).isInstanceOf(BasicCassandraPersistentTupleEntity.class);

		assertThat(this.mappingContext.getUserDefinedTypeEntities()).isEmpty();
		assertThat(this.mappingContext.getPersistentEntities()).hasSize(1);
		assertThat(this.mappingContext.getTableEntities()).isEmpty();
	}

	@Test // DATACASS-172
	public void getNonPrimaryKeyEntitiesShouldNotContainUdt() {

		BasicCassandraPersistentEntity<?> existingPersistentEntity = mappingContext
				.getRequiredPersistentEntity(MappedUdt.class);

		assertThat(mappingContext.getTableEntities()).doesNotContain(existingPersistentEntity);
	}

	@Test // DATACASS-172, DATACASS-359
	public void getPersistentEntitiesShouldContainUdt() {

		BasicCassandraPersistentEntity<?> existingPersistentEntity = mappingContext
				.getRequiredPersistentEntity(MappedUdt.class);

		assertThat(mappingContext.getPersistentEntities()).contains(existingPersistentEntity);
		assertThat(mappingContext.getUserDefinedTypeEntities()).contains(existingPersistentEntity);
		assertThat(mappingContext.getTableEntities()).doesNotContain(existingPersistentEntity);
	}

	@Test // DATACASS-172, DATACASS-455
	public void usesTypeShouldNotReportTypeUsage() {
		assertThat(mappingContext.usesUserType(CqlIdentifier.fromCql("mappedudt"))).isFalse();
	}

	@Test // DATACASS-172, DATACASS-455
	public void usesTypeShouldReportTypeUsageInMappedUdt() {

		UserDefinedType myTypeMock = mock(UserDefinedType.class, "mappedudt");

		mappingContext.setUserTypeResolver(typeName -> myTypeMock);

		mappingContext.getRequiredPersistentEntity(WithUdt.class);

		assertThat(mappingContext.usesUserType(CqlIdentifier.fromCql("mappedudt"))).isTrue();
	}

	@Test // DATACASS-172, DATACASS-455
	public void usesTypeShouldReportTypeUsageInColumn() {

		UserDefinedType myTypeMock = mock(UserDefinedType.class, "mappedudt");

		mappingContext.setUserTypeResolver(typeName -> myTypeMock);

		mappingContext.getRequiredPersistentEntity(MappedUdt.class);

		assertThat(mappingContext.usesUserType(CqlIdentifier.fromCql("mappedudt"))).isTrue();
	}

	@Test // DATACASS-282, DATACASS-455
	public void shouldNotRetainInvalidEntitiesInCache() {

		TableMetadata tableMetadata = mock(TableMetadata.class);

		when(tableMetadata.getName())
				.thenReturn(CqlIdentifier.fromCql(InvalidEntityWithIdAndPrimaryKeyColumn.class.getSimpleName().toLowerCase()));

		try {
			mappingContext.getPersistentEntity(InvalidEntityWithIdAndPrimaryKeyColumn.class);
			fail("Missing MappingException");
		} catch (MappingException e) {
			assertThat(e).isInstanceOf(VerifierMappingExceptions.class);
		}

		assertThat(mappingContext.getUserDefinedTypeEntities()).isEmpty();
		assertThat(mappingContext.getTableEntities()).isEmpty();
		assertThat(mappingContext.getPersistentEntities()).isEmpty();
		assertThat(mappingContext.usesTable(tableMetadata.getName())).isFalse();
	}

	@Test // DATACASS-747
	public void shouldQuoteFieldName() {

		BasicCassandraPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(UnsupportedFieldNames.class);

		CassandraPersistentProperty property = entity.getRequiredPersistentProperty("_ent1");
		assertThat(property.getColumnName()).isEqualTo(CqlIdentifier.fromCql("\"_ent1\""));
	}

	@Table
	private static class InvalidEntityWithIdAndPrimaryKeyColumn {
		@Id String foo;
		@PrimaryKeyColumn String bar;
	}

	@PrimaryKeyClass
	static class PrimaryKeyClassWithComplexId {
		@PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED) Object complexObject;
	}

	private static class Human {}

	@Table
	private static class X {
		@PrimaryKey String key;
	}

	@Table
	private static class Y {
		@PrimaryKey String key;
	}

	@Tuple
	private static class MappedTuple {
		@Element(0) String name;
	}

	@org.springframework.data.cassandra.core.mapping.UserDefinedType
	private static class MappedUdt {}

	@Table
	private static class WithUdt {
		@Id String id;
		@CassandraType(type = Name.UDT, userTypeName = "mappedudt") UdtValue udtValue;
		@CassandraType(type = Name.UDT, userTypeName = "NestedType") Nested nested;
	}

	enum HumanToStringConverter implements Converter<Human, String> {

		INSTANCE;

		@Override
		public String convert(Human source) {
			return "hello";
		}
	}

	@org.springframework.data.cassandra.core.mapping.UserDefinedType(value = "NestedType")
	public static class Nested {
		String s1;
		@CassandraType(type = Name.UDT, userTypeName = "AnotherNestedType") AnotherNested anotherNested;
	}

	@org.springframework.data.cassandra.core.mapping.UserDefinedType(value = "AnotherNestedType")
	public static class AnotherNested {
		String str;
	}

	class UnsupportedFieldNames {
		String _ent1;
		String _ent2;
	}
}
