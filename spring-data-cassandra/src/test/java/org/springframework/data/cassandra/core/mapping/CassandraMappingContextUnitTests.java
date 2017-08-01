/*
 * Copyright 2016-2017 the original author or authors.
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
import static org.mockito.Mockito.*;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.junit.Before;
import org.junit.Test;
import org.springframework.core.convert.converter.Converter;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.convert.CassandraCustomConversions;
import org.springframework.data.cassandra.core.cql.CqlIdentifier;
import org.springframework.data.cassandra.core.cql.Ordering;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.cql.keyspace.ColumnSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.CreateIndexSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.CreateIndexSpecification.ColumnFunction;
import org.springframework.data.cassandra.core.cql.keyspace.CreateTableSpecification;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.util.ClassTypeInformation;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.DataType.Name;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;

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
		mappingContext.setUserTypeResolver(typeName -> null);
	}

	@Test
	public void testgetRequiredPersistentEntityOfTransientType() {
		mappingContext.getRequiredPersistentEntity(Transient.class);
	}

	private static class Transient {}

	@Test // DATACASS-282, DATACASS-455
	public void testGetExistingPersistentEntityHappyPath() {

		TableMetadata tableMetadata = mock(TableMetadata.class);
		when(tableMetadata.getName()).thenReturn(X.class.getSimpleName().toLowerCase());

		mappingContext.getRequiredPersistentEntity(X.class);

		assertThat(mappingContext.getUserDefinedTypeEntities()).isEmpty();
		assertThat(mappingContext.getTableEntities()).hasSize(1);
		assertThat(mappingContext.getPersistentEntities()).hasSize(1);
		assertThat(mappingContext.usesTable(CqlIdentifier.of(tableMetadata.getName()))).isTrue();
	}

	@Test // DATACASS-248
	public void primaryKeyOnPropertyShouldWork() {

		CassandraPersistentEntity<?> persistentEntity = mappingContext
				.getRequiredPersistentEntity(PrimaryKeyOnProperty.class);

		CassandraPersistentProperty idProperty = persistentEntity.getIdProperty();

		assertThat(idProperty).satisfies(actual -> {

			assertThat(actual.getColumnName().toCql()).isEqualTo("foo");
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
		assertThat(firstname.getColumnName().toCql()).isEqualTo("firstname");

		CassandraPersistentProperty lastname = persistentEntity.getRequiredPersistentProperty("lastname");

		assertThat(lastname.isPrimaryKeyColumn()).isTrue();
		assertThat(lastname.isClusterKeyColumn()).isTrue();
		assertThat(lastname.getColumnName().toCql()).isEqualTo("mylastname");
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
		assertThat(firstname.getColumnName().toCql()).isEqualTo("firstname");

		CassandraPersistentProperty lastname = primaryKeyClass.getRequiredPersistentProperty("lastname");

		assertThat(lastname.isPrimaryKeyColumn()).isTrue();
		assertThat(lastname.isPartitionKeyColumn()).isFalse();
		assertThat(lastname.isClusterKeyColumn()).isTrue();
		assertThat(lastname.getColumnName().toCql()).isEqualTo("mylastname");
	}

	@Test // DATACASS-340
	public void createdTableSpecificationShouldConsiderClusterColumnOrdering() {

		CassandraPersistentEntity<?> persistentEntity = mappingContext
				.getRequiredPersistentEntity(EntityWithOrderedClusteredColumns.class);

		CreateTableSpecification tableSpecification = mappingContext.getCreateTableSpecificationFor(persistentEntity);

		assertThat(tableSpecification.getPartitionKeyColumns()).hasSize(1);
		assertThat(tableSpecification.getClusteredKeyColumns()).hasSize(3);

		ColumnSpecification breed = tableSpecification.getClusteredKeyColumns().get(0);
		assertThat(breed.getName().toCql()).isEqualTo("breed");
		assertThat(breed.getOrdering()).isEqualTo(Ordering.ASCENDING);

		ColumnSpecification color = tableSpecification.getClusteredKeyColumns().get(1);
		assertThat(color.getName().toCql()).isEqualTo("color");
		assertThat(color.getOrdering()).isEqualTo(Ordering.DESCENDING);

		ColumnSpecification kind = tableSpecification.getClusteredKeyColumns().get(2);
		assertThat(kind.getName().toCql()).isEqualTo("kind");
		assertThat(kind.getOrdering()).isEqualTo(Ordering.ASCENDING);
	}

	@Test // DATACASS-340
	public void createdTableSpecificationShouldConsiderPrimaryKeyClassClusterColumnOrdering() {

		CassandraPersistentEntity<?> persistentEntity = mappingContext
				.getRequiredPersistentEntity(EntityWithPrimaryKeyWithOrderedClusteredColumns.class);

		CreateTableSpecification tableSpecification = mappingContext.getCreateTableSpecificationFor(persistentEntity);

		assertThat(tableSpecification.getPartitionKeyColumns()).hasSize(1);
		assertThat(tableSpecification.getClusteredKeyColumns()).hasSize(3);

		ColumnSpecification breed = tableSpecification.getClusteredKeyColumns().get(0);
		assertThat(breed.getName().toCql()).isEqualTo("breed");
		assertThat(breed.getOrdering()).isEqualTo(Ordering.ASCENDING);

		ColumnSpecification color = tableSpecification.getClusteredKeyColumns().get(1);
		assertThat(color.getName().toCql()).isEqualTo("color");
		assertThat(color.getOrdering()).isEqualTo(Ordering.DESCENDING);

		ColumnSpecification kind = tableSpecification.getClusteredKeyColumns().get(2);
		assertThat(kind.getName().toCql()).isEqualTo("kind");
		assertThat(kind.getOrdering()).isEqualTo(Ordering.ASCENDING);
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

	@Test // DATACASS-213
	public void createIndexShouldConsiderAnnotatedProperties() {

		List<CreateIndexSpecification> specifications = mappingContext.getCreateIndexSpecificationsFor(
				mappingContext.getRequiredPersistentEntity(IndexedType.class));

		CreateIndexSpecification firstname = getSpecificationFor("first_name", specifications);

		assertThat(firstname.getColumnName()).isEqualTo(CqlIdentifier.of("first_name"));
		assertThat(firstname.getTableName()).isEqualTo(CqlIdentifier.of("indexedtype"));
		assertThat(firstname.getName()).isEqualTo(CqlIdentifier.of("my_index"));
		assertThat(firstname.getColumnFunction()).isEqualTo(ColumnFunction.NONE);

		CreateIndexSpecification phoneNumbers = getSpecificationFor("phoneNumbers", specifications);

		assertThat(phoneNumbers.getColumnName()).isEqualTo(CqlIdentifier.of("phoneNumbers"));
		assertThat(phoneNumbers.getTableName()).isEqualTo(CqlIdentifier.of("indexedtype"));
		assertThat(phoneNumbers.getName()).isNull();
		assertThat(phoneNumbers.getColumnFunction()).isEqualTo(ColumnFunction.NONE);
	}

	@Test // DATACASS-213
	public void createIndexForClusteredPrimaryKeyShouldConsiderAnnotatedAccessors() {

		List<CreateIndexSpecification> specifications = mappingContext
				.getCreateIndexSpecificationsFor(mappingContext.getRequiredPersistentEntity(CompositeKeyEntity.class));

		CreateIndexSpecification entries = getSpecificationFor("last_name", specifications);

		assertThat(entries.getColumnName()).isEqualTo(CqlIdentifier.of("last_name"));
		assertThat(entries.getTableName()).isEqualTo(CqlIdentifier.of("compositekeyentity"));
		assertThat(entries.getName()).isEqualTo(CqlIdentifier.of("my_index"));
		assertThat(entries.getColumnFunction()).isEqualTo(ColumnFunction.NONE);
	}

	private static CreateIndexSpecification getSpecificationFor(String column,
			List<CreateIndexSpecification> specifications) {

		return specifications.stream().filter(it -> it.getColumnName().equals(CqlIdentifier.of(column))).findFirst()
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

	@Test // DATACASS-349
	public void propertyTypeShouldConsiderRegisteredConverterForPropertyType() {

		mappingContext.setCustomConversions(
				new CassandraCustomConversions(Collections.singletonList(StringMapToStringConverter.INSTANCE)));

		CassandraPersistentEntity<?> persistentEntity = mappingContext
				.getRequiredPersistentEntity(TypeWithCustomConvertedMap.class);

		assertThat(mappingContext.getDataType(persistentEntity.getRequiredPersistentProperty("stringMap")))
				.isEqualTo(DataType.varchar());

		assertThat(mappingContext.getDataType(persistentEntity.getRequiredPersistentProperty("blobMap")))
				.isEqualTo(DataType.ascii());
	}

	@Test // DATACASS-349
	public void propertyTypeShouldConsiderRegisteredConverterForCollectionComponentType() {

		mappingContext.setCustomConversions(
				new CassandraCustomConversions(Collections.singletonList(HumanToStringConverter.INSTANCE)));

		CassandraPersistentEntity<?> persistentEntity = mappingContext
				.getRequiredPersistentEntity(TypeWithListOfHumans.class);

		assertThat(mappingContext.getDataType(persistentEntity.getRequiredPersistentProperty("humans")))
				.isEqualTo(DataType.list(DataType.varchar()));
	}

	@Test // DATACASS-172, DATACASS-455
	public void shouldRegisterUdtTypes() {

		CassandraPersistentEntity<?> persistentEntity = mappingContext.getRequiredPersistentEntity(MappedUdt.class);

		assertThat(persistentEntity.isUserDefinedType()).isTrue();

		assertThat(mappingContext.getUserDefinedTypeEntities()).hasSize(1);
		assertThat(mappingContext.getPersistentEntities()).hasSize(1);
		assertThat(mappingContext.getTableEntities()).hasSize(0);
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
		assertThat(mappingContext.usesUserType(CqlIdentifier.of("mappedudt"))).isFalse();
	}

	@Test // DATACASS-172, DATACASS-455
	public void usesTypeShouldReportTypeUsageInMappedUdt() {

		UserType myTypeMock = mock(UserType.class, "mappedudt");

		mappingContext.setUserTypeResolver(typeName -> myTypeMock);

		mappingContext.getRequiredPersistentEntity(WithUdt.class);

		assertThat(mappingContext.usesUserType(CqlIdentifier.of("mappedudt"))).isTrue();
	}

	@Test // DATACASS-172, DATACASS-455
	public void usesTypeShouldReportTypeUsageInColumn() {

		UserType myTypeMock = mock(UserType.class, "mappedudt");

		mappingContext.setUserTypeResolver(typeName -> myTypeMock);

		mappingContext.getRequiredPersistentEntity(MappedUdt.class);

		assertThat(mappingContext.usesUserType(CqlIdentifier.of("mappedudt"))).isTrue();
	}

	@Test // DATACASS-172
	public void createTableForComplexPrimaryKeyShouldFail() {

		try {
			mappingContext.getCreateTableSpecificationFor(
					mappingContext.getRequiredPersistentEntity(EntityWithComplexPrimaryKeyColumn.class));
			fail("Missing InvalidDataAccessApiUsageException");
		} catch (InvalidDataAccessApiUsageException e) {
			assertThat(e).hasMessageContaining("Unknown type [class java.lang.Object] for property [complexObject]");
		}

		try {
			mappingContext
					.getCreateTableSpecificationFor(mappingContext.getRequiredPersistentEntity(EntityWithComplexId.class));
			fail("Missing InvalidDataAccessApiUsageException");
		} catch (InvalidDataAccessApiUsageException e) {
			assertThat(e).hasMessageContaining("Unknown type [class java.lang.Object] for property [complexObject]");
		}

		try {
			mappingContext.getCreateTableSpecificationFor(
					mappingContext.getRequiredPersistentEntity(EntityWithPrimaryKeyClassWithComplexId.class));
			fail("Missing InvalidDataAccessApiUsageException");
		} catch (InvalidDataAccessApiUsageException e) {
			assertThat(e).hasMessageContaining("Unknown type [class java.lang.Object] for property [complexObject]");
		}
	}

	@Test // DATACASS-282, DATACASS-455
	public void shouldNotRetainInvalidEntitiesInCache() {

		TableMetadata tableMetadata = mock(TableMetadata.class);
		when(tableMetadata.getName())
				.thenReturn(InvalidEntityWithIdAndPrimaryKeyColumn.class.getSimpleName().toLowerCase());

		try {
			mappingContext.getPersistentEntity(InvalidEntityWithIdAndPrimaryKeyColumn.class);
			fail("Missing MappingException");
		} catch (MappingException e) {
			assertThat(e).isInstanceOf(VerifierMappingExceptions.class);
		}

		assertThat(mappingContext.getUserDefinedTypeEntities()).isEmpty();
		assertThat(mappingContext.getTableEntities()).isEmpty();
		assertThat(mappingContext.getPersistentEntities()).isEmpty();
		assertThat(mappingContext.usesTable(CqlIdentifier.of(tableMetadata.getName()))).isFalse();
	}

	@Table
	private static class InvalidEntityWithIdAndPrimaryKeyColumn {

		@Id String foo;
		@PrimaryKeyColumn String bar;
	}

	@Table
	static class EntityWithComplexPrimaryKeyColumn {

		@PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED) Object complexObject;
	}

	@Table
	static class EntityWithComplexId {

		@Id Object complexObject;
	}

	@PrimaryKeyClass
	static class PrimaryKeyClassWithComplexId {

		@PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED) Object complexObject;
	}

	@Table
	static class EntityWithPrimaryKeyClassWithComplexId {

		@Id PrimaryKeyClassWithComplexId primaryKeyClassWithComplexId;
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

	@UserDefinedType
	private static class MappedUdt {}

	@Table
	private static class WithUdt {

		@Id String id;

		@CassandraType(type = DataType.Name.UDT, userTypeName = "mappedudt") UDTValue udtValue;
	}

	enum HumanToStringConverter implements Converter<Human, String> {

		INSTANCE;

		@Override
		public String convert(Human source) {
			return "hello";
		}
	}

	@Table
	private static class TypeWithCustomConvertedMap {

		@Id String id;
		Map<String, Collection<String>> stringMap;

		@CassandraType(type = Name.ASCII) Map<String, Collection<String>> blobMap;
	}

	@Table
	private static class TypeWithListOfHumans {

		@Id String id;
		List<Human> humans;
	}

	@WritingConverter
	enum StringMapToStringConverter implements Converter<Map<String, Collection<String>>, String> {

		INSTANCE;

		@Override
		public String convert(Map<String, Collection<String>> source) {
			return "serialized";
		}
	}
}
