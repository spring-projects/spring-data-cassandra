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
package org.springframework.data.cassandra.core.convert;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.data.cassandra.core.mapping.CassandraType.*;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.cql.Ordering;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.cql.keyspace.ColumnSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.CreateIndexSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.CreateIndexSpecification.ColumnFunction;
import org.springframework.data.cassandra.core.cql.keyspace.CreateTableSpecification;
import org.springframework.data.cassandra.core.mapping.*;
import org.springframework.data.cassandra.domain.AllPossibleTypes;
import org.springframework.data.cassandra.support.UserDefinedTypeBuilder;
import org.springframework.data.mapping.MappingException;
import org.springframework.util.StringUtils;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Unit tests for {@link SchemaFactory}.
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
 * @author Christoph Strobl
 */
public class SchemaFactoryUnitTests {

	private CassandraMappingContext ctx = new CassandraMappingContext();
	private SchemaFactory schemaFactory;

	@BeforeEach
	void before() {

		List<Converter<?, ?>> converters = new ArrayList<>();
		converters.add(new PersonReadConverter());
		converters.add(new PersonWriteConverter());
		converters.add(HumanToStringConverter.INSTANCE);

		CassandraCustomConversions customConversions = new CassandraCustomConversions(converters);
		ctx.setCustomConversions(customConversions);
		schemaFactory = new SchemaFactory(new MappingCassandraConverter(ctx));
	}

	@Test // DATACASS-340
	void createdTableSpecificationShouldConsiderClusterColumnOrdering() {

		CassandraPersistentEntity<?> persistentEntity = ctx
				.getRequiredPersistentEntity(EntityWithOrderedClusteredColumns.class);

		CreateTableSpecification tableSpecification = schemaFactory.getCreateTableSpecificationFor(persistentEntity);

		assertThat(tableSpecification.getPartitionKeyColumns()).hasSize(1);
		assertThat(tableSpecification.getClusteredKeyColumns()).hasSize(3);

		ColumnSpecification breed = tableSpecification.getClusteredKeyColumns().get(0);
		assertThat(breed.getName().toString()).isEqualTo("breed");
		assertThat(breed.getOrdering()).isEqualTo(Ordering.ASCENDING);

		ColumnSpecification color = tableSpecification.getClusteredKeyColumns().get(1);
		assertThat(color.getName().toString()).isEqualTo("color");
		assertThat(color.getOrdering()).isEqualTo(Ordering.DESCENDING);

		ColumnSpecification kind = tableSpecification.getClusteredKeyColumns().get(2);
		assertThat(kind.getName().toString()).isEqualTo("kind");
		assertThat(kind.getOrdering()).isEqualTo(Ordering.ASCENDING);
	}

	@Test // DATACASS-340
	void createdTableSpecificationShouldConsiderPrimaryKeyClassClusterColumnOrdering() {

		CassandraPersistentEntity<?> persistentEntity = ctx
				.getRequiredPersistentEntity(EntityWithPrimaryKeyWithOrderedClusteredColumns.class);

		CreateTableSpecification tableSpecification = schemaFactory.getCreateTableSpecificationFor(persistentEntity);

		assertThat(tableSpecification.getPartitionKeyColumns()).hasSize(1);
		assertThat(tableSpecification.getClusteredKeyColumns()).hasSize(3);

		ColumnSpecification breed = tableSpecification.getClusteredKeyColumns().get(0);
		assertThat(breed.getName().toString()).isEqualTo("breed");
		assertThat(breed.getOrdering()).isEqualTo(Ordering.ASCENDING);

		ColumnSpecification color = tableSpecification.getClusteredKeyColumns().get(1);
		assertThat(color.getName().toString()).isEqualTo("color");
		assertThat(color.getOrdering()).isEqualTo(Ordering.DESCENDING);

		ColumnSpecification kind = tableSpecification.getClusteredKeyColumns().get(2);
		assertThat(kind.getName().toString()).isEqualTo("kind");
		assertThat(kind.getOrdering()).isEqualTo(Ordering.ASCENDING);
	}

	@Test // DATACASS-487
	void shouldCreateTableForMappedAndConvertedColumn() {

		UserDefinedType mappedudt = UserDefinedTypeBuilder.forName("mappedudt").withField("foo", DataTypes.ASCII).build();

		this.ctx.setUserTypeResolver(typeName -> mappedudt);

		CassandraPersistentEntity<?> persistentEntity = this.ctx.getRequiredPersistentEntity(WithMapOfMixedTypes.class);

		CreateTableSpecification tableSpecification = schemaFactory.getCreateTableSpecificationFor(persistentEntity);

		assertThat(tableSpecification.getColumns()).hasSize(2);

		ColumnSpecification column = tableSpecification.getColumns().get(1);

		assertThat(column.getType().asCql(true, true)).isEqualTo("map<frozen<mappedudt>, list<text>>");
	}

	@PrimaryKeyClass
	private static class CompositePrimaryKeyClassWithProperties implements Serializable {

		private String firstname;
		private String lastname;

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
	private static class EntityWithOrderedClusteredColumns {

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
	void createIndexShouldConsiderAnnotatedProperties() {

		List<CreateIndexSpecification> specifications = schemaFactory
				.getCreateIndexSpecificationsFor(ctx.getRequiredPersistentEntity(IndexedType.class));

		CreateIndexSpecification firstname = getSpecificationFor("first_name", specifications);

		assertThat(firstname.getColumnName()).isEqualTo(CqlIdentifier.fromCql("first_name"));
		assertThat(firstname.getTableName()).isEqualTo(CqlIdentifier.fromCql("indexedtype"));
		assertThat(firstname.getName()).isEqualTo(CqlIdentifier.fromCql("my_index"));
		assertThat(firstname.getColumnFunction()).isEqualTo(ColumnFunction.NONE);

		CreateIndexSpecification phoneNumbers = getSpecificationFor("phoneNumbers", specifications);

		assertThat(phoneNumbers.getColumnName()).isEqualTo(CqlIdentifier.fromCql("phoneNumbers"));
		assertThat(phoneNumbers.getTableName()).isEqualTo(CqlIdentifier.fromCql("indexedtype"));
		assertThat(phoneNumbers.getName()).isNull();
		assertThat(phoneNumbers.getColumnFunction()).isEqualTo(ColumnFunction.NONE);
	}

	@Test // DATACASS-213
	void createIndexForClusteredPrimaryKeyShouldConsiderAnnotatedAccessors() {

		List<CreateIndexSpecification> specifications = schemaFactory
				.getCreateIndexSpecificationsFor(ctx.getRequiredPersistentEntity(CompositeKeyEntity.class));

		CreateIndexSpecification entries = getSpecificationFor("last_name", specifications);

		assertThat(entries.getColumnName()).isEqualTo(CqlIdentifier.fromCql("last_name"));
		assertThat(entries.getTableName()).isEqualTo(CqlIdentifier.fromCql("compositekeyentity"));
		assertThat(entries.getName()).isEqualTo(CqlIdentifier.fromCql("my_index"));
		assertThat(entries.getColumnFunction()).isEqualTo(ColumnFunction.NONE);
	}

	@Test // DATACASS-284, DATACASS-651
	void shouldRejectUntypedTuples() {

		assertThatThrownBy(() -> this.schemaFactory
				.getCreateTableSpecificationFor(this.ctx.getRequiredPersistentEntity(UntypedTupleEntity.class)))
						.isInstanceOf(MappingException.class);

		assertThatThrownBy(() -> this.schemaFactory
				.getCreateTableSpecificationFor(this.ctx.getRequiredPersistentEntity(UntypedTupleMapEntity.class)))
						.isInstanceOf(MappingException.class);
	}

	@Test // DATACASS-284
	void shouldCreateTableForTypedTupleType() {

		CreateTableSpecification tableSpecification = this.schemaFactory
				.getCreateTableSpecificationFor(this.ctx.getRequiredPersistentEntity(TypedTupleEntity.class));

		assertThat(tableSpecification.getColumns()).hasSize(2);

		ColumnSpecification column = tableSpecification.getColumns().get(1);

		assertThat(column.getType()).isInstanceOf(TupleType.class);
		assertThat(column.getType()).isEqualTo(DataTypes.tupleOf(DataTypes.TEXT, DataTypes.BIGINT));
	}

	@Test // DATACASS-651
	void shouldCreateTableForEntityWithMapOfTuples() {

		CreateTableSpecification tableSpecification = this.schemaFactory
				.getCreateTableSpecificationFor(this.ctx.getRequiredPersistentEntity(EntityWithMapOfTuples.class));

		assertThat(tableSpecification.getColumns()).hasSize(2);

		ColumnSpecification column = tableSpecification.getColumns().get(1);
		assertThat(column.getName()).isEqualTo(CqlIdentifier.fromCql("map"));
		assertThat(column.getType().asCql(true, true)).isEqualTo("map<text, frozen<tuple<mappedudt, human_udt, text>>>");
	}

	private static CreateIndexSpecification getSpecificationFor(String column,
			List<CreateIndexSpecification> specifications) {

		return specifications.stream().filter(it -> it.getColumnName().equals(CqlIdentifier.fromCql(column))).findFirst()
				.orElseThrow(() -> new NoSuchElementException(column));
	}

	private static class IndexedType {

		@PrimaryKeyColumn("first_name") @Indexed("my_index") String firstname;

		@Indexed List<String> phoneNumbers;
	}

	@PrimaryKeyClass
	static class CompositeKeyWithIndex {

		@PrimaryKeyColumn(value = "first_name", type = PrimaryKeyType.PARTITIONED) String firstname;
		@PrimaryKeyColumn("last_name") @Indexed("my_index") String lastname;
	}

	private static class CompositeKeyEntity {

		@PrimaryKey CompositeKeyWithIndex key;
	}

	private static class InvalidMapIndex {

		@Indexed Map<@Indexed String, String> mixed;
	}

	@Test // DATACASS-506
	void shouldCreatedUserTypeSpecificationsWithAnnotatedTypeName() {

		assertThat(schemaFactory.getCreateUserTypeSpecificationFor(ctx.getRequiredPersistentEntity(WithUdt.class)))
				.isNotNull();
		assertThat(schemaFactory.getCreateUserTypeSpecificationFor(ctx.getRequiredPersistentEntity(Nested.class)))
				.isNotNull();
	}

	@Test // DATACASS-172
	void createTableForComplexPrimaryKeyShouldFail() {

		try {
			schemaFactory
					.getCreateTableSpecificationFor(ctx.getRequiredPersistentEntity(EntityWithComplexPrimaryKeyColumn.class));
			fail("Missing MappingException");
		} catch (MappingException e) {
			assertThat(e).hasMessageContaining(
					"Cannot resolve DataType for type [class java.lang.Object] for property [complexObject]");
		}

		try {
			schemaFactory.getCreateTableSpecificationFor(ctx.getRequiredPersistentEntity(EntityWithComplexId.class));
			fail("Missing MappingException");
		} catch (MappingException e) {
			assertThat(e).hasMessageContaining(
					"Cannot resolve DataType for type [class java.lang.Object] for property [complexObject]");
		}

		try {
			schemaFactory.getCreateTableSpecificationFor(
					ctx.getRequiredPersistentEntity(EntityWithPrimaryKeyClassWithComplexId.class));
			fail("Missing MappingException");
		} catch (MappingException e) {
			assertThat(e).hasMessageContaining(
					"Cannot resolve DataType for type [class java.lang.Object] for property [complexObject]");
		}
	}

	@Test // DATACASS-296
	void customConversionTestShouldCreateCorrectTableDefinition() {

		CassandraPersistentEntity<?> persistentEntity = ctx.getRequiredPersistentEntity(Employee.class);

		CreateTableSpecification specification = schemaFactory.getCreateTableSpecificationFor(persistentEntity);

		assertThat(getColumnType("human", specification)).isEqualTo(DataTypes.TEXT);

		ColumnSpecification friends = getColumn("friends", specification);
		assertThat(friends.getType()).isInstanceOf(ListType.class);

		ListType friendsCollection = (ListType) friends.getType();
		assertThat(friendsCollection.getProtocolCode()).isEqualTo(ProtocolConstants.DataType.LIST);
		assertThat(friendsCollection.getElementType()).isEqualTo(DataTypes.TEXT);

		ColumnSpecification people = getColumn("people", specification);
		assertThat(people.getType()).isInstanceOf(SetType.class);

		SetType peopleCollection = (SetType) people.getType();
		assertThat(peopleCollection.getProtocolCode()).isEqualTo(ProtocolConstants.DataType.SET);
		assertThat(peopleCollection.getElementType()).isEqualTo(DataTypes.TEXT);
	}

	@Test // DATACASS-296
	void customConversionTestShouldHonorTypeAnnotationAndCreateCorrectTableDefinition() {

		CassandraPersistentEntity<?> persistentEntity = ctx.getRequiredPersistentEntity(Employee.class);

		CreateTableSpecification specification = schemaFactory.getCreateTableSpecificationFor(persistentEntity);

		assertThat(getColumnType("floater", specification)).isEqualTo(DataTypes.FLOAT);

		ColumnSpecification enemies = getColumn("enemies", specification);
		assertThat(enemies.getType()).isInstanceOf(SetType.class);

		SetType enemiesCollection = (SetType) enemies.getType();
		assertThat(enemiesCollection.getProtocolCode()).isEqualTo(ProtocolConstants.DataType.SET);
		assertThat(enemiesCollection.getElementType()).isEqualTo(DataTypes.BIGINT);
	}

	@Test // DATACASS-296
	void columnsShouldMapToVarchar() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(AllPossibleTypes.class);

		assertThat(getColumnType("id", specification)).isEqualTo(DataTypes.TEXT);
		assertThat(getColumnType("zoneId", specification)).isEqualTo(DataTypes.TEXT);
		assertThat(getColumnType("bpZoneId", specification)).isEqualTo(DataTypes.TEXT);
		assertThat(getColumnType("anEnum", specification)).isEqualTo(DataTypes.TEXT);
	}

	@Test // DATACASS-296
	void columnsShouldMapToTinyInt() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(AllPossibleTypes.class);

		assertThat(getColumnType("boxedByte", specification)).isEqualTo(DataTypes.TINYINT);
		assertThat(getColumnType("primitiveByte", specification)).isEqualTo(DataTypes.TINYINT);
	}

	@Test // DATACASS-296
	void columnsShouldMapToSmallInt() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(AllPossibleTypes.class);

		assertThat(getColumnType("boxedShort", specification)).isEqualTo(DataTypes.SMALLINT);
		assertThat(getColumnType("primitiveShort", specification)).isEqualTo(DataTypes.SMALLINT);
	}

	@Test // DATACASS-296
	void columnsShouldMapToBigInt() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(AllPossibleTypes.class);

		assertThat(getColumnType("boxedLong", specification)).isEqualTo(DataTypes.BIGINT);
		assertThat(getColumnType("primitiveLong", specification)).isEqualTo(DataTypes.BIGINT);
	}

	@Test // DATACASS-296
	void columnsShouldMapToVarInt() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(AllPossibleTypes.class);

		assertThat(getColumnType("bigInteger", specification)).isEqualTo(DataTypes.VARINT);
	}

	@Test // DATACASS-296
	void columnsShouldMapToDecimal() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(AllPossibleTypes.class);

		assertThat(getColumnType("bigDecimal", specification)).isEqualTo(DataTypes.DECIMAL);
	}

	@Test // DATACASS-296
	void columnsShouldMapToInt() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(AllPossibleTypes.class);

		assertThat(getColumnType("boxedInteger", specification)).isEqualTo(DataTypes.INT);
		assertThat(getColumnType("primitiveInteger", specification)).isEqualTo(DataTypes.INT);
	}

	@Test // DATACASS-296
	void columnsShouldMapToFloat() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(AllPossibleTypes.class);

		assertThat(getColumnType("boxedFloat", specification)).isEqualTo(DataTypes.FLOAT);
		assertThat(getColumnType("primitiveFloat", specification)).isEqualTo(DataTypes.FLOAT);
	}

	@Test // DATACASS-296
	void columnsShouldMapToDouble() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(AllPossibleTypes.class);

		assertThat(getColumnType("boxedDouble", specification)).isEqualTo(DataTypes.DOUBLE);
		assertThat(getColumnType("primitiveDouble", specification)).isEqualTo(DataTypes.DOUBLE);
	}

	@Test // DATACASS-296
	void columnsShouldMapToBoolean() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(AllPossibleTypes.class);

		assertThat(getColumnType("boxedBoolean", specification)).isEqualTo(DataTypes.BOOLEAN);
		assertThat(getColumnType("primitiveBoolean", specification)).isEqualTo(DataTypes.BOOLEAN);
	}

	@Test // DATACASS-296
	void columnsShouldMapToDate() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(AllPossibleTypes.class);

		assertThat(getColumnType("date", specification)).isEqualTo(DataTypes.DATE);
		assertThat(getColumnType("jodaLocalDate", specification)).isEqualTo(DataTypes.DATE);
		assertThat(getColumnType("bpLocalDate", specification)).isEqualTo(DataTypes.DATE);
	}

	@Test // DATACASS-296
	void columnsShouldMapToTimestamp() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(AllPossibleTypes.class);

		assertThat(getColumnType("timestamp", specification)).isEqualTo(DataTypes.TIMESTAMP);
		assertThat(getColumnType("localDateTime", specification)).isEqualTo(DataTypes.TIMESTAMP);
		assertThat(getColumnType("instant", specification)).isEqualTo(DataTypes.TIMESTAMP);
		assertThat(getColumnType("jodaLocalDateTime", specification)).isEqualTo(DataTypes.TIMESTAMP);
		assertThat(getColumnType("jodaDateTime", specification)).isEqualTo(DataTypes.TIMESTAMP);
		assertThat(getColumnType("bpLocalDateTime", specification)).isEqualTo(DataTypes.TIMESTAMP);
		assertThat(getColumnType("bpInstant", specification)).isEqualTo(DataTypes.TIMESTAMP);
	}

	@Test // DATACASS-296
	void columnsShouldMapToTimestampUsingOverrides() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(TypeWithOverrides.class);

		assertThat(getColumnType("localDate", specification)).isEqualTo(DataTypes.TIMESTAMP);
		assertThat(getColumnType("jodaLocalDate", specification)).isEqualTo(DataTypes.TIMESTAMP);
	}

	@Test // DATACASS-296
	void columnsShouldMapToBlob() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(AllPossibleTypes.class);

		assertThat(getColumnType("blob", specification)).isEqualTo(DataTypes.BLOB);
	}

	@Test // DATACASS-172
	void columnsShouldMapToUdt() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(WithUdtFields.class);

		assertThat(getColumnType("human", specification).asCql(false, true)).isEqualTo("human_udt");
		assertThat(getColumnType("friends", specification).asCql(false, true)).isEqualTo("list<species_udt>");
		assertThat(getColumnType("people", specification).asCql(false, true)).isEqualTo("set<peeps_udt>");
	}

	@Test // DATACASS-172
	void columnsShouldMapToMappedUserType() {

		UserDefinedType mappedUdt = new SchemaFactory.ShallowUserDefinedType("mappedudt", true);

		ctx.setUserTypeResolver(typeName -> {

			if (typeName.equals(mappedUdt.getName())) {
				return mappedUdt;
			}
			return null;
		});

		CreateTableSpecification specification = getCreateTableSpecificationFor(WithMappedUdtFields.class);

		assertThat(getColumnType("human", specification)).isEqualTo(mappedUdt);
		assertThat(getColumnType("friends", specification)).isEqualTo(DataTypes.listOf(mappedUdt));
		assertThat(getColumnType("people", specification)).isEqualTo(DataTypes.setOf(mappedUdt));
		assertThat(getColumnType("stringToUdt", specification)).isEqualTo(DataTypes.mapOf(DataTypes.TEXT, mappedUdt));
		assertThat(getColumnType("udtToString", specification)).isEqualTo(DataTypes.mapOf(mappedUdt, DataTypes.TEXT));
	}

	@Test // DATACASS-523
	void columnsShouldMapToTuple() {

		UserDefinedType mappedUdt = mock(UserDefinedType.class, "mappedudt");
		UserDefinedType human_udt = mock(UserDefinedType.class, "human_udt");

		when(human_udt.copy(true)).thenReturn(human_udt);
		when(mappedUdt.copy(true)).thenReturn(mappedUdt);

		when(mappedUdt.asCql(anyBoolean(), anyBoolean())).thenReturn("mappedudt");
		when(human_udt.asCql(anyBoolean(), anyBoolean())).thenReturn("human_udt");

		ctx.setUserTypeResolver(typeName -> {

			if (typeName.toString().equals(mappedUdt.toString())) {
				return mappedUdt;
			}

			if (typeName.toString().equals(human_udt.toString())) {
				return human_udt;
			}
			return null;
		});

		CreateTableSpecification specification = getCreateTableSpecificationFor(WithMappedTuple.class);

		assertThat(getColumnType("mappedTuple", specification).asCql(true, true))
				.isEqualTo("frozen<tuple<mappedudt, human_udt, text>>");

		assertThat(getColumnType("mappedTuples", specification).asCql(true, true))
				.isEqualTo("list<frozen<tuple<mappedudt, human_udt, text>>>");
	}

	@Test // DATACASS-678
	void createTableSpecificationShouldConsiderCustomTableName() {

		CqlIdentifier customTableName = CqlIdentifier.fromCql("my_custom_came");

		CassandraPersistentEntity<?> persistentEntity = ctx.getRequiredPersistentEntity(Employee.class);
		CreateTableSpecification specification = schemaFactory.getCreateTableSpecificationFor(persistentEntity,
				customTableName);

		assertThat(specification).isNotNull();
		assertThat(specification.getName()).isEqualTo(customTableName);
	}

	private CreateTableSpecification getCreateTableSpecificationFor(Class<?> persistentEntityClass) {

		CassandraCustomConversions customConversions = new CassandraCustomConversions(Collections.emptyList());
		ctx.setCustomConversions(customConversions);

		CassandraPersistentEntity<?> persistentEntity = ctx.getRequiredPersistentEntity(persistentEntityClass);
		return schemaFactory.getCreateTableSpecificationFor(persistentEntity);
	}

	private DataType getColumnType(String columnName, CreateTableSpecification specification) {
		return getColumn(columnName, specification).getType();
	}

	private ColumnSpecification getColumn(String columnName, CreateTableSpecification specification) {

		for (ColumnSpecification columnSpecification : specification.getColumns()) {
			if (columnSpecification.getName().equals(CqlIdentifier.fromCql(columnName))) {
				return columnSpecification;
			}
		}

		throw new IllegalArgumentException(
				String.format("Cannot find column '%s' amongst '%s'", columnName, specification.getColumns()));
	}

	@Data
	@Table
	private static class Employee {

		@Id String id;

		Human human;
		List<Human> friends;
		Set<Human> people;

		@CassandraType(type = Name.FLOAT) Human floater;
		@CassandraType(type = Name.SET, typeArguments = Name.BIGINT) List<Human> enemies;
	}

	@Data
	@Table
	private static class WithMappedTuple {

		@Id String id;
		MappedTuple mappedTuple;
		List<MappedTuple> mappedTuples;
	}

	@Tuple
	private static class MappedTuple {

		@Element(0) MappedUdt mappedUdt;
		@Element(1) @CassandraType(type = Name.UDT, userTypeName = "human_udt") UdtValue human;
		@Element(2) String text;
	}

	@Data
	@Table
	private static class WithUdtFields {

		@Id String id;

		@CassandraType(type = Name.UDT, userTypeName = "human_udt") UdtValue human;
		@CassandraType(type = Name.LIST, typeArguments = Name.UDT, userTypeName = "species_udt") List<UdtValue> friends;
		@CassandraType(type = Name.SET, typeArguments = Name.UDT, userTypeName = "peeps_udt") Set<UdtValue> people;
	}

	@Data
	@Table
	private static class WithMappedUdtFields {

		@Id String id;

		MappedUdt human;
		List<MappedUdt> friends;
		Set<MappedUdt> people;
		Map<String, MappedUdt> stringToUdt;
		Map<MappedUdt, String> udtToString;
	}

	@org.springframework.data.cassandra.core.mapping.UserDefinedType
	private static class MappedUdt {}

	@Data
	@AllArgsConstructor
	@NoArgsConstructor
	static class Human {

		String firstname;
		String lastname;
	}

	@Data
	@Table
	private static class TypeWithOverrides {

		@Id String id;

		@CassandraType(type = Name.TIMESTAMP) java.time.LocalDate localDate;
		@CassandraType(type = Name.TIMESTAMP) org.joda.time.LocalDate jodaLocalDate;
	}

	private static class PersonReadConverter implements Converter<String, Human> {

		public Human convert(String source) {

			if (StringUtils.hasText(source)) {
				try {
					return new ObjectMapper().readValue(source, Human.class);
				} catch (IOException e) {
					throw new IllegalStateException(e);
				}
			}

			return null;
		}
	}

	private static class PersonWriteConverter implements Converter<Human, String> {

		public String convert(Human source) {

			try {
				return new ObjectMapper().writeValueAsString(source);
			} catch (IOException e) {
				throw new IllegalStateException(e);
			}
		}
	}

	@Table
	private static class InvalidEntityWithIdAndPrimaryKeyColumn {
		@Id String foo;
		@PrimaryKeyColumn String bar;
	}

	@Table
	private static class EntityWithComplexPrimaryKeyColumn {
		@PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED) Object complexObject;
	}

	@Table
	private static class EntityWithComplexId {
		@Id Object complexObject;
	}

	@PrimaryKeyClass
	static class PrimaryKeyClassWithComplexId {
		@PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED) Object complexObject;
	}

	@Table
	private static class EntityWithPrimaryKeyClassWithComplexId {
		@Id PrimaryKeyClassWithComplexId primaryKeyClassWithComplexId;
	}

	@Table
	private static class X {
		@PrimaryKey String key;
	}

	@Table
	private static class Y {
		@PrimaryKey String key;
	}

	@Table
	private static class WithUdt {
		@Id String id;
		@CassandraType(type = Name.UDT, userTypeName = "mappedudt") UdtValue udtValue;
		@CassandraType(type = Name.UDT, userTypeName = "NestedType") Nested nested;
	}

	@Table
	private static class WithMapOfMixedTypes {
		@Id String id;
		Map<MappedUdt, List<Human>> people;
	}

	enum HumanToStringConverter implements Converter<Human, String> {

		INSTANCE;

		@Override
		public String convert(Human source) {
			return "hello";
		}
	}

	@org.springframework.data.cassandra.core.mapping.UserDefinedType(value = "NestedType")
	static class Nested {
		String s1;
		@CassandraType(type = Name.UDT, userTypeName = "AnotherNestedType") AnotherNested anotherNested;
	}

	@org.springframework.data.cassandra.core.mapping.UserDefinedType(value = "AnotherNestedType")
	static class AnotherNested {
		String str;
	}

	@Table
	private static class TypedTupleEntity {
		@Id String id;
		@CassandraType(type = Name.TUPLE, typeArguments = { Name.VARCHAR, Name.BIGINT }) TupleValue typed;
	}

	@Table
	private static class EntityWithMapOfTuples {
		@Id String id;
		Map<String, MappedTuple> map;
	}

	@Table
	private static class UntypedTupleEntity {
		@Id String id;
		TupleValue untyped;
	}

	@Table
	private static class UntypedTupleMapEntity {
		@Id String id;
		Map<String, TupleValue> untyped;
	}

	@Table
	@Data
	static class TypeWithEmbedded {

		@Id String id;
		@Embedded.Nullable EmbeddedTpe name;
		@Embedded.Nullable(prefix = "a") EmbeddedTpe alias;
		@Indexed @Embedded.Nullable(prefix = "aego") EmbeddedTpe alterEgo;
	}

	@Data
	static class EmbeddedTpe {

		@Indexed String firstname;
		String lastname;
	}

	@Test // DATACASS-167
	void createTableSpecificationShouldConsiderEmbeddedType() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(TypeWithEmbedded.class);

		assertThat(specification).isNotNull();
		assertThat(getColumn("firstname", specification)).isNotNull();
		assertThat(getColumn("lastname", specification)).isNotNull();
		assertThat(getColumn("afirstname", specification)).isNotNull();
		assertThat(getColumn("alastname", specification)).isNotNull();
		assertThat(getColumn("aegofirstname", specification)).isNotNull();
		assertThat(getColumn("aegolastname", specification)).isNotNull();
	}

	@Test // DATACASS-167
	void createIndexSpecificationShouldConsiderEmbeddedType() {

		List<CreateIndexSpecification> specifications = schemaFactory
				.getCreateIndexSpecificationsFor(ctx.getRequiredPersistentEntity(TypeWithEmbedded.class));

		CreateIndexSpecification firstname = getSpecificationFor("firstname", specifications);

		assertThat(firstname.getColumnName()).isEqualTo(CqlIdentifier.fromCql("firstname"));
		assertThat(firstname.getName()).isNull();
		assertThat(firstname.getColumnFunction()).isEqualTo(ColumnFunction.NONE);

		CreateIndexSpecification afirstname = getSpecificationFor("afirstname", specifications);

		assertThat(afirstname.getColumnName()).isEqualTo(CqlIdentifier.fromCql("afirstname"));
		assertThat(afirstname.getName()).isNull();
		assertThat(afirstname.getColumnFunction()).isEqualTo(ColumnFunction.NONE);

		CreateIndexSpecification aegofirstname = getSpecificationFor("aegofirstname", specifications);
		assertThat(aegofirstname.getColumnName()).isEqualTo(CqlIdentifier.fromCql("aegofirstname"));

		CreateIndexSpecification aegolastname = getSpecificationFor("aegolastname", specifications);
		assertThat(aegolastname.getColumnName()).isEqualTo(CqlIdentifier.fromCql("aegolastname"));
	}

}
