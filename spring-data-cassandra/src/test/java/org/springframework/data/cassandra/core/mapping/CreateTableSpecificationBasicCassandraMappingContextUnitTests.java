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

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.convert.CassandraCustomConversions;
import org.springframework.data.cassandra.core.cql.keyspace.ColumnSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.CreateTableSpecification;
import org.springframework.data.cassandra.domain.AllPossibleTypes;
import org.springframework.util.StringUtils;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Unit tests for {@link CassandraMappingContext} targeted on {@link CreateTableSpecification}.
 *
 * @author Mark Paluch
 * @author Vagif Zeynalov
 * @soundtrack Black Rose - Volbeat
 */
public class CreateTableSpecificationBasicCassandraMappingContextUnitTests {

	private CassandraMappingContext ctx = new CassandraMappingContext();

	@Before
	public void setUp() {

		List<Converter<?, ?>> converters = new ArrayList<>();
		converters.add(new PersonReadConverter());
		converters.add(new PersonWriteConverter());

		CassandraCustomConversions customConversions = new CassandraCustomConversions(converters);
		ctx.setCustomConversions(customConversions);
	}

	@Test // DATACASS-296
	public void customConversionTestShouldCreateCorrectTableDefinition() {

		CassandraPersistentEntity<?> persistentEntity = ctx.getRequiredPersistentEntity(Employee.class);

		CreateTableSpecification specification = ctx.getCreateTableSpecificationFor(persistentEntity);

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
	public void customConversionTestShouldHonorTypeAnnotationAndCreateCorrectTableDefinition() {

		CassandraPersistentEntity<?> persistentEntity = ctx.getRequiredPersistentEntity(Employee.class);

		CreateTableSpecification specification = ctx.getCreateTableSpecificationFor(persistentEntity);

		assertThat(getColumnType("floater", specification)).isEqualTo(DataTypes.FLOAT);

		ColumnSpecification enemies = getColumn("enemies", specification);
		assertThat(enemies.getType()).isInstanceOf(SetType.class);

		SetType enemiesCollection = (SetType) enemies.getType();
		assertThat(enemiesCollection.getProtocolCode()).isEqualTo(ProtocolConstants.DataType.SET);
		assertThat(enemiesCollection.getElementType()).isEqualTo(DataTypes.BIGINT);
	}

	@Test // DATACASS-296
	public void columnsShouldMapToVarchar() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(AllPossibleTypes.class);

		assertThat(getColumnType("id", specification)).isEqualTo(DataTypes.TEXT);
		assertThat(getColumnType("zoneId", specification)).isEqualTo(DataTypes.TEXT);
		assertThat(getColumnType("bpZoneId", specification)).isEqualTo(DataTypes.TEXT);
		assertThat(getColumnType("anEnum", specification)).isEqualTo(DataTypes.TEXT);
	}

	@Test // DATACASS-296
	public void columnsShouldMapToTinyInt() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(AllPossibleTypes.class);

		assertThat(getColumnType("boxedByte", specification)).isEqualTo(DataTypes.TINYINT);
		assertThat(getColumnType("primitiveByte", specification)).isEqualTo(DataTypes.TINYINT);
	}

	@Test // DATACASS-296
	public void columnsShouldMapToSmallInt() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(AllPossibleTypes.class);

		assertThat(getColumnType("boxedShort", specification)).isEqualTo(DataTypes.SMALLINT);
		assertThat(getColumnType("primitiveShort", specification)).isEqualTo(DataTypes.SMALLINT);
	}

	@Test // DATACASS-296
	public void columnsShouldMapToBigInt() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(AllPossibleTypes.class);

		assertThat(getColumnType("boxedLong", specification)).isEqualTo(DataTypes.BIGINT);
		assertThat(getColumnType("primitiveLong", specification)).isEqualTo(DataTypes.BIGINT);
	}

	@Test // DATACASS-296
	public void columnsShouldMapToVarInt() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(AllPossibleTypes.class);

		assertThat(getColumnType("bigInteger", specification)).isEqualTo(DataTypes.VARINT);
	}

	@Test // DATACASS-296
	public void columnsShouldMapToDecimal() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(AllPossibleTypes.class);

		assertThat(getColumnType("bigDecimal", specification)).isEqualTo(DataTypes.DECIMAL);
	}

	@Test // DATACASS-296
	public void columnsShouldMapToInt() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(AllPossibleTypes.class);

		assertThat(getColumnType("boxedInteger", specification)).isEqualTo(DataTypes.INT);
		assertThat(getColumnType("primitiveInteger", specification)).isEqualTo(DataTypes.INT);
	}

	@Test // DATACASS-296
	public void columnsShouldMapToFloat() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(AllPossibleTypes.class);

		assertThat(getColumnType("boxedFloat", specification)).isEqualTo(DataTypes.FLOAT);
		assertThat(getColumnType("primitiveFloat", specification)).isEqualTo(DataTypes.FLOAT);
	}

	@Test // DATACASS-296
	public void columnsShouldMapToDouble() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(AllPossibleTypes.class);

		assertThat(getColumnType("boxedDouble", specification)).isEqualTo(DataTypes.DOUBLE);
		assertThat(getColumnType("primitiveDouble", specification)).isEqualTo(DataTypes.DOUBLE);
	}

	@Test // DATACASS-296
	public void columnsShouldMapToBoolean() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(AllPossibleTypes.class);

		assertThat(getColumnType("boxedBoolean", specification)).isEqualTo(DataTypes.BOOLEAN);
		assertThat(getColumnType("primitiveBoolean", specification)).isEqualTo(DataTypes.BOOLEAN);
	}

	@Test // DATACASS-296
	public void columnsShouldMapToDate() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(AllPossibleTypes.class);

		assertThat(getColumnType("date", specification)).isEqualTo(DataTypes.DATE);
		assertThat(getColumnType("localDate", specification)).isEqualTo(DataTypes.DATE);
		assertThat(getColumnType("jodaLocalDate", specification)).isEqualTo(DataTypes.DATE);
		assertThat(getColumnType("bpLocalDate", specification)).isEqualTo(DataTypes.DATE);
	}

	@Test // DATACASS-296
	public void columnsShouldMapToTimestamp() {

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
	public void columnsShouldMapToTimestampUsingOverrides() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(TypeWithOverrides.class);

		assertThat(getColumnType("localDate", specification)).isEqualTo(DataTypes.TIMESTAMP);
		assertThat(getColumnType("jodaLocalDate", specification)).isEqualTo(DataTypes.TIMESTAMP);
	}

	@Test // DATACASS-296
	public void columnsShouldMapToBlob() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(AllPossibleTypes.class);

		assertThat(getColumnType("blob", specification)).isEqualTo(DataTypes.BLOB);
	}

	@Test // DATACASS-172
	public void columnsShouldMapToUdt() {

		UserDefinedType human_udt = mock(UserDefinedType.class, "human_udt");
		UserDefinedType species_udt = mock(UserDefinedType.class, "species_udt");
		UserDefinedType peeps_udt = mock(UserDefinedType.class, "peeps_udt");

		ctx.setUserTypeResolver(typeName -> {

			if (typeName.toString().equals(human_udt.toString())) {
				return human_udt;
			}

			if (typeName.toString().equals(species_udt.toString())) {
				return species_udt;
			}

			if (typeName.toString().equals(peeps_udt.toString())) {
				return peeps_udt;
			}
			return null;
		});

		CreateTableSpecification specification = getCreateTableSpecificationFor(WithUdtFields.class);

		assertThat(getColumnType("human", specification)).isEqualTo(human_udt);
		assertThat(getColumnType("friends", specification)).isEqualTo(DataTypes.listOf(species_udt));
		assertThat(getColumnType("people", specification)).isEqualTo(DataTypes.setOf(peeps_udt));
	}

	@Test // DATACASS-172
	public void columnsShouldMapToMappedUserType() {

		UserDefinedType mappedUdt = mock(UserDefinedType.class, "mappedudt");

		ctx.setUserTypeResolver(typeName -> {

			if (typeName.toString().equals(mappedUdt.toString())) {
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
	public void columnsShouldMapToTuple() {

		UserDefinedType mappedUdt = mock(UserDefinedType.class, "mappedudt");
		UserDefinedType human_udt = mock(UserDefinedType.class, "human_udt");

		when(mappedUdt.toString()).thenReturn("mappedudt");
		when(human_udt.toString()).thenReturn("human_udt");

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
	public void createTableSpecificationShouldConsiderCustomTableName() {

		CqlIdentifier customTableName = CqlIdentifier.fromCql("my_custom_came");

		CassandraPersistentEntity<?> persistentEntity = ctx.getRequiredPersistentEntity(Employee.class);
		CreateTableSpecification specification = ctx.getCreateTableSpecificationFor(customTableName, persistentEntity);

		assertThat(specification).isNotNull();
		assertThat(specification.getName()).isEqualTo(customTableName);
	}

	private CreateTableSpecification getCreateTableSpecificationFor(Class<?> persistentEntityClass) {

		CassandraCustomConversions customConversions = new CassandraCustomConversions(Collections.emptyList());
		ctx.setCustomConversions(customConversions);

		CassandraPersistentEntity<?> persistentEntity = ctx.getRequiredPersistentEntity(persistentEntityClass);
		return ctx.getCreateTableSpecificationFor(persistentEntity);
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

		@CassandraType(type = CassandraSimpleTypeHolder.Name.FLOAT) Human floater;
		@CassandraType(type = CassandraSimpleTypeHolder.Name.SET,
				typeArguments = CassandraSimpleTypeHolder.Name.BIGINT) List<Human> enemies;
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
		@Element(1) @CassandraType(type = CassandraSimpleTypeHolder.Name.UDT, userTypeName = "human_udt") UdtValue human;
		@Element(2) String text;
	}

	@Data
	@Table
	private static class WithUdtFields {

		@Id String id;

		@CassandraType(type = CassandraSimpleTypeHolder.Name.UDT, userTypeName = "human_udt") UdtValue human;
		@CassandraType(type = CassandraSimpleTypeHolder.Name.LIST, typeArguments = CassandraSimpleTypeHolder.Name.UDT,
				userTypeName = "species_udt") List<UdtValue> friends;
		@CassandraType(type = CassandraSimpleTypeHolder.Name.SET, typeArguments = CassandraSimpleTypeHolder.Name.UDT,
				userTypeName = "peeps_udt") Set<UdtValue> people;
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

		@CassandraType(type = CassandraSimpleTypeHolder.Name.TIMESTAMP) java.time.LocalDate localDate;
		@CassandraType(type = CassandraSimpleTypeHolder.Name.TIMESTAMP) org.joda.time.LocalDate jodaLocalDate;
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
}
