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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import org.junit.Before;
import org.junit.Test;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.convert.CassandraCustomConversions;
import org.springframework.data.cassandra.core.cql.CqlIdentifier;
import org.springframework.data.cassandra.core.cql.keyspace.ColumnSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.CreateTableSpecification;
import org.springframework.data.cassandra.domain.AllPossibleTypes;
import org.springframework.util.StringUtils;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.DataType.CollectionType;
import com.datastax.driver.core.DataType.Name;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Unit tests for {@link CassandraMappingContext} targeted on {@link CreateTableSpecification}.
 *
 * @author Mark Paluch
 * @soundtrack Black Rose - Volbeat
 */
public class CreateTableSpecificationBasicCassandraMappingContextUnitTests {

	private CassandraMappingContext ctx = new CassandraMappingContext();

	@Before
	public void setUp() throws Exception {

		List<Converter<?, ?>> converters = new ArrayList<>();
		converters.add(new PersonReadConverter());
		converters.add(new PersonWriteConverter());

		CassandraCustomConversions customConversions = new CassandraCustomConversions(converters);
		ctx.setCustomConversions(customConversions);
		ctx.setTupleTypeFactory(types -> TupleType.of(ProtocolVersion.NEWEST_SUPPORTED, CodecRegistry.DEFAULT_INSTANCE,
				types.toArray(new DataType[0])));
	}

	@Test // DATACASS-296
	public void customConversionTestShouldCreateCorrectTableDefinition() {

		CassandraPersistentEntity<?> persistentEntity = ctx.getRequiredPersistentEntity(Employee.class);

		CreateTableSpecification specification = ctx.getCreateTableSpecificationFor(persistentEntity);

		assertThat(getColumnType("human", specification)).isEqualTo(DataType.varchar());

		ColumnSpecification friends = getColumn("friends", specification);
		assertThat(friends.getType().isCollection()).isTrue();

		CollectionType friendsCollection = (CollectionType) friends.getType();
		assertThat(friendsCollection.getName()).isEqualTo(Name.LIST);
		assertThat(friendsCollection.getTypeArguments()).hasSize(1);
		assertThat(friendsCollection.getTypeArguments().get(0)).isEqualTo(DataType.varchar());

		ColumnSpecification people = getColumn("people", specification);
		assertThat(people.getType().isCollection()).isTrue();

		CollectionType peopleCollection = (CollectionType) people.getType();
		assertThat(peopleCollection.getName()).isEqualTo(Name.SET);
		assertThat(peopleCollection.getTypeArguments()).hasSize(1);
		assertThat(peopleCollection.getTypeArguments().get(0)).isEqualTo(DataType.varchar());
	}

	@Test // DATACASS-296
	public void customConversionTestShouldHonorTypeAnnotationAndCreateCorrectTableDefinition() {

		CassandraPersistentEntity<?> persistentEntity = ctx.getRequiredPersistentEntity(Employee.class);

		CreateTableSpecification specification = ctx.getCreateTableSpecificationFor(persistentEntity);

		assertThat(getColumnType("floater", specification)).isEqualTo(DataType.cfloat());

		ColumnSpecification enemies = getColumn("enemies", specification);
		assertThat(enemies.getType().isCollection()).isTrue();

		CollectionType enemiesCollection = (CollectionType) enemies.getType();
		assertThat(enemiesCollection.getName()).isEqualTo(Name.SET);
		assertThat(enemiesCollection.getTypeArguments()).hasSize(1);
		assertThat(enemiesCollection.getTypeArguments().get(0)).isEqualTo(DataType.bigint());
	}

	@Test // DATACASS-296
	public void columnsShouldMapToVarchar() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(AllPossibleTypes.class);

		assertThat(getColumnType("id", specification)).isEqualTo(DataType.varchar());
		assertThat(getColumnType("zoneId", specification)).isEqualTo(DataType.varchar());
		assertThat(getColumnType("bpZoneId", specification)).isEqualTo(DataType.varchar());
		assertThat(getColumnType("anEnum", specification)).isEqualTo(DataType.varchar());
	}

	@Test // DATACASS-296
	public void columnsShouldMapToTinyInt() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(AllPossibleTypes.class);

		assertThat(getColumnType("boxedByte", specification)).isEqualTo(DataType.tinyint());
		assertThat(getColumnType("primitiveByte", specification)).isEqualTo(DataType.tinyint());
	}

	@Test // DATACASS-296
	public void columnsShouldMapToSmallInt() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(AllPossibleTypes.class);

		assertThat(getColumnType("boxedShort", specification)).isEqualTo(DataType.smallint());
		assertThat(getColumnType("primitiveShort", specification)).isEqualTo(DataType.smallint());
	}

	@Test // DATACASS-296
	public void columnsShouldMapToBigInt() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(AllPossibleTypes.class);

		assertThat(getColumnType("boxedLong", specification)).isEqualTo(DataType.bigint());
		assertThat(getColumnType("primitiveLong", specification)).isEqualTo(DataType.bigint());
	}

	@Test // DATACASS-296
	public void columnsShouldMapToVarInt() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(AllPossibleTypes.class);

		assertThat(getColumnType("bigInteger", specification)).isEqualTo(DataType.varint());
	}

	@Test // DATACASS-296
	public void columnsShouldMapToDecimal() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(AllPossibleTypes.class);

		assertThat(getColumnType("bigDecimal", specification)).isEqualTo(DataType.decimal());
	}

	@Test // DATACASS-296
	public void columnsShouldMapToInt() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(AllPossibleTypes.class);

		assertThat(getColumnType("boxedInteger", specification)).isEqualTo(DataType.cint());
		assertThat(getColumnType("primitiveInteger", specification)).isEqualTo(DataType.cint());
	}

	@Test // DATACASS-296
	public void columnsShouldMapToFloat() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(AllPossibleTypes.class);

		assertThat(getColumnType("boxedFloat", specification)).isEqualTo(DataType.cfloat());
		assertThat(getColumnType("primitiveFloat", specification)).isEqualTo(DataType.cfloat());
	}

	@Test // DATACASS-296
	public void columnsShouldMapToDouble() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(AllPossibleTypes.class);

		assertThat(getColumnType("boxedDouble", specification)).isEqualTo(DataType.cdouble());
		assertThat(getColumnType("primitiveDouble", specification)).isEqualTo(DataType.cdouble());
	}

	@Test // DATACASS-296
	public void columnsShouldMapToBoolean() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(AllPossibleTypes.class);

		assertThat(getColumnType("boxedBoolean", specification)).isEqualTo(DataType.cboolean());
		assertThat(getColumnType("primitiveBoolean", specification)).isEqualTo(DataType.cboolean());
	}

	@Test // DATACASS-296
	public void columnsShouldMapToDate() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(AllPossibleTypes.class);

		assertThat(getColumnType("date", specification)).isEqualTo(DataType.date());
		assertThat(getColumnType("localDate", specification)).isEqualTo(DataType.date());
		assertThat(getColumnType("jodaLocalDate", specification)).isEqualTo(DataType.date());
		assertThat(getColumnType("bpLocalDate", specification)).isEqualTo(DataType.date());
	}

	@Test // DATACASS-296
	public void columnsShouldMapToTimestamp() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(AllPossibleTypes.class);

		assertThat(getColumnType("timestamp", specification)).isEqualTo(DataType.timestamp());
		assertThat(getColumnType("localDateTime", specification)).isEqualTo(DataType.timestamp());
		assertThat(getColumnType("instant", specification)).isEqualTo(DataType.timestamp());
		assertThat(getColumnType("jodaLocalDateTime", specification)).isEqualTo(DataType.timestamp());
		assertThat(getColumnType("jodaDateTime", specification)).isEqualTo(DataType.timestamp());
		assertThat(getColumnType("bpLocalDateTime", specification)).isEqualTo(DataType.timestamp());
		assertThat(getColumnType("bpInstant", specification)).isEqualTo(DataType.timestamp());
	}

	@Test // DATACASS-296
	public void columnsShouldMapToTimestampUsingOverrides() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(TypeWithOverrides.class);

		assertThat(getColumnType("localDate", specification)).isEqualTo(DataType.timestamp());
		assertThat(getColumnType("jodaLocalDate", specification)).isEqualTo(DataType.timestamp());
	}

	@Test // DATACASS-296
	public void columnsShouldMapToBlob() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(AllPossibleTypes.class);

		assertThat(getColumnType("blob", specification)).isEqualTo(DataType.blob());
	}

	@Test // DATACASS-172
	public void columnsShouldMapToUdt() {

		final UserType human_udt = mock(UserType.class, "human_udt");
		final UserType species_udt = mock(UserType.class, "species_udt");
		final UserType peeps_udt = mock(UserType.class, "peeps_udt");

		ctx.setUserTypeResolver(typeName -> {

			if (typeName.toCql().equals(human_udt.toString())) {
				return human_udt;
			}

			if (typeName.toCql().equals(species_udt.toString())) {
				return species_udt;
			}

			if (typeName.toCql().equals(peeps_udt.toString())) {
				return peeps_udt;
			}
			return null;
		});

		CreateTableSpecification specification = getCreateTableSpecificationFor(WithUdtFields.class);

		assertThat(getColumnType("human", specification)).isEqualTo(human_udt);
		assertThat(getColumnType("friends", specification)).isEqualTo(DataType.list(species_udt));
		assertThat(getColumnType("people", specification)).isEqualTo(DataType.set(peeps_udt));
	}

	@Test // DATACASS-172
	public void columnsShouldMapToMappedUserType() {

		final UserType mappedUdt = mock(UserType.class, "mappedudt");

		ctx.setUserTypeResolver(typeName -> {

			if (typeName.toCql().equals(mappedUdt.toString())) {
				return mappedUdt;
			}
			return null;
		});

		CreateTableSpecification specification = getCreateTableSpecificationFor(WithMappedUdtFields.class);

		assertThat(getColumnType("human", specification)).isEqualTo(mappedUdt);
		assertThat(getColumnType("friends", specification)).isEqualTo(DataType.list(mappedUdt));
		assertThat(getColumnType("people", specification)).isEqualTo(DataType.set(mappedUdt));
		assertThat(getColumnType("stringToUdt", specification)).isEqualTo(DataType.map(DataType.varchar(), mappedUdt));
		assertThat(getColumnType("udtToString", specification)).isEqualTo(DataType.map(mappedUdt, DataType.varchar()));
	}

	@Test // DATACASS-523
	public void columnsShouldMapToTuple() {

		UserType mappedUdt = mock(UserType.class, "mappedudt");
		UserType human_udt = mock(UserType.class, "human_udt");

		when(mappedUdt.asFunctionParameterString()).thenReturn("mappedudt");
		when(human_udt.asFunctionParameterString()).thenReturn("human_udt");

		ctx.setUserTypeResolver(typeName -> {

			if (typeName.toCql().equals(mappedUdt.toString())) {
				return mappedUdt;
			}

			if (typeName.toCql().equals(human_udt.toString())) {
				return human_udt;
			}
			return null;
		});

		CreateTableSpecification specification = getCreateTableSpecificationFor(WithMappedTuple.class);

		assertThat(getColumnType("mappedTuple", specification).toString())
				.isEqualTo("frozen<tuple<mappedudt, human_udt, text>>");

		assertThat(getColumnType("mappedTuples", specification).toString())
				.isEqualTo("list<frozen<tuple<mappedudt, human_udt, text>>>");
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
			if (columnSpecification.getName().equals(CqlIdentifier.of(columnName))) {
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
		@Element(1) @CassandraType(type = Name.UDT, userTypeName = "human_udt") UDTValue human;
		@Element(2) String text;
	}

	@Data
	@Table
	private static class WithUdtFields {

		@Id String id;

		@CassandraType(type = Name.UDT, userTypeName = "human_udt") UDTValue human;
		@CassandraType(type = Name.LIST, typeArguments = Name.UDT, userTypeName = "species_udt") List<UDTValue> friends;
		@CassandraType(type = Name.SET, typeArguments = Name.UDT, userTypeName = "peeps_udt") Set<UDTValue> people;
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

	@UserDefinedType
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
}
