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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.springframework.cassandra.core.cql.CqlIdentifier;
import org.springframework.cassandra.core.keyspace.ColumnSpecification;
import org.springframework.cassandra.core.keyspace.CreateTableSpecification;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.convert.CustomConversions;
import org.springframework.data.cassandra.domain.AllPossibleTypes;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.util.StringUtils;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.DataType.CollectionType;
import com.datastax.driver.core.DataType.Name;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Unit tests for {@link BasicCassandraMappingContext} targeted on {@link CreateTableSpecification}.
 *
 * @author Mark Paluch
 * @soundtrack Black Rose - Volbeat
 */
public class CreateTableSpecificationBasicCassandraMappingContextUnitTests {

	BasicCassandraMappingContext ctx = new BasicCassandraMappingContext();

	@Before
	public void setUp() throws Exception {

		List<Converter<?, ?>> converters = new ArrayList<Converter<?, ?>>();
		converters.add(new PersonReadConverter());
		converters.add(new PersonWriteConverter());

		CustomConversions customConversions = new CustomConversions(converters);
		ctx.setCustomConversions(customConversions);
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void customConversionTestShouldCreateCorrectTableDefinition() {

		CassandraPersistentEntity<?> persistentEntity = ctx.getPersistentEntity(Employee.class);

		CreateTableSpecification specification = ctx.getCreateTableSpecificationFor(persistentEntity);

		assertThat(getColumn("human", specification).getType(), is(DataType.varchar()));

		ColumnSpecification friends = getColumn("friends", specification);
		assertThat(friends.getType().isCollection(), is(true));

		CollectionType friendsCollection = (CollectionType) friends.getType();
		assertThat(friendsCollection.getName(), is(Name.LIST));
		assertThat(friendsCollection.getTypeArguments().size(), is(1));
		assertThat(friendsCollection.getTypeArguments().get(0), is(DataType.varchar()));

		ColumnSpecification people = getColumn("people", specification);
		assertThat(people.getType().isCollection(), is(true));

		CollectionType peopleCollection = (CollectionType) people.getType();
		assertThat(peopleCollection.getName(), is(Name.SET));
		assertThat(peopleCollection.getTypeArguments().size(), is(1));
		assertThat(peopleCollection.getTypeArguments().get(0), is(DataType.varchar()));
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void customConversionTestShouldHonorTypeAnnotationAndCreateCorrectTableDefinition() {

		CassandraPersistentEntity<?> persistentEntity = ctx.getPersistentEntity(Employee.class);

		CreateTableSpecification specification = ctx.getCreateTableSpecificationFor(persistentEntity);

		assertThat(getColumn("floater", specification).getType(), is(DataType.cfloat()));

		ColumnSpecification enemies = getColumn("enemies", specification);
		assertThat(enemies.getType().isCollection(), is(true));

		CollectionType enemiesCollection = (CollectionType) enemies.getType();
		assertThat(enemiesCollection.getName(), is(Name.SET));
		assertThat(enemiesCollection.getTypeArguments().size(), is(1));
		assertThat(enemiesCollection.getTypeArguments().get(0), is(DataType.bigint()));
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void columnsShouldMapToVarchar() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(AllPossibleTypes.class);

		assertThat(getColumn("id", specification).getType(), is(DataType.varchar()));
		assertThat(getColumn("zoneId", specification).getType(), is(DataType.varchar()));
		assertThat(getColumn("bpZoneId", specification).getType(), is(DataType.varchar()));
		assertThat(getColumn("anEnum", specification).getType(), is(DataType.varchar()));
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void columnsShouldMapToTinyInt() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(AllPossibleTypes.class);

		assertThat(getColumn("boxedByte", specification).getType(), is(DataType.tinyint()));
		assertThat(getColumn("primitiveByte", specification).getType(), is(DataType.tinyint()));
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void columnsShouldMapToSmallInt() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(AllPossibleTypes.class);

		assertThat(getColumn("boxedShort", specification).getType(), is(DataType.smallint()));
		assertThat(getColumn("primitiveShort", specification).getType(), is(DataType.smallint()));
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void columnsShouldMapToBigInt() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(AllPossibleTypes.class);

		assertThat(getColumn("boxedLong", specification).getType(), is(DataType.bigint()));
		assertThat(getColumn("primitiveLong", specification).getType(), is(DataType.bigint()));
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void columnsShouldMapToVarInt() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(AllPossibleTypes.class);

		assertThat(getColumn("bigInteger", specification).getType(), is(DataType.varint()));
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void columnsShouldMapToDecimal() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(AllPossibleTypes.class);

		assertThat(getColumn("bigDecimal", specification).getType(), is(DataType.decimal()));
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void columnsShouldMapToInt() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(AllPossibleTypes.class);

		assertThat(getColumn("boxedInteger", specification).getType(), is(DataType.cint()));
		assertThat(getColumn("primitiveInteger", specification).getType(), is(DataType.cint()));
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void columnsShouldMapToFloat() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(AllPossibleTypes.class);

		assertThat(getColumn("boxedFloat", specification).getType(), is(DataType.cfloat()));
		assertThat(getColumn("primitiveFloat", specification).getType(), is(DataType.cfloat()));
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void columnsShouldMapToDouble() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(AllPossibleTypes.class);

		assertThat(getColumn("boxedDouble", specification).getType(), is(DataType.cdouble()));
		assertThat(getColumn("primitiveDouble", specification).getType(), is(DataType.cdouble()));
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void columnsShouldMapToBoolean() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(AllPossibleTypes.class);

		assertThat(getColumn("boxedBoolean", specification).getType(), is(DataType.cboolean()));
		assertThat(getColumn("primitiveBoolean", specification).getType(), is(DataType.cboolean()));
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void columnsShouldMapToDate() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(AllPossibleTypes.class);

		assertThat(getColumn("date", specification).getType(), is(DataType.date()));
		assertThat(getColumn("localDate", specification).getType(), is(DataType.date()));
		assertThat(getColumn("jodaLocalDate", specification).getType(), is(DataType.date()));
		assertThat(getColumn("jodaDateMidnight", specification).getType(), is(DataType.date()));
		assertThat(getColumn("bpLocalDate", specification).getType(), is(DataType.date()));
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void columnsShouldMapToTimestamp() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(AllPossibleTypes.class);

		assertThat(getColumn("timestamp", specification).getType(), is(DataType.timestamp()));
		assertThat(getColumn("localDateTime", specification).getType(), is(DataType.timestamp()));
		assertThat(getColumn("instant", specification).getType(), is(DataType.timestamp()));
		assertThat(getColumn("jodaLocalDateTime", specification).getType(), is(DataType.timestamp()));
		assertThat(getColumn("jodaDateTime", specification).getType(), is(DataType.timestamp()));
		assertThat(getColumn("bpLocalDateTime", specification).getType(), is(DataType.timestamp()));
		assertThat(getColumn("bpInstant", specification).getType(), is(DataType.timestamp()));
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void columnsShouldMapToTimestampUsingOverrides() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(TypeWithOverrides.class);

		assertThat(getColumn("localDate", specification).getType(), is(DataType.timestamp()));
		assertThat(getColumn("jodaLocalDate", specification).getType(), is(DataType.timestamp()));
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void columnsShouldMapToBlob() {

		CreateTableSpecification specification = getCreateTableSpecificationFor(AllPossibleTypes.class);

		assertThat(getColumn("blob", specification).getType(), is(DataType.blob()));
	}

	public CreateTableSpecification getCreateTableSpecificationFor(Class<?> persistentEntityClass) {

		CustomConversions customConversions = new CustomConversions(Collections.EMPTY_LIST);
		ctx.setCustomConversions(customConversions);

		CassandraPersistentEntity<?> persistentEntity = ctx.getPersistentEntity(persistentEntityClass);
		return ctx.getCreateTableSpecificationFor(persistentEntity);
	}

	private ColumnSpecification getColumn(String columnName, CreateTableSpecification specification) {

		for (ColumnSpecification columnSpecification : specification.getColumns()) {
			if (columnSpecification.getName().equals(CqlIdentifier.cqlId(columnName))) {
				return columnSpecification;
			}
		}

		throw new IllegalArgumentException(
				String.format("Cannot find column '%s' amongst '%s'", columnName, specification.getColumns()));
	}

	/**
	 * @author Mark Paluch
	 */
	@Data
	@Table
	public static class Employee {

		@Id String id;

		Human human;
		List<Human> friends;
		Set<Human> people;

		@CassandraType(type = Name.FLOAT) Human floater;
		@CassandraType(type = Name.SET, typeArguments = Name.BIGINT) List<Human> enemies;
	}

	/**
	 * @author Mark Paluch
	 */
	@Data
	@AllArgsConstructor
	@NoArgsConstructor
	static class Human {

		String firstname;
		String lastname;
	}

	/**
	 * @author Mark Paluch
	 */
	@Data
	@Table
	static class TypeWithOverrides {

		@Id String id;

		@CassandraType(type = Name.TIMESTAMP) java.time.LocalDate localDate;

		@CassandraType(type = Name.TIMESTAMP) org.joda.time.LocalDate jodaLocalDate;
	}

	static class PersonReadConverter implements Converter<String, Human> {

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

	static class PersonWriteConverter implements Converter<Human, String> {

		public String convert(Human source) {

			try {
				return new ObjectMapper().writeValueAsString(source);
			} catch (IOException e) {
				throw new IllegalStateException(e);
			}
		}
	}
}
