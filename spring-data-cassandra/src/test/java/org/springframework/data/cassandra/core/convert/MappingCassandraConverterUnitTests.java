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
package org.springframework.data.cassandra.core.convert;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assume.assumeTrue;
import static org.springframework.data.cassandra.core.mapping.BasicMapId.id;
import static org.springframework.data.cassandra.test.util.RowMockUtil.column;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.Value;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.springframework.core.SpringVersion;
import org.springframework.core.convert.ConverterNotFoundException;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.BasicMapId;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.cassandra.core.mapping.MapId;
import org.springframework.data.cassandra.core.mapping.PrimaryKey;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyClass;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.core.mapping.Table;
import org.springframework.data.cassandra.domain.CompositeKey;
import org.springframework.data.cassandra.domain.TypeWithCompositeKey;
import org.springframework.data.cassandra.domain.TypeWithKeyClass;
import org.springframework.data.cassandra.domain.TypeWithMapId;
import org.springframework.data.cassandra.domain.User;
import org.springframework.data.cassandra.domain.UserToken;
import org.springframework.data.cassandra.test.util.RowMockUtil;
import org.springframework.data.util.Version;
import org.springframework.test.util.ReflectionTestUtils;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.DataType.Name;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.Assignment;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Delete.Where;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;
import com.datastax.driver.core.querybuilder.Update.Assignments;

/**
 * Unit tests for {@link MappingCassandraConverter}.
 *
 * @author Mark Paluch
 * @soundtrack Outlandich - Dont Leave Me Feat Cyt (Sun Kidz Electrocore Mix)
 */
public class MappingCassandraConverterUnitTests {

	private static final Version VERSION_4_3 = Version.parse("4.3");

	@Rule public final ExpectedException expectedException = ExpectedException.none();

	Row rowMock;

	CassandraMappingContext mappingContext;
	MappingCassandraConverter mappingCassandraConverter;

	@Before
	public void setUp() throws Exception {

		this.mappingContext = new CassandraMappingContext();

		this.mappingCassandraConverter = new MappingCassandraConverter(mappingContext);
		this.mappingCassandraConverter.afterPropertiesSet();
	}

	@Test // DATACASS-260
	public void insertEnumShouldMapToString() {

		WithEnumColumns withEnumColumns = new WithEnumColumns();

		withEnumColumns.setCondition(Condition.MINT);

		Insert insert = QueryBuilder.insertInto("table");

		mappingCassandraConverter.write(withEnumColumns, insert);

		assertThat(getValues(insert)).contains("MINT");
	}

	@Test // DATACASS-260
	public void insertEnumDoesNotMapToOrdinalBeforeSpring43() {

		assumeTrue(Version.parse(SpringVersion.getVersion()).isLessThan(VERSION_4_3));

		expectedException.expect(ConverterNotFoundException.class);

		UnsupportedEnumToOrdinalMapping unsupportedEnumToOrdinalMapping = new UnsupportedEnumToOrdinalMapping();
		unsupportedEnumToOrdinalMapping.setAsOrdinal(Condition.MINT);

		Insert insert = QueryBuilder.insertInto("table");

		mappingCassandraConverter.write(unsupportedEnumToOrdinalMapping, insert);
	}

	@Test // DATACASS-255
	public void insertEnumMapsToOrdinalWithSpring43AndHiger() {

		assumeTrue(Version.parse(SpringVersion.getVersion()).isGreaterThanOrEqualTo(VERSION_4_3));

		UnsupportedEnumToOrdinalMapping unsupportedEnumToOrdinalMapping = new UnsupportedEnumToOrdinalMapping();
		unsupportedEnumToOrdinalMapping.setAsOrdinal(Condition.USED);

		Insert insert = QueryBuilder.insertInto("table");

		mappingCassandraConverter.write(unsupportedEnumToOrdinalMapping, insert);

		assertThat(getValues(insert)).contains((Object) Integer.valueOf(Condition.USED.ordinal()));
	}

	@Test // DATACASS-260
	public void insertEnumAsPrimaryKeyShouldMapToString() {

		EnumPrimaryKey key = new EnumPrimaryKey();
		key.setCondition(Condition.MINT);

		Insert insert = QueryBuilder.insertInto("table");

		mappingCassandraConverter.write(key, insert);

		assertThat(getValues(insert)).contains((Object) "MINT");
	}

	@Test // DATACASS-260
	public void insertEnumInCompositePrimaryKeyShouldMapToString() {

		EnumCompositePrimaryKey key = new EnumCompositePrimaryKey();
		key.setCondition(Condition.MINT);

		CompositeKeyThing composite = new CompositeKeyThing();
		composite.setKey(key);

		Insert insert = QueryBuilder.insertInto("table");

		mappingCassandraConverter.write(composite, insert);

		assertThat(getValues(insert)).contains((Object) "MINT");
	}

	@Test // DATACASS-260
	public void updateEnumShouldMapToString() {

		WithEnumColumns withEnumColumns = new WithEnumColumns();
		withEnumColumns.setCondition(Condition.MINT);

		Update update = QueryBuilder.update("table");

		mappingCassandraConverter.write(withEnumColumns, update);

		assertThat(getAssignmentValues(update)).contains((Object) "MINT");
	}

	@Test // DATACASS-260
	public void updateEnumAsPrimaryKeyShouldMapToString() {

		EnumPrimaryKey key = new EnumPrimaryKey();
		key.setCondition(Condition.MINT);

		Update update = QueryBuilder.update("table");

		mappingCassandraConverter.write(key, update);

		assertThat(getWhereValues(update)).contains((Object) "MINT");
	}

	@Test // DATACASS-260
	public void updateEnumInCompositePrimaryKeyShouldMapToString() {

		EnumCompositePrimaryKey key = new EnumCompositePrimaryKey();
		key.setCondition(Condition.MINT);

		CompositeKeyThing composite = new CompositeKeyThing();
		composite.setKey(key);

		Update update = QueryBuilder.update("table");

		mappingCassandraConverter.write(composite, update);

		assertThat(getWhereValues(update)).contains((Object) "MINT");
	}

	@Test // DATACASS-260
	public void whereEnumAsPrimaryKeyShouldMapToString() {

		EnumPrimaryKey key = new EnumPrimaryKey();
		key.setCondition(Condition.MINT);

		Where where = QueryBuilder.delete().from("table").where();

		mappingCassandraConverter.write(key, where);

		assertThat(getWhereValues(where)).contains((Object) "MINT");
	}

	@Test // DATACASS-260
	public void whereEnumInCompositePrimaryKeyShouldMapToString() {

		EnumCompositePrimaryKey key = new EnumCompositePrimaryKey();
		key.setCondition(Condition.MINT);

		CompositeKeyThing composite = new CompositeKeyThing();
		composite.setKey(key);

		Where where = QueryBuilder.delete().from("table").where();

		mappingCassandraConverter.write(composite, where);

		assertThat(getWhereValues(where)).contains((Object) "MINT");
	}

	@Test // DATACASS-280
	public void shouldReadStringCorrectly() {

		rowMock = RowMockUtil.newRowMock(column("foo", "foo", DataType.varchar()));

		String result = mappingCassandraConverter.readRow(String.class, rowMock);

		assertThat(result).isEqualTo("foo");
	}

	@Test // DATACASS-280
	public void shouldReadIntegerCorrectly() {

		rowMock = RowMockUtil.newRowMock(column("foo", 2, DataType.varint()));

		Integer result = mappingCassandraConverter.readRow(Integer.class, rowMock);

		assertThat(result).isEqualTo(2);
	}

	@Test // DATACASS-280
	public void shouldReadLongCorrectly() {

		rowMock = RowMockUtil.newRowMock(column("foo", 2, DataType.varint()));

		Long result = mappingCassandraConverter.readRow(Long.class, rowMock);

		assertThat(result).isEqualTo(2L);
	}

	@Test // DATACASS-280
	public void shouldReadDoubleCorrectly() {

		rowMock = RowMockUtil.newRowMock(column("foo", 2D, DataType.cdouble()));

		Double result = mappingCassandraConverter.readRow(Double.class, rowMock);

		assertThat(result).isEqualTo(2D);
	}

	@Test // DATACASS-280
	public void shouldReadFloatCorrectly() {

		rowMock = RowMockUtil.newRowMock(column("foo", 2F, DataType.cdouble()));

		Float result = mappingCassandraConverter.readRow(Float.class, rowMock);

		assertThat(result).isEqualTo(2F);
	}

	@Test // DATACASS-280
	public void shouldReadBigIntegerCorrectly() {

		rowMock = RowMockUtil.newRowMock(column("foo", BigInteger.valueOf(2), DataType.bigint()));

		BigInteger result = mappingCassandraConverter.readRow(BigInteger.class, rowMock);

		assertThat(result).isEqualTo(BigInteger.valueOf(2));
	}

	@Test // DATACASS-280
	public void shouldReadBigDecimalCorrectly() {

		rowMock = RowMockUtil.newRowMock(column("foo", BigDecimal.valueOf(2), DataType.decimal()));

		BigDecimal result = mappingCassandraConverter.readRow(BigDecimal.class, rowMock);

		assertThat(result).isEqualTo(BigDecimal.valueOf(2));
	}

	@Test // DATACASS-280
	public void shouldReadUUIDCorrectly() {

		UUID uuid = UUID.randomUUID();

		rowMock = RowMockUtil.newRowMock(column("foo", uuid, DataType.uuid()));

		UUID result = mappingCassandraConverter.readRow(UUID.class, rowMock);

		assertThat(result).isEqualTo(uuid);
	}

	@Test // DATACASS-280
	public void shouldReadInetAddressCorrectly() throws UnknownHostException {

		InetAddress localHost = InetAddress.getLocalHost();

		rowMock = RowMockUtil.newRowMock(column("foo", localHost, DataType.inet()));

		InetAddress result = mappingCassandraConverter.readRow(InetAddress.class, rowMock);

		assertThat(result).isEqualTo(localHost);
	}

	@Test // DATACASS-280, DATACASS-271
	public void shouldReadTimestampCorrectly() {

		Date date = new Date(1);

		rowMock = RowMockUtil.newRowMock(column("foo", date, DataType.timestamp()));

		Date result = mappingCassandraConverter.readRow(Date.class, rowMock);

		assertThat(result).isEqualTo(date);
	}

	@Test // DATACASS-271
	public void shouldReadDateCorrectly() {

		LocalDate date = LocalDate.fromDaysSinceEpoch(1234);

		rowMock = RowMockUtil.newRowMock(column("foo", date, DataType.date()));

		LocalDate result = mappingCassandraConverter.readRow(LocalDate.class, rowMock);

		assertThat(result).isEqualTo(date);
	}

	@Test // DATACASS-280
	public void shouldReadBooleanCorrectly() {

		rowMock = RowMockUtil.newRowMock(column("foo", true, DataType.cboolean()));

		Boolean result = mappingCassandraConverter.readRow(Boolean.class, rowMock);

		assertThat(result).isEqualTo(true);
	}

	@Test // DATACASS-296
	public void shouldReadLocalDateCorrectly() {

		LocalDateTime now = LocalDateTime.now();
		Instant instant = now.toInstant(ZoneOffset.UTC);

		rowMock = RowMockUtil.newRowMock(column("id", "my-id", DataType.ascii()),
				column("localdate", Date.from(instant), DataType.timestamp()));

		TypeWithLocalDate result = mappingCassandraConverter.readRow(TypeWithLocalDate.class, rowMock);

		assertThat(result.localDate).isNotNull();
		assertThat(result.localDate.getYear()).isEqualTo(now.getYear());
		assertThat(result.localDate.getMonthValue()).isEqualTo(now.getMonthValue());
	}

	@Test // DATACASS-296
	public void shouldCreateInsertWithLocalDateCorrectly() {

		java.time.LocalDate now = java.time.LocalDate.now();

		TypeWithLocalDate typeWithLocalDate = new TypeWithLocalDate();
		typeWithLocalDate.localDate = now;

		Insert insert = QueryBuilder.insertInto("table");

		mappingCassandraConverter.write(typeWithLocalDate, insert);

		assertThat(getValues(insert))
				.contains(LocalDate.fromYearMonthDay(now.getYear(), now.getMonthValue(), now.getDayOfMonth()));
	}

	@Test // DATACASS-296
	public void shouldCreateUpdateWithLocalDateCorrectly() {

		java.time.LocalDate now = java.time.LocalDate.now();

		TypeWithLocalDate typeWithLocalDate = new TypeWithLocalDate();
		typeWithLocalDate.localDate = now;

		Update update = QueryBuilder.update("table");

		mappingCassandraConverter.write(typeWithLocalDate, update);

		assertThat(getAssignmentValues(update))
				.contains(LocalDate.fromYearMonthDay(now.getYear(), now.getMonthValue(), now.getDayOfMonth()));
	}

	@Test // DATACASS-296
	public void shouldCreateInsertWithLocalDateListUsingCassandraDate() {

		java.time.LocalDate now = java.time.LocalDate.now();
		java.time.LocalDate localDate = java.time.LocalDate.of(2010, 7, 4);

		TypeWithLocalDate typeWithLocalDate = new TypeWithLocalDate();
		typeWithLocalDate.list = Arrays.asList(now, localDate);

		Insert insert = QueryBuilder.insertInto("table");

		mappingCassandraConverter.write(typeWithLocalDate, insert);

		List<LocalDate> dates = getListValue(insert);

		assertThat(dates).contains(LocalDate.fromYearMonthDay(now.getYear(), now.getMonthValue(), now.getDayOfMonth()));
		assertThat(dates).contains(LocalDate.fromYearMonthDay(2010, 7, 4));
	}

	@Test // DATACASS-296
	public void shouldCreateInsertWithLocalDateSetUsingCassandraDate() {

		java.time.LocalDate now = java.time.LocalDate.now();
		java.time.LocalDate localDate = java.time.LocalDate.of(2010, 7, 4);

		TypeWithLocalDate typeWithLocalDate = new TypeWithLocalDate();
		typeWithLocalDate.set = new HashSet<>(Arrays.asList(now, localDate));

		Insert insert = QueryBuilder.insertInto("table");

		mappingCassandraConverter.write(typeWithLocalDate, insert);

		Set<LocalDate> dates = getSetValue(insert);

		assertThat(dates).contains(LocalDate.fromYearMonthDay(now.getYear(), now.getMonthValue(), now.getDayOfMonth()));
		assertThat(dates).contains(LocalDate.fromYearMonthDay(2010, 7, 4));
	}

	@Test // DATACASS-296
	public void shouldReadLocalDateTimeUsingCassandraDateCorrectly() {

		rowMock = RowMockUtil.newRowMock(column("id", "my-id", DataType.ascii()),
				column("localDate", LocalDate.fromYearMonthDay(2010, 7, 4), DataType.date()));

		TypeWithLocalDateMappedToDate result = mappingCassandraConverter.readRow(TypeWithLocalDateMappedToDate.class,
				rowMock);

		assertThat(result.localDate).isNotNull();
		assertThat(result.localDate.getYear()).isEqualTo(2010);
		assertThat(result.localDate.getMonthValue()).isEqualTo(7);
		assertThat(result.localDate.getDayOfMonth()).isEqualTo(4);
	}

	@Test // DATACASS-296, DATACASS-400
	public void shouldCreateInsertWithLocalDateUsingCassandraDateCorrectly() {

		TypeWithLocalDateMappedToDate typeWithLocalDate = new TypeWithLocalDateMappedToDate(null,
				java.time.LocalDate.of(2010, 7, 4));

		Insert insert = QueryBuilder.insertInto("table");

		mappingCassandraConverter.write(typeWithLocalDate, insert);

		assertThat(getValues(insert).contains(LocalDate.fromYearMonthDay(2010, 7, 4))).isTrue();
	}

	@Test // DATACASS-296
	public void shouldCreateUpdateWithLocalDateUsingCassandraDateCorrectly() {

		TypeWithLocalDateMappedToDate typeWithLocalDate = new TypeWithLocalDateMappedToDate(null,
				java.time.LocalDate.of(2010, 7, 4));

		Update update = QueryBuilder.update("table");

		mappingCassandraConverter.write(typeWithLocalDate, update);

		assertThat(getAssignmentValues(update)).contains(LocalDate.fromYearMonthDay(2010, 7, 4));
	}

	@Test // DATACASS-296
	public void shouldReadLocalDateTimeCorrectly() {

		LocalDateTime now = LocalDateTime.now();
		Instant instant = now.toInstant(ZoneOffset.UTC);

		rowMock = RowMockUtil.newRowMock(column("id", "my-id", DataType.ascii()),
				column("localDateTime", Date.from(instant), DataType.timestamp()));

		TypeWithLocalDate result = mappingCassandraConverter.readRow(TypeWithLocalDate.class, rowMock);

		assertThat(result.localDateTime).isNotNull();
		assertThat(result.localDateTime.getYear()).isEqualTo(now.getYear());
		assertThat(result.localDateTime.getMinute()).isEqualTo(now.getMinute());
	}

	@Test // DATACASS-296
	public void shouldReadInstantCorrectly() {

		LocalDateTime now = LocalDateTime.now();
		Instant instant = now.toInstant(ZoneOffset.UTC);

		rowMock = RowMockUtil.newRowMock(column("id", "my-id", DataType.ascii()),
				column("instant", Date.from(instant), DataType.timestamp()));

		TypeWithInstant result = mappingCassandraConverter.readRow(TypeWithInstant.class, rowMock);

		assertThat(result.instant).isNotNull();
		assertThat(result.instant.getEpochSecond()).isEqualTo(instant.getEpochSecond());
	}

	@Test // DATACASS-296
	public void shouldReadZoneIdCorrectly() {

		rowMock = RowMockUtil.newRowMock(column("id", "my-id", DataType.ascii()),
				column("zoneId", "Europe/Paris", DataType.varchar()));

		TypeWithZoneId result = mappingCassandraConverter.readRow(TypeWithZoneId.class, rowMock);

		assertThat(result.zoneId).isNotNull();
		assertThat(result.zoneId.getId()).isEqualTo("Europe/Paris");
	}

	@Test // DATACASS-296
	public void shouldReadJodaLocalDateTimeUsingCassandraDateCorrectly() {

		rowMock = RowMockUtil.newRowMock(column("id", "my-id", DataType.ascii()),
				column("localDate", LocalDate.fromYearMonthDay(2010, 7, 4), DataType.date()));

		TypeWithJodaLocalDateMappedToDate result = mappingCassandraConverter
				.readRow(TypeWithJodaLocalDateMappedToDate.class, rowMock);

		assertThat(result.localDate).isNotNull();
		assertThat(result.localDate.getYear()).isEqualTo(2010);
		assertThat(result.localDate.getMonthOfYear()).isEqualTo(7);
		assertThat(result.localDate.getDayOfMonth()).isEqualTo(4);
	}

	@Test // DATACASS-296
	public void shouldCreateInsertWithJodaLocalDateUsingCassandraDateCorrectly() {

		TypeWithJodaLocalDateMappedToDate typeWithLocalDate = new TypeWithJodaLocalDateMappedToDate();
		typeWithLocalDate.localDate = new org.joda.time.LocalDate(2010, 7, 4);

		Insert insert = QueryBuilder.insertInto("table");

		mappingCassandraConverter.write(typeWithLocalDate, insert);

		assertThat(getValues(insert).contains(LocalDate.fromYearMonthDay(2010, 7, 4))).isTrue();
	}

	@Test // DATACASS-296
	public void shouldCreateUpdateWithJodaLocalDateUsingCassandraDateCorrectly() {

		TypeWithJodaLocalDateMappedToDate typeWithLocalDate = new TypeWithJodaLocalDateMappedToDate();
		typeWithLocalDate.localDate = new org.joda.time.LocalDate(2010, 7, 4);

		Update update = QueryBuilder.update("table");

		mappingCassandraConverter.write(typeWithLocalDate, update);

		assertThat(getAssignmentValues(update)).contains(LocalDate.fromYearMonthDay(2010, 7, 4));
	}

	@Test // DATACASS-296
	public void shouldReadThreeTenBpLocalDateTimeUsingCassandraDateCorrectly() {

		rowMock = RowMockUtil.newRowMock(column("id", "my-id", DataType.ascii()),
				column("localDate", LocalDate.fromYearMonthDay(2010, 7, 4), DataType.date()));

		TypeWithThreeTenBpLocalDateMappedToDate result = mappingCassandraConverter
				.readRow(TypeWithThreeTenBpLocalDateMappedToDate.class, rowMock);

		assertThat(result.localDate).isNotNull();
		assertThat(result.localDate.getYear()).isEqualTo(2010);
		assertThat(result.localDate.getMonthValue()).isEqualTo(7);
		assertThat(result.localDate.getDayOfMonth()).isEqualTo(4);
	}

	@Test // DATACASS-296
	public void shouldCreateInsertWithThreeTenBpLocalDateUsingCassandraDateCorrectly() {

		TypeWithThreeTenBpLocalDateMappedToDate typeWithLocalDate = new TypeWithThreeTenBpLocalDateMappedToDate();
		typeWithLocalDate.localDate = org.threeten.bp.LocalDate.of(2010, 7, 4);

		Insert insert = QueryBuilder.insertInto("table");

		mappingCassandraConverter.write(typeWithLocalDate, insert);

		assertThat(getValues(insert).contains(LocalDate.fromYearMonthDay(2010, 7, 4))).isTrue();
	}

	@Test // DATACASS-296
	public void shouldCreateUpdateWithThreeTenBpLocalDateUsingCassandraDateCorrectly() {

		TypeWithThreeTenBpLocalDateMappedToDate typeWithLocalDate = new TypeWithThreeTenBpLocalDateMappedToDate();
		typeWithLocalDate.localDate = org.threeten.bp.LocalDate.of(2010, 7, 4);

		Update update = QueryBuilder.update("table");

		mappingCassandraConverter.write(typeWithLocalDate, update);

		assertThat(getAssignmentValues(update)).contains(LocalDate.fromYearMonthDay(2010, 7, 4));
	}

	@Test // DATACASS-206
	public void updateShouldUseSpecifiedColumnNames() {

		UserToken userToken = new UserToken();
		userToken.setUserId(UUID.randomUUID());
		userToken.setToken(UUID.randomUUID());
		userToken.setAdminComment("admin comment");
		userToken.setUserComment("user comment");

		Update update = QueryBuilder.update("table");

		mappingCassandraConverter.write(userToken, update);

		assertThat(getAssignments(update)).containsEntry("admincomment", "admin comment");
		assertThat(getAssignments(update)).containsEntry("user_comment", "user comment");
		assertThat(getWherePredicates(update)).containsEntry("user_id", userToken.getUserId());
	}

	@Test // DATACASS-206
	public void deleteShouldUseSpecifiedColumnNames() {

		UserToken userToken = new UserToken();
		userToken.setUserId(UUID.randomUUID());
		userToken.setToken(UUID.randomUUID());
		userToken.setAdminComment("admin comment");
		userToken.setUserComment("user comment");

		Delete delete = QueryBuilder.delete().from("table");

		mappingCassandraConverter.write(userToken, delete.where());

		assertThat(getWherePredicates(delete)).containsEntry("user_id", userToken.getUserId());
	}

	@Test // DATACASS-308
	public void shouldWriteWhereConditionUsingPlainId() {

		Delete delete = QueryBuilder.delete().from("table");

		mappingCassandraConverter.write("42", delete.where(), mappingContext.getRequiredPersistentEntity(User.class));

		assertThat(getWherePredicates(delete)).containsEntry("id", "42");
	}

	@Test // DATACASS-308
	public void shouldWriteWhereConditionUsingEntity() {

		Delete delete = QueryBuilder.delete().from("table");

		User user = new User();
		user.setId("42");

		mappingCassandraConverter.write(user, delete.where(), mappingContext.getRequiredPersistentEntity(User.class));

		assertThat(getWherePredicates(delete)).containsEntry("id", "42");
	}

	@Test(expected = IllegalArgumentException.class) // DATACASS-308
	public void shouldFailWriteWhereConditionUsingEntityWithNullId() {

		Delete delete = QueryBuilder.delete().from("table");

		mappingCassandraConverter.write(new User(), delete.where(), mappingContext.getRequiredPersistentEntity(User.class));
	}

	@Test // DATACASS-308
	public void shouldWriteWhereConditionUsingMapId() {

		Delete delete = QueryBuilder.delete().from("table");

		mappingCassandraConverter.write(id("id", "42"), delete.where(),
				mappingContext.getRequiredPersistentEntity(User.class));

		assertThat(getWherePredicates(delete)).containsEntry("id", "42");
	}

	@Test // DATACASS-308
	public void shouldWriteWhereConditionForCompositeKeyUsingEntity() {

		Delete delete = QueryBuilder.delete().from("table");

		TypeWithCompositeKey entity = new TypeWithCompositeKey();
		entity.setFirstname("Walter");
		entity.setLastname("White");

		mappingCassandraConverter.write(entity, delete.where(),
				mappingContext.getRequiredPersistentEntity(TypeWithCompositeKey.class));

		assertThat(getWherePredicates(delete)).containsEntry("firstname", "Walter");
		assertThat(getWherePredicates(delete)).containsEntry("lastname", "White");
	}

	@Test // DATACASS-308
	public void shouldWriteWhereConditionForCompositeKeyUsingMapId() {

		Delete delete = QueryBuilder.delete().from("table");

		mappingCassandraConverter.write(id("firstname", "Walter").with("lastname", "White"), delete.where(),
				mappingContext.getRequiredPersistentEntity(TypeWithCompositeKey.class));

		assertThat(getWherePredicates(delete)).containsEntry("firstname", "Walter");
		assertThat(getWherePredicates(delete)).containsEntry("lastname", "White");
	}

	@Test // DATACASS-308
	public void shouldWriteWhereConditionForMapIdKeyUsingEntity() {

		Delete delete = QueryBuilder.delete().from("table");

		TypeWithMapId entity = new TypeWithMapId();
		entity.setFirstname("Walter");
		entity.setLastname("White");

		mappingCassandraConverter.write(entity, delete.where(),
				mappingContext.getRequiredPersistentEntity(TypeWithMapId.class));

		assertThat(getWherePredicates(delete)).containsEntry("firstname", "Walter");
		assertThat(getWherePredicates(delete)).containsEntry("lastname", "White");
	}

	@Test // DATACASS-308
	public void shouldWriteEnumWhereCondition() {

		Delete delete = QueryBuilder.delete().from("table");

		mappingCassandraConverter.write(Condition.MINT, delete.where(),
				mappingContext.getRequiredPersistentEntity(EnumPrimaryKey.class));

		assertThat(getWherePredicates(delete)).containsEntry("condition", "MINT");
	}

	@Test // DATACASS-308
	public void shouldWriteWhereConditionForMapIdKeyUsingMapId() {

		Delete delete = QueryBuilder.delete().from("table");

		mappingCassandraConverter.write(id("firstname", "Walter").with("lastname", "White"), delete.where(),
				mappingContext.getRequiredPersistentEntity(TypeWithMapId.class));

		assertThat(getWherePredicates(delete)).containsEntry("firstname", "Walter");
		assertThat(getWherePredicates(delete)).containsEntry("lastname", "White");
	}

	@Test // DATACASS-308
	public void shouldWriteWhereConditionForTypeWithPkClassKeyUsingEntity() {

		Delete delete = QueryBuilder.delete().from("table");

		CompositeKey key = new CompositeKey();
		key.setFirstname("Walter");
		key.setLastname("White");

		TypeWithKeyClass entity = new TypeWithKeyClass();
		entity.setKey(key);

		mappingCassandraConverter.write(entity, delete.where(),
				mappingContext.getRequiredPersistentEntity(TypeWithKeyClass.class));

		assertThat(getWherePredicates(delete)).containsEntry("first_name", "Walter");
		assertThat(getWherePredicates(delete)).containsEntry("lastname", "White");
	}

	@Test(expected = IllegalArgumentException.class) // DATACASS-308
	public void shouldFailWritingWhereConditionForTypeWithPkClassKeyUsingEntityWithNullId() {

		Delete delete = QueryBuilder.delete().from("table");

		mappingCassandraConverter.write(new TypeWithKeyClass(), delete.where(),
				mappingContext.getRequiredPersistentEntity(TypeWithKeyClass.class));
	}

	@Test // DATACASS-308
	public void shouldWriteWhereConditionForTypeWithPkClassKeyUsingKey() {

		Delete delete = QueryBuilder.delete().from("table");

		CompositeKey key = new CompositeKey();
		key.setFirstname("Walter");
		key.setLastname("White");

		mappingCassandraConverter.write(key, delete.where(),
				mappingContext.getRequiredPersistentEntity(TypeWithKeyClass.class));

		assertThat(getWherePredicates(delete)).containsEntry("first_name", "Walter");
		assertThat(getWherePredicates(delete)).containsEntry("lastname", "White");
	}

	@Test // DATACASS-463
	public void shouldReadTypeWithCompositePrimaryKeyCorrectly() {

		// condition, localDate
		Row row = RowMockUtil.newRowMock(column("condition", "MINT", DataType.varchar()),
				column("localdate", LocalDate.fromYearMonthDay(2017, 1, 2), DataType.date()));

		TypeWithEnumAndLocalDateKey result = mappingCassandraConverter.read(TypeWithEnumAndLocalDateKey.class, row);

		assertThat(result.id.condition).isEqualTo(Condition.MINT);
		assertThat(result.id.localDate).isEqualTo(java.time.LocalDate.of(2017, 1, 2));
	}

	@Test // DATACASS-308
	public void shouldWriteWhereConditionForTypeWithPkClassKeyUsingMapId() {

		Delete delete = QueryBuilder.delete().from("table");

		mappingCassandraConverter.write(id("firstname", "Walter").with("lastname", "White"), delete.where(),
				mappingContext.getRequiredPersistentEntity(TypeWithKeyClass.class));

		assertThat(getWherePredicates(delete)).containsEntry("first_name", "Walter");
		assertThat(getWherePredicates(delete)).containsEntry("lastname", "White");
	}

	@Test(expected = IllegalArgumentException.class) // DATACASS-308
	public void shouldFailWhereConditionForTypeWithPkClassKeyUsingMapIdHavingUnknownProperty() {

		Delete delete = QueryBuilder.delete().from("table");

		mappingCassandraConverter.write(id("unknown", "Walter"), delete.where(),
				mappingContext.getRequiredPersistentEntity(TypeWithMapId.class));
	}

	@Test // DATACASS-362
	public void shouldSelectCompositeIdUsingMapId() {

		Select select = QueryBuilder.select().from("foo");

		MapId mapId = BasicMapId.id("firstname", "first").with("lastname", "last");

		mappingCassandraConverter.write(mapId, select.where(),
				mappingContext.getRequiredPersistentEntity(TypeWithMapId.class));

		assertThat(select.toString()).isEqualTo("SELECT * FROM foo WHERE firstname='first' AND lastname='last';");
	}

	@Test // DATACASS-362
	public void shouldSelectCompositeIdUsingCompositeKeyClass() {

		Select select = QueryBuilder.select().from("foo");

		CompositeKey key = new CompositeKey();
		key.setFirstname("first");
		key.setLastname("last");

		mappingCassandraConverter.write(key, select.where(),
				mappingContext.getRequiredPersistentEntity(TypeWithKeyClass.class));

		assertThat(select.toString()).isEqualTo("SELECT * FROM foo WHERE first_name='first' AND lastname='last';");
	}

	@Test // DATACASS-362
	public void shouldSelectCompositeIdUsingCompositeKeyClassViaMapId() {

		Select select = QueryBuilder.select().from("foo");

		MapId mapId = BasicMapId.id("firstname", "first").with("lastname", "last");

		mappingCassandraConverter.write(mapId, select.where(),
				mappingContext.getRequiredPersistentEntity(TypeWithKeyClass.class));

		assertThat(select.toString()).isEqualTo("SELECT * FROM foo WHERE first_name='first' AND lastname='last';");
	}

	@Test // DATACASS-362
	public void shouldDeleteCompositeIdUsingCompositeKeyClass() {

		Delete delete = QueryBuilder.delete().from("foo");

		CompositeKey key = new CompositeKey();
		key.setFirstname("first");
		key.setLastname("last");

		mappingCassandraConverter.write(key, delete.where(),
				mappingContext.getRequiredPersistentEntity(TypeWithKeyClass.class));

		assertThat(delete.toString()).isEqualTo("DELETE FROM foo WHERE first_name='first' AND lastname='last';");
	}

	@Test // DATACASS-487
	public void shouldReadConvertedMap() {

		LocalDate date1 = LocalDate.fromYearMonthDay(2018, 1, 1);
		LocalDate date2 = LocalDate.fromYearMonthDay(2019, 1, 1);

		Map<String, List<LocalDate>> times = Collections.singletonMap("Europe/Paris", Arrays.asList(date1, date2));

		rowMock = RowMockUtil.newRowMock(
				RowMockUtil.column("times", times, DataType.map(DataType.varchar(), DataType.list(DataType.date()))));

		TypeWithConvertedMap converted = this.mappingCassandraConverter.read(TypeWithConvertedMap.class, rowMock);

		assertThat(converted.times).containsKeys(ZoneId.of("Europe/Paris"));

		List<java.time.LocalDate> convertedTimes = converted.times.get(ZoneId.of("Europe/Paris"));

		assertThat(convertedTimes).hasSize(2).hasOnlyElementsOfType(java.time.LocalDate.class);
	}

	@Test // DATACASS-487
	public void shouldWriteConvertedMap() {

		java.time.LocalDate date1 = java.time.LocalDate.of(2018, 1, 1);
		java.time.LocalDate date2 = java.time.LocalDate.of(2019, 1, 1);

		TypeWithConvertedMap typeWithConvertedMap = new TypeWithConvertedMap();

		typeWithConvertedMap.times = Collections.singletonMap(ZoneId.of("Europe/Paris"), Arrays.asList(date1, date2));

		Insert insert = QueryBuilder.insertInto("table");

		this.mappingCassandraConverter.write(typeWithConvertedMap, insert);

		List<Object> values = getValues(insert);

		assertThat(values).hasSize(1);
		assertThat(values.get(0)).isInstanceOf(Map.class);

		Map<String, List<LocalDate>> map = (Map) values.get(0);

		assertThat(map).containsKey("Europe/Paris");
		assertThat(map.get("Europe/Paris")).hasOnlyElementsOfType(LocalDate.class);
	}

	@SuppressWarnings("unchecked")
	private static <T> List<T> getListValue(Insert statement) {

		List<Object> values = getValues(statement);
		return (List<T>) values.stream().filter(value -> value instanceof List).findFirst().orElse(null);
	}

	@SuppressWarnings("unchecked")
	private static <T> Set<T> getSetValue(Insert statement) {

		List<Object> values = getValues(statement);
		return (Set<T>) values.stream().filter(value -> value instanceof Set).findFirst().orElse(null);
	}

	@SuppressWarnings("unchecked")
	private static List<Object> getValues(Insert statement) {
		return (List<Object>) ReflectionTestUtils.getField(statement, "values");
	}

	@SuppressWarnings("unchecked")
	private static Collection<Object> getAssignmentValues(Update statement) {
		return getAssignments(statement).values();
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> getAssignments(Update statement) {

		Map<String, Object> result = new LinkedHashMap<>();

		Assignments assignments = (Assignments) ReflectionTestUtils.getField(statement, "assignments");

		List<Assignment> listOfAssignments = (List<Assignment>) ReflectionTestUtils.getField(assignments, "assignments");

		for (Assignment assignment : listOfAssignments) {
			result.put(assignment.getColumnName(), ReflectionTestUtils.getField(assignment, "value"));
		}

		return result;
	}

	private static Collection<Object> getWhereValues(Update update) {
		return getWherePredicates(update.where()).values();
	}

	private static Collection<Object> getWhereValues(BuiltStatement where) {
		return getWherePredicates(where).values();
	}

	private static Map<String, Object> getWherePredicates(Update statement) {
		return getWherePredicates(statement.where());
	}

	private static Map<String, Object> getWherePredicates(Delete statement) {
		return getWherePredicates(statement.where());
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> getWherePredicates(BuiltStatement where) {

		Map<String, Object> result = new LinkedHashMap<>();

		List<Clause> clauses = (List<Clause>) ReflectionTestUtils.getField(where, "clauses");

		for (Clause clause : clauses) {
			result.put(ReflectionTestUtils.invokeMethod(clause, "name"), ReflectionTestUtils.getField(clause, "value"));
		}

		return result;
	}

	@Table
	public static class UnsupportedEnumToOrdinalMapping {

		@PrimaryKey private String id;

		@CassandraType(type = Name.INT) private Condition asOrdinal;

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public Condition getAsOrdinal() {
			return asOrdinal;
		}

		public void setAsOrdinal(Condition asOrdinal) {
			this.asOrdinal = asOrdinal;
		}
	}

	@Table
	public static class WithEnumColumns {

		@PrimaryKey private String id;

		private Condition condition;

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public Condition getCondition() {
			return condition;
		}

		public void setCondition(Condition condition) {
			this.condition = condition;
		}
	}

	@PrimaryKeyClass
	public static class EnumCompositePrimaryKey implements Serializable {

		@PrimaryKeyColumn(ordinal = 1, type = PrimaryKeyType.PARTITIONED) private Condition condition;

		public EnumCompositePrimaryKey() {}

		public EnumCompositePrimaryKey(Condition condition) {
			this.condition = condition;
		}

		public Condition getCondition() {
			return condition;
		}

		public void setCondition(Condition condition) {
			this.condition = condition;
		}
	}

	@PrimaryKeyClass
	@RequiredArgsConstructor
	@Value
	public static class EnumAndDateCompositePrimaryKey implements Serializable {

		@PrimaryKeyColumn(ordinal = 1, type = PrimaryKeyType.PARTITIONED) private final Condition condition;

		@PrimaryKeyColumn(ordinal = 2, type = PrimaryKeyType.PARTITIONED) private final java.time.LocalDate localDate;
	}

	@RequiredArgsConstructor
	public static class TypeWithEnumAndLocalDateKey {

		@PrimaryKey private final EnumAndDateCompositePrimaryKey id;
	}

	@Table
	public static class EnumPrimaryKey {

		@PrimaryKey private Condition condition;

		public Condition getCondition() {
			return condition;
		}

		public void setCondition(Condition condition) {
			this.condition = condition;
		}
	}

	@Table
	public static class CompositeKeyThing {

		@PrimaryKey private EnumCompositePrimaryKey key;

		public CompositeKeyThing() {}

		public CompositeKeyThing(EnumCompositePrimaryKey key) {
			this.key = key;
		}

		public EnumCompositePrimaryKey getKey() {
			return key;
		}

		public void setKey(EnumCompositePrimaryKey key) {
			this.key = key;
		}
	}

	public enum Condition {
		MINT, USED
	}

	@Table
	public static class TypeWithLocalDate {

		@PrimaryKey private String id;

		java.time.LocalDate localDate;
		java.time.LocalDateTime localDateTime;

		List<java.time.LocalDate> list;
		Set<java.time.LocalDate> set;
	}

	/**
	 * Uses Cassandra's {@link Name#DATE} which maps by default to {@link LocalDate}
	 */
	@Table
	@AllArgsConstructor
	public static class TypeWithLocalDateMappedToDate {

		@PrimaryKey private String id;

		@CassandraType(type = Name.DATE) java.time.LocalDate localDate;
	}

	/**
	 * Uses Cassandra's {@link Name#DATE} which maps by default to Joda {@link LocalDate}
	 */
	@Table
	public static class TypeWithJodaLocalDateMappedToDate {

		@PrimaryKey private String id;

		@CassandraType(type = Name.DATE) org.joda.time.LocalDate localDate;
	}

	/**
	 * Uses Cassandra's {@link Name#DATE} which maps by default to Joda {@link LocalDate}
	 */
	@Table
	public static class TypeWithThreeTenBpLocalDateMappedToDate {

		@PrimaryKey private String id;

		@CassandraType(type = Name.DATE) org.threeten.bp.LocalDate localDate;
	}

	@Table
	public static class TypeWithInstant {

		@PrimaryKey private String id;

		Instant instant;
	}

	@Table
	public static class TypeWithZoneId {

		@PrimaryKey private String id;

		ZoneId zoneId;
	}

	@Table
	public static class TypeWithConvertedMap {

		@PrimaryKey private String id;

		Map<ZoneId, List<java.time.LocalDate>> times;
	}
}
