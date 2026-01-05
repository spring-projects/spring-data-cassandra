/*
 * Copyright 2016-present the original author or authors.
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
import static org.springframework.data.cassandra.core.mapping.BasicMapId.*;
import static org.springframework.data.cassandra.test.util.RowMockUtil.*;

import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.*;

import org.assertj.core.data.Percentage;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.ReadOnlyProperty;
import org.springframework.data.annotation.Transient;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.*;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.domain.AllPossibleTypes;
import org.springframework.data.cassandra.domain.CompositeKey;
import org.springframework.data.cassandra.domain.TypeWithCompositeKey;
import org.springframework.data.cassandra.domain.TypeWithKeyClass;
import org.springframework.data.cassandra.domain.TypeWithMapId;
import org.springframework.data.cassandra.domain.User;
import org.springframework.data.cassandra.domain.UserToken;
import org.springframework.data.cassandra.support.UserDefinedTypeBuilder;
import org.springframework.data.cassandra.test.util.RowMockUtil;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.SimplePropertyValueConversions;
import org.springframework.data.convert.ValueConverter;
import org.springframework.data.projection.EntityProjection;
import org.springframework.data.projection.EntityProjectionIntrospector;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.util.ObjectUtils;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.internal.core.data.DefaultTupleValue;
import com.datastax.oss.driver.internal.core.type.DefaultTupleType;

/**
 * Unit tests for {@link MappingCassandraConverter}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @soundtrack Outlandich - Dont Leave Me Feat Cyt (Sun Kidz Electrocore Mix)
 */
public class MappingCassandraConverterUnitTests {

	private Row rowMock;

	private CassandraMappingContext mappingContext;
	private MappingCassandraConverter converter;

	@BeforeEach
	void setUp() {

		CassandraCustomConversions conversions = new CassandraCustomConversions(
				List.of(new ByteBufferToDoubleHolderConverter()));
		this.mappingContext = new CassandraMappingContext();
		this.mappingContext.setSimpleTypeHolder(conversions.getSimpleTypeHolder());

		this.converter = new MappingCassandraConverter(mappingContext);
		this.converter.setCustomConversions(conversions);
		this.converter.afterPropertiesSet();
	}

	@Test // DATACASS-260
	void insertEnumShouldMapToString() {

		WithEnumColumns withEnumColumns = new WithEnumColumns();

		withEnumColumns.setCondition(Condition.MINT);

		Map<CqlIdentifier, Object> insert = new LinkedHashMap<>();

		converter.write(withEnumColumns, insert);

		assertThat(getValues(insert)).contains("MINT");
	}

	@Test // DATACASS-260
	void shouldWriteEnumSet() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setSetOfEnum(Collections.singleton(CassandraTypeMappingIntegrationTests.Condition.MINT));

		Map<CqlIdentifier, Object> insert = new LinkedHashMap<>();

		converter.write(entity, insert);

		assertThat(insert.get(CqlIdentifier.fromCql("setofenum"))).isInstanceOf(Set.class);
	}

	@Test // DATACASS-255
	void insertEnumMapsToOrdinal() {

		EnumToOrdinalMapping enumToOrdinalMapping = new EnumToOrdinalMapping();
		enumToOrdinalMapping.setAsOrdinal(Condition.USED);

		Map<CqlIdentifier, Object> insert = new LinkedHashMap<>();

		converter.write(enumToOrdinalMapping, insert);

		assertThat(getValues(insert)).contains(Condition.USED.ordinal());
	}

	@Test // DATACASS-255, DATACASS-652
	void selectEnumMapsToOrdinal() {

		rowMock = RowMockUtil.newRowMock(column("asOrdinal", 1, DataTypes.INT));

		EnumToOrdinalMapping loaded = converter.read(EnumToOrdinalMapping.class, rowMock);

		assertThat(loaded.getAsOrdinal()).isEqualTo(Condition.USED);
	}

	@Test // DATACASS-260
	void insertEnumAsPrimaryKeyShouldMapToString() {

		EnumPrimaryKey key = new EnumPrimaryKey();
		key.setCondition(Condition.MINT);

		Map<CqlIdentifier, Object> insert = new LinkedHashMap<>();

		converter.write(key, insert);

		assertThat(getValues(insert)).contains("MINT");
	}

	@Test // DATACASS-260
	void insertEnumInCompositePrimaryKeyShouldMapToString() {

		EnumCompositePrimaryKey key = new EnumCompositePrimaryKey();
		key.setCondition(Condition.MINT);

		CompositeKeyThing composite = new CompositeKeyThing();
		composite.setKey(key);

		Map<CqlIdentifier, Object> insert = new LinkedHashMap<>();

		converter.write(composite, insert);

		assertThat(getValues(insert)).contains("MINT");
	}

	@Test // DATACASS-260
	void updateEnumAsPrimaryKeyShouldMapToString() {

		EnumPrimaryKey key = new EnumPrimaryKey();
		key.setCondition(Condition.MINT);

		Where where = new Where();

		converter.write(key, where);

		assertThat(getWhereValues(where)).contains("MINT");
	}

	@Test // DATACASS-260
	void writeWhereEnumInCompositePrimaryKeyShouldMapToString() {

		EnumCompositePrimaryKey key = new EnumCompositePrimaryKey();
		key.setCondition(Condition.MINT);

		CompositeKeyThing composite = new CompositeKeyThing();
		composite.setKey(key);

		Where where = new Where();

		converter.write(composite, where);

		assertThat(getWhereValues(where)).contains("MINT");
	}

	@Test // DATACASS-260
	void writeWhereEnumAsPrimaryKeyShouldMapToString() {

		EnumPrimaryKey key = new EnumPrimaryKey();
		key.setCondition(Condition.MINT);

		Where where = new Where();

		converter.write(key, where);

		assertThat(getWhereValues(where)).contains("MINT");
	}

	@Test // DATACASS-280
	void shouldReadStringCorrectly() {

		rowMock = RowMockUtil.newRowMock(column("foo", "foo", DataTypes.TEXT));

		String result = converter.readRow(String.class, rowMock);

		assertThat(result).isEqualTo("foo");
	}

	@Test // DATACASS-280
	void shouldReadIntegerCorrectly() {

		rowMock = RowMockUtil.newRowMock(column("foo", 2, DataTypes.VARINT));

		Integer result = converter.readRow(Integer.class, rowMock);

		assertThat(result).isEqualTo(2);
	}

	@Test // DATACASS-280
	void shouldReadLongCorrectly() {

		rowMock = RowMockUtil.newRowMock(column("foo", 2, DataTypes.VARINT));

		Long result = converter.readRow(Long.class, rowMock);

		assertThat(result).isEqualTo(2L);
	}

	@Test // DATACASS-280
	void shouldReadDoubleCorrectly() {

		rowMock = RowMockUtil.newRowMock(column("foo", 2D, DataTypes.DOUBLE));

		Double result = converter.readRow(Double.class, rowMock);

		assertThat(result).isEqualTo(2D);
	}

	@Test // DATACASS-280
	void shouldReadFloatCorrectly() {

		rowMock = RowMockUtil.newRowMock(column("foo", 2F, DataTypes.DOUBLE));

		Float result = converter.readRow(Float.class, rowMock);

		assertThat(result).isEqualTo(2F);
	}

	@Test // DATACASS-280
	void shouldReadBigIntegerCorrectly() {

		rowMock = RowMockUtil.newRowMock(column("foo", BigInteger.valueOf(2), DataTypes.BIGINT));

		BigInteger result = converter.readRow(BigInteger.class, rowMock);

		assertThat(result).isEqualTo(BigInteger.valueOf(2));
	}

	@Test // DATACASS-280
	void shouldReadBigDecimalCorrectly() {

		rowMock = RowMockUtil.newRowMock(column("foo", BigDecimal.valueOf(2), DataTypes.DECIMAL));

		BigDecimal result = converter.readRow(BigDecimal.class, rowMock);

		assertThat(result).isEqualTo(BigDecimal.valueOf(2));
	}

	@Test // DATACASS-280
	void shouldReadUUIDCorrectly() {

		UUID uuid = UUID.randomUUID();

		rowMock = RowMockUtil.newRowMock(column("foo", uuid, DataTypes.UUID));

		UUID result = converter.readRow(UUID.class, rowMock);

		assertThat(result).isEqualTo(uuid);
	}

	@Test // DATACASS-280
	void shouldReadInetAddressCorrectly() throws UnknownHostException {

		InetAddress localHost = InetAddress.getLoopbackAddress();
		rowMock = RowMockUtil.newRowMock(column("foo", localHost, DataTypes.UUID));

		InetAddress result = converter.readRow(InetAddress.class, rowMock);

		assertThat(result).isEqualTo(localHost);
	}

	@Test // DATACASS-280, DATACASS-271
	void shouldReadTimestampCorrectly() {

		Instant instant = Instant.now();

		rowMock = RowMockUtil.newRowMock(column("foo", instant, DataTypes.TIMESTAMP));

		Date result = converter.readRow(Date.class, rowMock);

		assertThat(result).isEqualTo(Date.from(instant));
	}

	@Test // DATACASS-280, DATACASS-271
	void shouldReadInstantTimestampCorrectly() {

		Instant instant = Instant.now();

		rowMock = RowMockUtil.newRowMock(column("foo", instant, DataTypes.TIMESTAMP));

		Instant result = converter.readRow(Instant.class, rowMock);

		assertThat(result).isEqualTo(instant);
	}

	@Test // DATACASS-656
	void shouldReadAndWriteTimestampFromObject() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setInstant(Instant.now());
		entity.setTimestamp(new Date(1));

		Map<CqlIdentifier, Object> insert = new LinkedHashMap<>();

		converter.write(entity, insert);

		assertThat(insert.get(CqlIdentifier.fromCql("instant"))).isInstanceOf(Instant.class);
		assertThat(insert.get(CqlIdentifier.fromCql("timestamp"))).isInstanceOf(Instant.class);
	}

	@Test // DATACASS-271
	void shouldReadDateCorrectly() {

		LocalDate date = LocalDate.ofEpochDay(1234);

		rowMock = RowMockUtil.newRowMock(column("foo", date, DataTypes.DATE));

		LocalDate result = converter.readRow(LocalDate.class, rowMock);

		assertThat(result).isEqualTo(date);
	}

	@Test // DATACASS-280
	void shouldReadBooleanCorrectly() {

		rowMock = RowMockUtil.newRowMock(column("foo", true, DataTypes.BOOLEAN));

		Boolean result = converter.readRow(Boolean.class, rowMock);

		assertThat(result).isEqualTo(true);
	}

	@Test // DATACASS-296
	void shouldReadLocalDateCorrectly() {

		LocalDateTime now = LocalDateTime.now();
		Instant instant = now.toInstant(ZoneOffset.UTC);

		rowMock = RowMockUtil.newRowMock(column("id", "my-id", DataTypes.ASCII),
				column("localdate", Date.from(instant), DataTypes.TIMESTAMP));

		TypeWithLocalDate result = converter.readRow(TypeWithLocalDate.class, rowMock);

		assertThat(result.localDate).isNotNull();
		assertThat(result.localDate.getYear()).isEqualTo(now.getYear());
		assertThat(result.localDate.getMonthValue()).isEqualTo(now.getMonthValue());
	}

	@Test // DATACASS-296
	void shouldCreateInsertWithLocalDateCorrectly() {

		java.time.LocalDate now = java.time.LocalDate.now();

		TypeWithLocalDate typeWithLocalDate = new TypeWithLocalDate();
		typeWithLocalDate.localDate = now;

		Map<CqlIdentifier, Object> insert = new LinkedHashMap<>();

		converter.write(typeWithLocalDate, insert);

		assertThat(getValues(insert)).contains(LocalDate.of(now.getYear(), now.getMonthValue(), now.getDayOfMonth()));
	}

	@Test // DATACASS-296
	void shouldCreateUpdateWithLocalDateCorrectly() {

		java.time.LocalDate now = java.time.LocalDate.now();

		TypeWithLocalDate typeWithLocalDate = new TypeWithLocalDate();
		typeWithLocalDate.localDate = now;

		Map<CqlIdentifier, Object> insert = new LinkedHashMap<>();

		converter.write(typeWithLocalDate, insert);

		assertThat(getValues(insert)).contains(LocalDate.of(now.getYear(), now.getMonthValue(), now.getDayOfMonth()));
	}

	@Test // DATACASS-296
	void shouldCreateInsertWithLocalDateListUsingCassandraDate() {

		java.time.LocalDate now = java.time.LocalDate.now();
		java.time.LocalDate localDate = java.time.LocalDate.of(2010, 7, 4);

		TypeWithLocalDate typeWithLocalDate = new TypeWithLocalDate();
		typeWithLocalDate.list = Arrays.asList(now, localDate);

		Map<CqlIdentifier, Object> insert = new LinkedHashMap<>();

		converter.write(typeWithLocalDate, insert);

		List<LocalDate> dates = (List) insert.get(CqlIdentifier.fromCql("list"));

		assertThat(dates).contains(LocalDate.of(now.getYear(), now.getMonthValue(), now.getDayOfMonth()));
		assertThat(dates).contains(LocalDate.of(2010, 7, 4));
	}

	@Test // DATACASS-296
	void shouldCreateInsertWithLocalDateSetUsingCassandraDate() {

		java.time.LocalDate now = java.time.LocalDate.now();
		java.time.LocalDate localDate = java.time.LocalDate.of(2010, 7, 4);

		TypeWithLocalDate typeWithLocalDate = new TypeWithLocalDate();
		typeWithLocalDate.set = new HashSet<>(Arrays.asList(now, localDate));

		Map<CqlIdentifier, Object> insert = new LinkedHashMap<>();

		converter.write(typeWithLocalDate, insert);

		Set<LocalDate> dates = (Set) insert.get(CqlIdentifier.fromInternal("set"));

		assertThat(dates).contains(LocalDate.of(now.getYear(), now.getMonthValue(), now.getDayOfMonth()));
		assertThat(dates).contains(LocalDate.of(2010, 7, 4));
	}

	@Test // DATACASS-296
	void shouldReadLocalDateTimeUsingCassandraDateCorrectly() {

		rowMock = RowMockUtil.newRowMock(column("id", "my-id", DataTypes.ASCII),
				column("localDate", LocalDate.of(2010, 7, 4), DataTypes.DATE));

		TypeWithLocalDateMappedToDate result = converter.readRow(TypeWithLocalDateMappedToDate.class, rowMock);

		assertThat(result.localDate).isNotNull();
		assertThat(result.localDate.getYear()).isEqualTo(2010);
		assertThat(result.localDate.getMonthValue()).isEqualTo(7);
		assertThat(result.localDate.getDayOfMonth()).isEqualTo(4);
	}

	@Test // DATACASS-296, DATACASS-400
	void shouldCreateInsertWithLocalDateUsingCassandraDateCorrectly() {

		TypeWithLocalDateMappedToDate typeWithLocalDate = new TypeWithLocalDateMappedToDate(null,
				java.time.LocalDate.of(2010, 7, 4));

		Map<CqlIdentifier, Object> insert = new LinkedHashMap<>();

		converter.write(typeWithLocalDate, insert);

		assertThat(getValues(insert)).contains(LocalDate.of(2010, 7, 4));
	}

	@Test // DATACASS-296
	void shouldCreateUpdateWithLocalDateUsingCassandraDateCorrectly() {

		TypeWithLocalDateMappedToDate typeWithLocalDate = new TypeWithLocalDateMappedToDate(null,
				java.time.LocalDate.of(2010, 7, 4));

		Map<CqlIdentifier, Object> update = new LinkedHashMap<>();

		converter.write(typeWithLocalDate, update);

		assertThat(getValues(update)).contains(LocalDate.of(2010, 7, 4));
	}

	@Test // DATACASS-296
	void shouldReadLocalDateTimeCorrectly() {

		LocalDateTime now = LocalDateTime.now();
		Instant instant = now.toInstant(ZoneOffset.UTC);

		rowMock = RowMockUtil.newRowMock(column("id", "my-id", DataTypes.ASCII),
				column("localDateTime", Date.from(instant), DataTypes.TIMESTAMP));

		TypeWithLocalDate result = converter.readRow(TypeWithLocalDate.class, rowMock);

		assertThat(result.localDateTime).isNotNull();
		assertThat(result.localDateTime.getYear()).isEqualTo(now.getYear());
		assertThat(result.localDateTime.getMinute()).isEqualTo(now.getMinute());
	}

	@Test // DATACASS-296
	void shouldReadInstantCorrectly() {

		LocalDateTime now = LocalDateTime.now();
		Instant instant = now.toInstant(ZoneOffset.UTC);

		rowMock = RowMockUtil.newRowMock(column("id", "my-id", DataTypes.ASCII),
				column("instant", Date.from(instant), DataTypes.TIMESTAMP));

		TypeWithInstant result = converter.readRow(TypeWithInstant.class, rowMock);

		assertThat(result.instant).isNotNull();
		assertThat(result.instant.getEpochSecond()).isEqualTo(instant.getEpochSecond());
	}

	@Test // DATACASS-296
	void shouldReadZoneIdCorrectly() {

		rowMock = RowMockUtil.newRowMock(column("id", "my-id", DataTypes.ASCII),
				column("zoneId", "Europe/Paris", DataTypes.TEXT));

		TypeWithZoneId result = converter.readRow(TypeWithZoneId.class, rowMock);

		assertThat(result.zoneId).isNotNull();
		assertThat(result.zoneId.getId()).isEqualTo("Europe/Paris");
	}

	@Test // DATACASS-206
	void updateShouldUseSpecifiedColumnNames() {

		UserToken userToken = new UserToken();
		userToken.setUserId(UUID.randomUUID());
		userToken.setToken(UUID.randomUUID());
		userToken.setAdminComment("admin comment");
		userToken.setUserComment("user comment");

		Map<CqlIdentifier, Object> update = new LinkedHashMap<>();
		Where where = new Where();

		converter.write(userToken, update);
		converter.write(userToken, where);

		assertThat(update).containsEntry(CqlIdentifier.fromCql("admincomment"), "admin comment");
		assertThat(update).containsEntry(CqlIdentifier.fromCql("user_comment"), "user comment");
		assertThat(where).containsEntry(CqlIdentifier.fromCql("user_id"), userToken.getUserId());
	}

	@Test // DATACASS-308
	void shouldWriteWhereConditionUsingPlainId() {

		Where where = new Where();

		converter.write("42", where, mappingContext.getRequiredPersistentEntity(User.class));

		assertThat(where).containsEntry(CqlIdentifier.fromCql("id"), "42");
	}

	@Test // DATACASS-308
	void shouldWriteWhereConditionUsingEntity() {

		Where where = new Where();

		User user = new User();
		user.setId("42");

		converter.write(user, where, mappingContext.getRequiredPersistentEntity(User.class));

		assertThat(where).containsEntry(CqlIdentifier.fromCql("id"), "42");
	}

	@Test // DATACASS-308
	void shouldFailWriteWhereConditionUsingEntityWithNullId() {

		assertThatIllegalArgumentException().isThrownBy(
				() -> converter.write(new User(), new Where(), mappingContext.getRequiredPersistentEntity(User.class)));
	}

	@Test // DATACASS-308
	void shouldWriteWhereConditionUsingMapId() {

		Where where = new Where();

		converter.write(id("id", "42"), where, mappingContext.getRequiredPersistentEntity(User.class));

		assertThat(where).containsEntry(CqlIdentifier.fromCql("id"), "42");
	}

	@Test // DATACASS-308
	void shouldWriteWhereConditionForCompositeKeyUsingEntity() {

		Where where = new Where();

		TypeWithCompositeKey entity = new TypeWithCompositeKey();
		entity.setFirstname("Walter");
		entity.setLastname("White");

		converter.write(entity, where, mappingContext.getRequiredPersistentEntity(TypeWithCompositeKey.class));

		assertThat(where).containsEntry(CqlIdentifier.fromCql("firstname"), "Walter");
		assertThat(where).containsEntry(CqlIdentifier.fromCql("lastname"), "White");
	}

	@Test // DATACASS-308
	void shouldWriteWhereConditionForCompositeKeyUsingMapId() {

		Where where = new Where();

		converter.write(id("firstname", "Walter").with("lastname", "White"), where,
				mappingContext.getRequiredPersistentEntity(TypeWithCompositeKey.class));

		assertThat(where).containsEntry(CqlIdentifier.fromCql("firstname"), "Walter");
		assertThat(where).containsEntry(CqlIdentifier.fromCql("lastname"), "White");
	}

	@Test // DATACASS-308
	void shouldWriteWhereConditionForMapIdKeyUsingEntity() {

		Where where = new Where();

		TypeWithMapId entity = new TypeWithMapId();
		entity.setFirstname("Walter");
		entity.setLastname("White");

		converter.write(entity, where, mappingContext.getRequiredPersistentEntity(TypeWithMapId.class));

		assertThat(where).containsEntry(CqlIdentifier.fromCql("firstname"), "Walter");
		assertThat(where).containsEntry(CqlIdentifier.fromCql("lastname"), "White");
	}

	@Test // DATACASS-308
	void shouldWriteEnumWhereCondition() {

		Where where = new Where();

		converter.write(Condition.MINT, where, mappingContext.getRequiredPersistentEntity(EnumPrimaryKey.class));

		assertThat(where).containsEntry(CqlIdentifier.fromCql("condition"), "MINT");
	}

	@Test // DATACASS-308
	void shouldWriteWhereConditionForMapIdKeyUsingMapId() {

		Where where = new Where();

		converter.write(id("firstname", "Walter").with("lastname", "White"), where,
				mappingContext.getRequiredPersistentEntity(TypeWithMapId.class));

		assertThat(where).containsEntry(CqlIdentifier.fromCql("firstname"), "Walter");
		assertThat(where).containsEntry(CqlIdentifier.fromCql("lastname"), "White");
	}

	@Test // DATACASS-308
	void shouldWriteWhereConditionForTypeWithPkClassKeyUsingEntity() {

		Where where = new Where();

		CompositeKey key = new CompositeKey();
		key.setFirstname("Walter");
		key.setLastname("White");

		TypeWithKeyClass entity = new TypeWithKeyClass();
		entity.setKey(key);

		converter.write(entity, where, mappingContext.getRequiredPersistentEntity(TypeWithKeyClass.class));

		assertThat(where).containsEntry(CqlIdentifier.fromCql("first_name"), "Walter");
		assertThat(where).containsEntry(CqlIdentifier.fromCql("lastname"), "White");
	}

	@Test // DATACASS-308
	void shouldFailWritingWhereConditionForTypeWithPkClassKeyUsingEntityWithNullId() {

		assertThatIllegalArgumentException().isThrownBy(() -> converter.write(new TypeWithKeyClass(), new Where(),
				mappingContext.getRequiredPersistentEntity(TypeWithKeyClass.class)));
	}

	@Test // DATACASS-308
	void shouldWriteWhereConditionForTypeWithPkClassKeyUsingKey() {

		Where where = new Where();

		CompositeKey key = new CompositeKey();
		key.setFirstname("Walter");
		key.setLastname("White");

		converter.write(key, where, mappingContext.getRequiredPersistentEntity(TypeWithKeyClass.class));

		assertThat(where).containsEntry(CqlIdentifier.fromCql("first_name"), "Walter");
		assertThat(where).containsEntry(CqlIdentifier.fromCql("lastname"), "White");
	}

	@Test // DATACASS-463
	void shouldReadTypeWithCompositePrimaryKeyCorrectly() {

		// condition, localDate
		Row row = RowMockUtil.newRowMock(column("condition", "MINT", DataTypes.TEXT),
				column("localdate", LocalDate.of(2017, 1, 2), DataTypes.DATE));

		TypeWithEnumAndLocalDateKey result = converter.read(TypeWithEnumAndLocalDateKey.class, row);

		assertThat(result.id.condition).isEqualTo(Condition.MINT);
		assertThat(result.id.localDate).isEqualTo(java.time.LocalDate.of(2017, 1, 2));
	}

	@Test // DATACASS-672
	void shouldReadTypeCompositePrimaryKeyUsingEntityInstantiatorAndPropertyPopulationInKeyCorrectly() {

		// condition, localDate
		Row row = RowMockUtil.newRowMock(column("firstname", "Walter", DataTypes.TEXT),
				column("lastname", "White", DataTypes.TEXT));

		TableWithCompositeKeyViaConstructor result = converter.read(TableWithCompositeKeyViaConstructor.class, row);

		assertThat(result.key.firstname).isEqualTo("Walter");
		assertThat(result.key.lastname).isEqualTo("White");
	}

	@Test // DATACASS-308
	void shouldWriteWhereConditionForTypeWithPkClassKeyUsingMapId() {

		Where where = new Where();

		converter.write(id("firstname", "Walter").with("lastname", "White"), where,
				mappingContext.getRequiredPersistentEntity(TypeWithKeyClass.class));

		assertThat(where).containsEntry(CqlIdentifier.fromCql("first_name"), "Walter");
		assertThat(where).containsEntry(CqlIdentifier.fromCql("lastname"), "White");
	}

	@Test // DATACASS-308
	void shouldFailWhereConditionForTypeWithPkClassKeyUsingMapIdHavingUnknownProperty() {

		assertThatIllegalArgumentException().isThrownBy(() -> converter.write(id("unknown", "Walter"), new Where(),
				mappingContext.getRequiredPersistentEntity(TypeWithMapId.class)));
	}

	@Test // DATACASS-362
	void shouldWriteWhereCompositeIdUsingCompositeKeyClass() {

		Where where = new Where();

		CompositeKey key = new CompositeKey();
		key.setFirstname("first");
		key.setLastname("last");

		converter.write(key, where, mappingContext.getRequiredPersistentEntity(TypeWithKeyClass.class));

		assertThat(where).containsEntry(CqlIdentifier.fromCql("first_name"), "first");
		assertThat(where).containsEntry(CqlIdentifier.fromCql("lastname"), "last");
	}

	@Test // DATACASS-362
	void writeWhereCompositeIdUsingCompositeKeyClassViaMapId() {

		Where where = new Where();

		MapId mapId = BasicMapId.id("firstname", "first").with("lastname", "last");

		converter.write(mapId, where, mappingContext.getRequiredPersistentEntity(TypeWithKeyClass.class));

		assertThat(where).containsEntry(CqlIdentifier.fromCql("first_name"), "first");
		assertThat(where).containsEntry(CqlIdentifier.fromCql("lastname"), "last");
	}

	@Test // DATACASS-487
	void shouldReadConvertedMap() {

		LocalDate date1 = LocalDate.of(2018, 1, 1);
		LocalDate date2 = LocalDate.of(2019, 1, 1);

		Map<String, List<LocalDate>> times = Collections.singletonMap("Europe/Paris", Arrays.asList(date1, date2));

		rowMock = RowMockUtil.newRowMock(
				RowMockUtil.column("times", times, DataTypes.mapOf(DataTypes.TEXT, DataTypes.listOf(DataTypes.DATE))));

		TypeWithConvertedMap converted = this.converter.read(TypeWithConvertedMap.class, rowMock);

		assertThat(converted.times).containsKeys(ZoneId.of("Europe/Paris"));

		List<java.time.LocalDate> convertedTimes = converted.times.get(ZoneId.of("Europe/Paris"));

		assertThat(convertedTimes).hasSize(2).hasOnlyElementsOfType(java.time.LocalDate.class);
	}

	@Test // DATACASS-487
	void shouldWriteConvertedMap() {

		java.time.LocalDate date1 = java.time.LocalDate.of(2018, 1, 1);
		java.time.LocalDate date2 = java.time.LocalDate.of(2019, 1, 1);

		TypeWithConvertedMap typeWithConvertedMap = new TypeWithConvertedMap();

		typeWithConvertedMap.times = Collections.singletonMap(ZoneId.of("Europe/Paris"), Arrays.asList(date1, date2));

		Map<CqlIdentifier, Object> insert = new LinkedHashMap<>();

		this.converter.write(typeWithConvertedMap, insert);

		List<Object> values = getValues(insert);

		assertThat(values).isNotEmpty();
		assertThat(values.get(1)).isInstanceOf(Map.class);

		Map<String, List<LocalDate>> map = (Map) values.get(1);

		assertThat(map).containsKey("Europe/Paris");
		assertThat(map.get("Europe/Paris")).hasOnlyElementsOfType(LocalDate.class);
	}

	@Test // GH-1449
	void shouldConsiderPropertyValueConverterOnRowWrite() {

		TypeWithPropertyValueConverter toInsert = new TypeWithPropertyValueConverter();
		toInsert.name = "Walter";
		toInsert.other = "Some other value";
		toInsert.tuple = new TupleWithConverter();
		toInsert.tuple.name = "Heisenberg";
		toInsert.tuple.other = "Mike";

		Map<CqlIdentifier, Object> object = new LinkedHashMap<>();
		converter.write(toInsert, object);

		assertThat(object).containsEntry(CqlIdentifier.fromCql("name"), "Other: Some other value, reversed: retlaW");
		assertThat(object).containsEntry(CqlIdentifier.fromCql("other"), "Some other value");
		assertThat(object).containsKey(CqlIdentifier.fromCql("tuple"));

		DefaultTupleValue tupleValue = (DefaultTupleValue) object.get(CqlIdentifier.fromCql("tuple"));
		assertThat(tupleValue.getString(0)).isEqualTo("Other: Mike, reversed: grebnesieH");
		assertThat(tupleValue.getString(1)).isEqualTo("Mike");
	}

	@Test // GH-1449
	void shouldConsiderPropertyValueConverterOnRowRead() {

		TupleType tupleType = DataTypes.tupleOf(DataTypes.TEXT, DataTypes.TEXT);
		DefaultTupleValue tupleValue = new DefaultTupleValue(tupleType);
		tupleValue.setString(0, "grebnesieH");
		tupleValue.setString(1, "Mike");

		rowMock = RowMockUtil.newRowMock(RowMockUtil.column("name", "retlaW", DataTypes.TEXT),
				RowMockUtil.column("other", "Some other value", DataTypes.TEXT),
				RowMockUtil.column("tuple", tupleValue, tupleType));

		TypeWithPropertyValueConverter result = converter.read(TypeWithPropertyValueConverter.class, rowMock);

		assertThat(result.name).isEqualTo("Other: Some other value, reversed: Walter");
		assertThat(result.other).isEqualTo("Some other value");
		assertThat(result.tuple).isNotNull();
		assertThat(result.tuple.name).isEqualTo("Other: Mike, reversed: Heisenberg");
		assertThat(result.tuple.other).isEqualTo("Mike");
	}

	@Test // GH-1449
	void shouldConsiderProgrammaticConverterRead() {

		CassandraCustomConversions conversions = CassandraCustomConversions.create(adapter -> {

			adapter.configurePropertyConversions(registrar -> {

				registrar.registerConverter(AllPossibleTypes.class, "id", String.class)
						.writing((from, ctx) -> from.toUpperCase()).reading((from, ctx) -> from.toLowerCase());
			});
		});

		MappingCassandraConverter converter = new MappingCassandraConverter(mappingContext);
		converter.setCustomConversions(conversions);

		rowMock = RowMockUtil.newRowMock(RowMockUtil.column("id", "WALTER", DataTypes.TEXT));

		AllPossibleTypes result = converter.read(AllPossibleTypes.class, rowMock);

		assertThat(result.getId()).isEqualTo("walter");
	}

	@Test // GH-1449
	void shouldConsiderProgrammaticConverterWrite() {

		CassandraCustomConversions conversions = CassandraCustomConversions.create(adapter -> {

			adapter.configurePropertyConversions(registrar -> {

				registrar.registerConverter(AllPossibleTypes.class, "id", String.class)
						.writing((from, ctx) -> from.toUpperCase()).reading((from, ctx) -> from.toLowerCase());
			});
		});

		MappingCassandraConverter converter = new MappingCassandraConverter(mappingContext);
		converter.setCustomConversions(conversions);

		AllPossibleTypes apt = new AllPossibleTypes();
		apt.setId("walter");

		Map<CqlIdentifier, Object> object = new LinkedHashMap<>();
		converter.write(apt, object);

		assertThat(object).containsEntry(CqlIdentifier.fromCql("id"), "WALTER");
	}

	@Test // DATACASS-189
	void writeShouldSkipTransientProperties() {

		WithTransient withTransient = new WithTransient();
		withTransient.firstname = "Foo";
		withTransient.lastname = "Bar";
		withTransient.displayName = "FooBar";

		Map<CqlIdentifier, Object> insert = new LinkedHashMap<>();

		this.converter.write(withTransient, insert);

		assertThat(insert).containsKey(CqlIdentifier.fromCql("firstname"))
				.doesNotContainKey(CqlIdentifier.fromCql("displayName"));
	}

	@Test // DATACASS-623
	void writeShouldSkipTransientReadProperties() {

		WithTransient withTransient = new WithTransient();
		withTransient.firstname = "Foo";
		withTransient.computedName = "FooBar";

		Map<CqlIdentifier, Object> insert = new LinkedHashMap<>();

		this.converter.write(withTransient, insert);

		assertThat(insert).containsKey(CqlIdentifier.fromCql("firstname"))
				.doesNotContainKey(CqlIdentifier.fromCql("computedName"));
	}

	@Test // DATACASS-741
	void shouldComputeValueInConstructor() {

		rowMock = RowMockUtil.newRowMock(RowMockUtil.column("id", "id", DataTypes.TEXT),
				RowMockUtil.column("fn", "fn", DataTypes.TEXT));

		WithValue result = this.converter.read(WithValue.class, rowMock);

		assertThat(result.id).isEqualTo("id");
		assertThat(result.firstname).isEqualTo("fn");
	}

	@Test // DATACASS-743
	void shouldConsiderCassandraTypeOnList() {

		TypeWithConvertedCollections value = new TypeWithConvertedCollections();
		value.conditionList = Arrays.asList(Condition.MINT, Condition.USED);

		Map<CqlIdentifier, Object> insert = new LinkedHashMap<>();

		this.converter.write(value, insert);

		assertThat(insert).containsEntry(CqlIdentifier.fromCql("conditionlist"), Arrays.asList(0, 1));
	}

	@Test // DATACASS-743
	void shouldConsiderCassandraTypeOnSet() {

		TypeWithConvertedCollections value = new TypeWithConvertedCollections();
		value.conditionSet = new LinkedHashSet<>(Arrays.asList(Condition.MINT, Condition.USED));

		Map<CqlIdentifier, Object> insert = new LinkedHashMap<>();

		this.converter.write(value, insert);

		assertThat(insert).containsEntry(CqlIdentifier.fromCql("conditionset"), new LinkedHashSet<>(Arrays.asList(0, 1)));
	}

	@Test // DATACASS-743
	void shouldConsiderCassandraTypeOnMap() {

		TypeWithConvertedCollections value = new TypeWithConvertedCollections();
		value.conditionMap = Collections.singletonMap(Condition.MINT, Condition.USED);

		Map<CqlIdentifier, Object> insert = new LinkedHashMap<>();

		this.converter.write(value, insert);

		assertThat(insert).containsEntry(CqlIdentifier.fromCql("conditionmap"), Collections.singletonMap(0, 1));
	}

	@Test
	void shouldConsiderColumnAnnotationOnConstructor() {

		rowMock = RowMockUtil.newRowMock(RowMockUtil.column("fn", "Walter", DataTypes.ASCII),
				RowMockUtil.column("firstname", "Heisenberg", DataTypes.ASCII),
				RowMockUtil.column("lastname", "White", DataTypes.ASCII));

		WithColumnAnnotationInConstructor converted = this.converter.read(WithColumnAnnotationInConstructor.class, rowMock);

		assertThat(converted.firstname).isEqualTo("Walter");
		assertThat(converted.lastname).isEqualTo("White");
	}

	@Test
	void shouldConsiderElementAnnotationOnConstructor() {

		DefaultTupleValue value = new DefaultTupleValue(
				new DefaultTupleType(Arrays.asList(DataTypes.ASCII, DataTypes.ASCII, DataTypes.ASCII)));

		value.setString(0, "Zero");
		value.setString(1, "One");
		value.setString(2, "Two");

		rowMock = RowMockUtil.newRowMock(RowMockUtil.column("firstname", "Heisenberg", DataTypes.ASCII),
				RowMockUtil.column("tuple", value, value.getType()));

		WithMappedTuple converted = this.converter.read(WithMappedTuple.class, rowMock);

		assertThat(converted.firstname).isEqualTo("Heisenberg");
		assertThat(converted.tuple.firstname).isEqualTo("Two");
	}

	@Test // GH-1202
	void shouldConsiderNestedProjections() {

		DefaultTupleValue value = new DefaultTupleValue(
				new DefaultTupleType(Arrays.asList(DataTypes.ASCII, DataTypes.ASCII, DataTypes.ASCII)));

		value.setString(0, "Zero");
		value.setString(1, "One");
		value.setString(2, "Two");

		EntityProjectionIntrospector introspector = EntityProjectionIntrospector.create(
				new SpelAwareProxyProjectionFactory(), EntityProjectionIntrospector.ProjectionPredicate.typeHierarchy(),
				this.mappingContext);

		rowMock = RowMockUtil.newRowMock(RowMockUtil.column("firstname", "Heisenberg", DataTypes.ASCII),
				RowMockUtil.column("tuple", value, value.getType()));

		EntityProjection<WithMappedTupleDtoProjection, WithMappedTuple> projection = introspector
				.introspect(WithMappedTupleDtoProjection.class, WithMappedTuple.class);

		WithMappedTupleDtoProjection result = this.converter.project(projection, rowMock);

		assertThat(result.firstname()).isEqualTo("Heisenberg");
		assertThat(result.tuple().getOne()).isEqualTo("One");
	}

	@Test // GH-1202
	void shouldCreateDtoProjectionsThroughConstructor() {

		DefaultTupleValue value = new DefaultTupleValue(
				new DefaultTupleType(Arrays.asList(DataTypes.ASCII, DataTypes.ASCII, DataTypes.ASCII)));

		value.setString(0, "Zero");
		value.setString(1, "One");
		value.setString(2, "Two");

		EntityProjectionIntrospector introspector = EntityProjectionIntrospector.create(
				new SpelAwareProxyProjectionFactory(), EntityProjectionIntrospector.ProjectionPredicate.typeHierarchy(),
				this.mappingContext);

		rowMock = RowMockUtil.newRowMock(RowMockUtil.column("firstname", "Heisenberg", DataTypes.ASCII),
				RowMockUtil.column("tuple", value, value.getType()));

		EntityProjection<TupleAndNameProjection, WithMappedTuple> projection = introspector
				.introspect(TupleAndNameProjection.class, WithMappedTuple.class);

		TupleAndNameProjection result = this.converter.project(projection, rowMock);

		assertThat(result.name().firstname()).isEqualTo("Heisenberg");
		assertThat(result.tuple().zero).isEqualTo("Zero");
		assertThat(result.tuple().one).isEqualTo("One");
	}

	@Test // GH-1472
	void projectShouldReadDtoProjectionPropertiesOnlyOnce() {

		ByteBuffer number = ByteBuffer.allocate(8);
		number.putDouble(1.2d);
		number.flip();

		rowMock = RowMockUtil.newRowMock(RowMockUtil.column("number", number, DataTypes.BLOB));

		EntityProjectionIntrospector introspector = EntityProjectionIntrospector.create(
				new SpelAwareProxyProjectionFactory(), EntityProjectionIntrospector.ProjectionPredicate.typeHierarchy(),
				this.mappingContext);

		EntityProjection<DoubleHolderDto, WithDoubleHolder> projection = introspector.introspect(DoubleHolderDto.class,
				WithDoubleHolder.class);

		DoubleHolderDto result = this.converter.project(projection, rowMock);

		assertThat(result.number.number).isCloseTo(1.2, Percentage.withPercentage(1));
	}

	@Test // GH-1471
	void propertyValueConversionsCacheShouldConsiderPropertyEquality() {

		rowMock = RowMockUtil.newRowMock(RowMockUtil.column("fn", "Walter", DataTypes.ASCII),
				RowMockUtil.column("lastname", "White", DataTypes.ASCII),
				RowMockUtil.column("n_firstname", "Heisenberg", DataTypes.ASCII),
				RowMockUtil.column("c_firstname", "", DataTypes.ASCII), // required as marker to prevent Nullable embedded being
																																// null
				RowMockUtil.column("c_fn", "Nested Walter", DataTypes.ASCII),
				RowMockUtil.column("c_lastname", "Nested White", DataTypes.ASCII));

		SimplePropertyValueConversions spvc = (SimplePropertyValueConversions) converter.getCustomConversions()
				.getPropertyValueConversions();
		DirectFieldAccessor accessor = new DirectFieldAccessor(spvc.getConverterFactory());
		Map<?, ?> cache = (Map<?, ?>) new DirectFieldAccessor(accessor.getPropertyValue("cache"))
				.getPropertyValue("perPropertyCache");

		assertThat(cache).isEmpty();

		SourceForPersistentPropertyDerivation converted = this.converter.read(SourceForPersistentPropertyDerivation.class,
				rowMock);

		assertThat(converted.firstname).isEqualTo("Walter");
		assertThat(converted.lastname).isEqualTo("White");
		assertThat(converted.name.firstname()).isEqualTo("Heisenberg");
		assertThat(converted.constructor.firstname).isEqualTo("Nested Walter");
		assertThat(converted.constructor.lastname).isEqualTo("Nested White");

		assertThat(cache).hasSize(5);

		for (int i = 0; i < 10; i++) {
			this.converter.read(SourceForPersistentPropertyDerivation.class, rowMock);
		}

		assertThat(cache).hasSize(5);
	}

	private static List<Object> getValues(Map<CqlIdentifier, Object> statement) {
		return new ArrayList<>(statement.values());
	}

	private static Collection<Object> getWhereValues(Where update) {
		return update.values();
	}

	@Table
	private static class EnumToOrdinalMapping {

		@PrimaryKey private String id;

		@CassandraType(type = CassandraType.Name.INT) private Condition asOrdinal;

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		private Condition getAsOrdinal() {
			return asOrdinal;
		}

		private void setAsOrdinal(Condition asOrdinal) {
			this.asOrdinal = asOrdinal;
		}
	}

	@Table
	private static class WithEnumColumns {

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

		private void setCondition(Condition condition) {
			this.condition = condition;
		}
	}

	@PrimaryKeyClass
	public static class EnumCompositePrimaryKey implements Serializable {

		@PrimaryKeyColumn(ordinal = 1, type = PrimaryKeyType.PARTITIONED) private Condition condition;

		private EnumCompositePrimaryKey() {}

		public EnumCompositePrimaryKey(Condition condition) {
			this.condition = condition;
		}

		public Condition getCondition() {
			return condition;
		}

		private void setCondition(Condition condition) {
			this.condition = condition;
		}
	}

	@PrimaryKeyClass
	record EnumAndDateCompositePrimaryKey(
			@PrimaryKeyColumn(ordinal = 1, type = PrimaryKeyType.PARTITIONED) Condition condition,
			@PrimaryKeyColumn(ordinal = 2, type = PrimaryKeyType.PARTITIONED) java.time.LocalDate localDate)
			implements
				Serializable {

	}

	record TypeWithEnumAndLocalDateKey(@PrimaryKey EnumAndDateCompositePrimaryKey id) {

	}

	@Table
	private static class EnumPrimaryKey {

		@PrimaryKey private Condition condition;

		public Condition getCondition() {
			return condition;
		}

		private void setCondition(Condition condition) {
			this.condition = condition;
		}
	}

	@Table
	private static class CompositeKeyThing {

		@PrimaryKey private EnumCompositePrimaryKey key;

		private CompositeKeyThing() {}

		public CompositeKeyThing(EnumCompositePrimaryKey key) {
			this.key = key;
		}

		public EnumCompositePrimaryKey getKey() {
			return key;
		}

		private void setKey(EnumCompositePrimaryKey key) {
			this.key = key;
		}
	}

	public enum Condition {
		MINT, USED
	}

	@PrimaryKeyClass
	private static class CompositeKeyWithPropertyAccessors {

		@PrimaryKeyColumn(type = PrimaryKeyType.PARTITIONED) private String firstname;
		@PrimaryKeyColumn private String lastname;
	}

	@Table
	public static class TableWithCompositeKeyViaConstructor {

		@PrimaryKey private final CompositeKeyWithPropertyAccessors key;

		public TableWithCompositeKeyViaConstructor(CompositeKeyWithPropertyAccessors key) {
			this.key = key;
		}
	}

	@Table
	private static class TypeWithLocalDate {

		@PrimaryKey private String id;

		private java.time.LocalDate localDate;
		private java.time.LocalDateTime localDateTime;

		private List<java.time.LocalDate> list;
		private Set<java.time.LocalDate> set;
	}

	/**
	 * Uses Cassandra's {@link CassandraType.Name#DATE} which maps by default to {@link LocalDate}
	 */
	@Table
	public static class TypeWithLocalDateMappedToDate {

		@PrimaryKey private String id;

		@CassandraType(type = CassandraType.Name.DATE) java.time.LocalDate localDate;

		public TypeWithLocalDateMappedToDate(String id, LocalDate localDate) {
			this.id = id;
			this.localDate = localDate;
		}
	}

	@Table
	private static class TypeWithInstant {

		@PrimaryKey private String id;

		private Instant instant;
	}

	@Table
	private static class TypeWithZoneId {

		@PrimaryKey private String id;

		private ZoneId zoneId;
	}

	@Table
	private static class TypeWithConvertedMap {

		@PrimaryKey private String id;

		private Map<ZoneId, List<java.time.LocalDate>> times;
	}

	private static class TypeWithPropertyValueConverter {

		@ValueConverter(ReversingValueConverter.class) private String name;

		private String other;

		private TupleWithConverter tuple;
	}

	@Tuple
	private static class TupleWithConverter {

		@Element(0)
		@ValueConverter(ReversingValueConverter.class) private String name;

		@Element(1) private String other;

	}

	static class ReversingValueConverter implements CassandraValueConverter<String, String> {

		@Nullable
		@Override
		public String read(@Nullable String value, CassandraConversionContext context) {
			return String.format("Other: %s, reversed: %s", context.getValue("other"), reverse(value));
		}

		@Nullable
		@Override
		public String write(@Nullable String value, CassandraConversionContext context) {
			return String.format("Other: %s, reversed: %s", context.getValue("other"), reverse(value));
		}

		private String reverse(String source) {

			if (source == null) {
				return null;
			}

			return new StringBuilder(source).reverse().toString();
		}
	}

	private static class WithTransient {

		@Id String id;

		private String firstname;
		private String lastname;
		@Transient private String displayName;
		@ReadOnlyProperty private String computedName;
	}

	private static class TypeWithConvertedCollections {

		@CassandraType(type = CassandraType.Name.LIST,
				typeArguments = CassandraType.Name.INT) private List<Condition> conditionList;

		@CassandraType(type = CassandraType.Name.SET,
				typeArguments = CassandraType.Name.INT) private Set<Condition> conditionSet;

		@CassandraType(type = CassandraType.Name.MAP, typeArguments = { CassandraType.Name.INT,
				CassandraType.Name.INT }) private Map<Condition, Condition> conditionMap;

	}

	private static class WithValue {

		private final @Id String id;
		private final @Transient String firstname;

		private WithValue(String id, @Value("#root.getString(1)") String firstname) {
			this.id = id;
			this.firstname = firstname;
		}
	}

	private static class WithColumnAnnotationInConstructor {

		String firstname;
		final @Transient String lastname;

		public WithColumnAnnotationInConstructor(@Column("fn") String firstname, @Column("lastname") String lastname) {
			this.firstname = firstname;
			this.lastname = lastname;
		}
	}

	private static class SourceForPersistentPropertyDerivation {

		String firstname;
		final @Transient String lastname;
		@Embedded.Nullable("n_") Name name;
		@Embedded.Nullable("c_") WithColumnAnnotationInConstructor constructor;

		public SourceForPersistentPropertyDerivation(@Column("fn") String fn, @Column("lastname") String lastname) {
			this.firstname = fn;
			this.lastname = lastname;
		}
	}

	private static class WithMappedTuple {

		String firstname;
		@Embedded.Nullable Name name;
		TupleWithElementAnnotationInConstructor tuple;
	}

	private record Name(String firstname) {

	}

	record WithMappedTupleDtoProjection(String firstname, TupleProjection tuple) {
	}

	private record TupleAndNameProjection(@Embedded.Nullable Name name, TupleWithElementAnnotationInConstructor tuple) {

	}

	private interface TupleProjection {

		String getZero();

		String getOne();
	}

	@Tuple
	private static class TupleWithElementAnnotationInConstructor {

		@Element(0) String zero;
		@Element(1) String one;
		@Element(2) String two;

		@Transient String firstname;

		public TupleWithElementAnnotationInConstructor(@Element(2) String firstname) {
			this.firstname = firstname;
		}
	}

	static class WithNullableEmbeddedType {

		String id;

		@Embedded.Nullable EmbeddedWithSimpleTypes nested;

		@Override
		public String toString() {
			StringBuffer sb = new StringBuffer();
			sb.append(getClass().getSimpleName());
			sb.append(" [id='").append(id).append('\'');
			sb.append(", nested=").append(nested);
			sb.append(']');
			return sb.toString();
		}
	}

	static class WithPrefixedNullableEmbeddedType {

		String id;

		@Embedded.Nullable("prefix") EmbeddedWithSimpleTypes nested;

		@Override
		public String toString() {
			StringBuffer sb = new StringBuffer();
			sb.append(getClass().getSimpleName());
			sb.append(" [id='").append(id).append('\'');
			sb.append(", nested=").append(nested);
			sb.append(']');
			return sb.toString();
		}
	}

	static class WithEmptyEmbeddedType {

		String id;

		@Embedded.Empty EmbeddedWithSimpleTypes nested;

		@Override
		public String toString() {
			StringBuffer sb = new StringBuffer();
			sb.append(getClass().getSimpleName());
			sb.append(" [id='").append(id).append('\'');
			sb.append(", nested=").append(nested);
			sb.append(']');
			return sb.toString();
		}
	}

	static class EmbeddedWithSimpleTypes {

		String firstname;
		Integer age;
		@Transient String displayName;

		public EmbeddedWithSimpleTypes() {}

		public EmbeddedWithSimpleTypes(String firstname, Integer age, String displayName) {
			this.firstname = firstname;
			this.age = age;
			this.displayName = displayName;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o)
				return true;
			if (o == null || getClass() != o.getClass())
				return false;

			EmbeddedWithSimpleTypes that = (EmbeddedWithSimpleTypes) o;

			if (!ObjectUtils.nullSafeEquals(firstname, that.firstname)) {
				return false;
			}
			if (!ObjectUtils.nullSafeEquals(age, that.age)) {
				return false;
			}
			return ObjectUtils.nullSafeEquals(displayName, that.displayName);
		}

		@Override
		public int hashCode() {
			int result = ObjectUtils.nullSafeHashCode(firstname);
			result = 31 * result + ObjectUtils.nullSafeHashCode(age);
			result = 31 * result + ObjectUtils.nullSafeHashCode(displayName);
			return result;
		}
	}

	@Test // DATACASS-167
	void writeFlattensEmbeddedType() {

		WithNullableEmbeddedType entity = new WithNullableEmbeddedType();
		entity.id = "id-1";
		entity.nested = new EmbeddedWithSimpleTypes();
		entity.nested.firstname = "fn";
		entity.nested.age = 30;
		entity.nested.displayName = "dp-name";

		Map<CqlIdentifier, Object> sink = new LinkedHashMap<>();

		converter.write(entity, sink);

		assertThat(sink) //
				.containsEntry(CqlIdentifier.fromCql("id"), "id-1") //
				.containsEntry(CqlIdentifier.fromCql("age"), 30) //
				.containsEntry(CqlIdentifier.fromCql("firstname"), "fn") //
				.doesNotContainKey(CqlIdentifier.fromCql("displayName"));
	}

	@Test // DATACASS-167
	void writePrefixesEmbeddedType() {

		WithPrefixedNullableEmbeddedType entity = new WithPrefixedNullableEmbeddedType();
		entity.id = "id-1";
		entity.nested = new EmbeddedWithSimpleTypes();
		entity.nested.firstname = "fn";
		entity.nested.age = 30;
		entity.nested.displayName = "dp-name";

		Map<CqlIdentifier, Object> sink = new LinkedHashMap<>();

		converter.write(entity, sink);

		assertThat(sink) //
				.containsEntry(CqlIdentifier.fromCql("id"), "id-1") //
				.containsEntry(CqlIdentifier.fromCql("prefixage"), 30) //
				.containsEntry(CqlIdentifier.fromCql("prefixfirstname"), "fn") //
				.doesNotContainKey(CqlIdentifier.fromCql("displayName"));
	}

	@Test // DATACASS-167
	void writeNullEmbeddedType() {

		WithNullableEmbeddedType entity = new WithNullableEmbeddedType();
		entity.id = "id-1";
		entity.nested = null;

		Map<CqlIdentifier, Object> sink = new LinkedHashMap<>();

		converter.write(entity, sink);

		assertThat(sink) //
				.containsEntry(CqlIdentifier.fromCql("id"), "id-1") //
				.doesNotContainKey(CqlIdentifier.fromCql("age")) //
				.doesNotContainKey(CqlIdentifier.fromCql("firstname")) //
				.doesNotContainKey(CqlIdentifier.fromCql("displayName"));
	}

	@Test // DATACASS-167
	void readEmbeddedType() {

		Row source = RowMockUtil.newRowMock(column("id", "id-1", DataTypes.TEXT), column("age", 30, DataTypes.INT),
				column("firstname", "fn", DataTypes.TEXT));

		WithNullableEmbeddedType target = converter.read(WithNullableEmbeddedType.class, source);
		assertThat(target.nested).isEqualTo(new EmbeddedWithSimpleTypes("fn", 30, null));
	}

	@Test // DATACASS-167
	void readPrefixedEmbeddedType() {

		Row source = RowMockUtil.newRowMock(column("id", "id-1", DataTypes.TEXT), column("prefixage", 30, DataTypes.INT),
				column("prefixfirstname", "fn", DataTypes.TEXT));

		WithPrefixedNullableEmbeddedType target = converter.read(WithPrefixedNullableEmbeddedType.class, source);
		assertThat(target.nested).isEqualTo(new EmbeddedWithSimpleTypes("fn", 30, null));
	}

	@Test // DATACASS-167
	void readEmbeddedTypeWhenSourceDoesNotContainValues() {

		Row source = RowMockUtil.newRowMock(column("id", "id-1", DataTypes.TEXT));

		WithNullableEmbeddedType target = converter.read(WithNullableEmbeddedType.class, source);
		assertThat(target.nested).isNull();
	}

	@Test // DATACASS-1181
	void shouldApplyCustomConverterToMapLikeType() {

		CassandraCustomConversions conversions = new CassandraCustomConversions(
				Arrays.asList(JsonToStringConverter.INSTANCE, StringToJsonConverter.INSTANCE));

		this.mappingContext = new CassandraMappingContext();
		this.mappingContext.setSimpleTypeHolder(conversions.getSimpleTypeHolder());

		this.converter = new MappingCassandraConverter(mappingContext);
		this.converter.setCustomConversions(conversions);
		this.converter.afterPropertiesSet();

		Row source = RowMockUtil.newRowMock(column("thejson", "{\"hello\":\"world\"}", DataTypes.TEXT));

		TypeWithJsonObject target = converter.read(TypeWithJsonObject.class, source);
		assertThat(target.theJson).isNotNull();
		assertThat(target.theJson.get("hello")).isEqualTo("world");
	}

	@Test // GH-1240
	void shouldReadOpenProjectionWithNestedObject() {

		com.datastax.oss.driver.api.core.type.UserDefinedType authorType = UserDefinedTypeBuilder.forName("author")
				.withField("firstName", DataTypes.TEXT).withField("lastName", DataTypes.TEXT).build();

		UdtValue udtValue = authorType.newValue().setString("firstName", "Walter").setString("lastName", "White");

		Row source = RowMockUtil.newRowMock(column("id", "id-1", DataTypes.TEXT), column("name", "my-book", DataTypes.INT),
				column("author", udtValue, authorType));

		EntityProjectionIntrospector introspector = EntityProjectionIntrospector.create(converter.getProjectionFactory(),
				EntityProjectionIntrospector.ProjectionPredicate.typeHierarchy()
						.and((target, underlyingType) -> !converter.getCustomConversions().isSimpleType(target)),
				mappingContext);

		BookProjection projection = converter.project(introspector.introspect(BookProjection.class, Book.class), source);

		assertThat(projection.getName()).isEqualTo("my-book by Walter White");
	}

	static class TypeWithJsonObject {

		JSONObject theJson;
	}

	enum StringToJsonConverter implements Converter<String, JSONObject> {
		INSTANCE;

		@Override
		public JSONObject convert(String source) {
			try {
				return (JSONObject) new JSONParser().parse(source);
			} catch (ParseException e) {
				throw new RuntimeException(e);
			}
		}

	}

	enum JsonToStringConverter implements Converter<JSONObject, String> {
		INSTANCE;

		@Override
		public String convert(JSONObject source) {
			return source.toJSONString();
		}

	}

	interface BookProjection {

		@Value("#{target.name + ' by ' + target.author.firstName + ' ' + target.author.lastName}")
		String getName();
	}

	static class Book {

		@Id String id;

		String name;

		Author author = new Author();

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public Author getAuthor() {
			return author;
		}

		public void setAuthor(Author author) {
			this.author = author;
		}
	}

	@UserDefinedType
	static class Author {

		@Id String id;

		String firstName;

		String lastName;

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public String getFirstName() {
			return firstName;
		}

		public void setFirstName(String firstName) {
			this.firstName = firstName;
		}

		public String getLastName() {
			return lastName;
		}

		public void setLastName(String lastName) {
			this.lastName = lastName;
		}
	}

	@ReadingConverter
	static class ByteBufferToDoubleHolderConverter implements Converter<ByteBuffer, DoubleHolder> {

		@Override
		public DoubleHolder convert(ByteBuffer source) {
			return new DoubleHolder(source.getDouble());
		}
	}

	record DoubleHolder(double number) {

	}

	static class WithDoubleHolder {
		DoubleHolder number;
	}

	static class DoubleHolderDto {
		DoubleHolder number;

		public DoubleHolderDto(DoubleHolder number) {
			this.number = number;
		}
	}

}
