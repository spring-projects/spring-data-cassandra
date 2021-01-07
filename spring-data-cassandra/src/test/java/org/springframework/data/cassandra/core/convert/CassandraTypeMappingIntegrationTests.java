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
import static org.junit.Assume.*;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.domain.AllPossibleTypes;
import org.springframework.data.cassandra.repository.support.SchemaTestUtils;
import org.springframework.data.cassandra.support.CassandraVersion;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTests;
import org.springframework.data.util.Version;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.TupleType;

/**
 * Integration tests for type mapping using {@link CassandraOperations}.
 *
 * @author Mark Paluch
 * @author Hurelhuyag
 * @soundtrack DJ THT meets Scarlet - Live 2 Dance (Extended Mix) (Zgin Remix)
 */
@SuppressWarnings("Since15")
public class CassandraTypeMappingIntegrationTests extends AbstractKeyspaceCreatingIntegrationTests {

	private static final Version VERSION_3_10 = Version.parse("3.10");
	private static boolean initialized = false;

	private CassandraOperations operations;
	private Version cassandraVersion;

	@BeforeEach
	void before() {

		operations = new CassandraTemplate(session);
		cassandraVersion = CassandraVersion.get(session);

		SchemaTestUtils.potentiallyCreateTableFor(AllPossibleTypes.class, operations);
		SchemaTestUtils.potentiallyCreateTableFor(TimeEntity.class, operations);

		if (!initialized) {
			initialized = true;
			operations.getCqlOperations().execute("DROP TABLE IF EXISTS ListOfTuples;");
			operations.getCqlOperations()
					.execute("CREATE TABLE ListOfTuples (id varchar PRIMARY KEY, tuples frozen<list<tuple<varchar, bigint>>>);");
		}

		SchemaTestUtils.truncate(AllPossibleTypes.class, operations);
		SchemaTestUtils.truncate(TimeEntity.class, operations);
		SchemaTestUtils.truncate(ListOfTuples.class, operations);

		if (cassandraVersion.isGreaterThanOrEqualTo(VERSION_3_10)) {

			SchemaTestUtils.potentiallyCreateTableFor(WithDuration.class, operations);
			SchemaTestUtils.truncate(WithDuration.class, operations);
		}
	}

	@Test // DATACASS-280
	void shouldReadAndWriteInetAddress() throws Exception {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setInet(InetAddress.getByName("127.0.0.1"));

		operations.insert(entity);
		AllPossibleTypes loaded = load(entity);

		assertThat(loaded.getInet()).isEqualTo(entity.getInet());
	}

	@Test // DATACASS-280
	void shouldReadAndWriteUUID() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setUuid(UUID.randomUUID());

		operations.insert(entity);
		AllPossibleTypes loaded = load(entity);

		assertThat(loaded.getUuid()).isEqualTo(entity.getUuid());
	}

	@Test // DATACASS-280
	void shouldReadAndWriteBoxedShort() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBoxedShort(Short.MAX_VALUE);

		operations.insert(entity);
		AllPossibleTypes loaded = load(entity);

		assertThat(loaded.getBoxedShort()).isEqualTo(entity.getBoxedShort());
	}

	@Test // DATACASS-280
	void shouldReadAndWritePrimitiveShort() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setPrimitiveShort(Short.MAX_VALUE);

		operations.insert(entity);
		AllPossibleTypes loaded = load(entity);

		assertThat(loaded.getPrimitiveShort()).isEqualTo(entity.getPrimitiveShort());
	}

	@Test // DATACASS-271
	void shouldReadAndWriteBoxedByte() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBoxedByte(Byte.MAX_VALUE);

		operations.insert(entity);
		AllPossibleTypes loaded = load(entity);

		assertThat(loaded.getBoxedByte()).isEqualTo(entity.getBoxedByte());
	}

	@Test // DATACASS-271
	void shouldReadAndWritePrimitiveByte() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setPrimitiveByte(Byte.MAX_VALUE);

		operations.insert(entity);
		AllPossibleTypes loaded = load(entity);

		assertThat(loaded.getPrimitiveByte()).isEqualTo(entity.getPrimitiveByte());
	}

	@Test // DATACASS-280
	void shouldReadAndWriteBoxedLong() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBoxedLong(Long.MAX_VALUE);

		operations.insert(entity);
		AllPossibleTypes loaded = load(entity);

		assertThat(loaded.getBoxedLong()).isEqualTo(entity.getBoxedLong());
	}

	@Test // DATACASS-280
	void shouldReadAndWritePrimitiveLong() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setPrimitiveLong(Long.MAX_VALUE);

		operations.insert(entity);
		AllPossibleTypes loaded = load(entity);

		assertThat(loaded.getPrimitiveLong()).isEqualTo(entity.getPrimitiveLong());
	}

	@Test // DATACASS-280
	void shouldReadAndWriteBoxedInteger() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBoxedInteger(Integer.MAX_VALUE);

		operations.insert(entity);
		AllPossibleTypes loaded = load(entity);

		assertThat(loaded.getBoxedInteger()).isEqualTo(entity.getBoxedInteger());
	}

	@Test // DATACASS-280
	void shouldReadAndWritePrimitiveInteger() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setPrimitiveInteger(Integer.MAX_VALUE);

		operations.insert(entity);
		AllPossibleTypes loaded = load(entity);

		assertThat(loaded.getPrimitiveInteger()).isEqualTo(entity.getPrimitiveInteger());
	}

	@Test // DATACASS-280
	void shouldReadAndWriteBoxedFloat() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBoxedFloat(Float.MAX_VALUE);

		operations.insert(entity);
		AllPossibleTypes loaded = load(entity);

		assertThat(loaded.getBoxedFloat()).isEqualTo(entity.getBoxedFloat());
	}

	@Test // DATACASS-280
	void shouldReadAndWritePrimitiveFloat() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setPrimitiveFloat(Float.MAX_VALUE);

		operations.insert(entity);
		AllPossibleTypes loaded = load(entity);

		assertThat(loaded.getPrimitiveFloat()).isEqualTo(entity.getPrimitiveFloat());
	}

	@Test // DATACASS-280
	void shouldReadAndWriteBoxedDouble() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBoxedDouble(Double.MAX_VALUE);

		operations.insert(entity);
		AllPossibleTypes loaded = load(entity);

		assertThat(loaded.getBoxedDouble()).isEqualTo(entity.getBoxedDouble());
	}

	@Test // DATACASS-280
	void shouldReadAndWritePrimitiveDouble() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setPrimitiveDouble(Double.MAX_VALUE);

		operations.insert(entity);
		AllPossibleTypes loaded = load(entity);

		assertThat(loaded.getPrimitiveDouble()).isEqualTo(entity.getPrimitiveDouble());
	}

	@Test // DATACASS-280
	void shouldReadAndWriteBoxedBoolean() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBoxedBoolean(Boolean.TRUE);

		operations.insert(entity);
		AllPossibleTypes loaded = load(entity);

		assertThat(loaded.getBoxedBoolean()).isEqualTo(entity.getBoxedBoolean());
	}

	@Test // DATACASS-280
	void shouldReadAndWritePrimitiveBoolean() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setPrimitiveBoolean(Boolean.TRUE);

		operations.insert(entity);
		AllPossibleTypes loaded = load(entity);

		assertThat(loaded.isPrimitiveBoolean()).isEqualTo(entity.isPrimitiveBoolean());
	}

	@Test // DATACASS-280, DATACASS-271
	void shouldReadAndWriteTimestamp() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setTimestamp(new Date(1));

		operations.insert(entity);
		AllPossibleTypes loaded = load(entity);

		assertThat(loaded.getTimestamp()).isEqualTo(entity.getTimestamp());
	}

	@Test // DATACASS-271
	void shouldReadAndWriteDate() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setDate(LocalDate.ofEpochDay(1));

		operations.insert(entity);
		AllPossibleTypes loaded = load(entity);

		assertThat(loaded.getDate()).isEqualTo(entity.getDate());
	}

	@Test // DATACASS-280
	void shouldReadAndWriteBigInteger() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBigInteger(new BigInteger("123456"));

		operations.insert(entity);
		AllPossibleTypes loaded = load(entity);

		assertThat(loaded.getBigInteger()).isEqualTo(entity.getBigInteger());
	}

	@Test // DATACASS-280
	void shouldReadAndWriteBigDecimal() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBigDecimal(new BigDecimal("123456.7890123"));

		operations.insert(entity);
		AllPossibleTypes loaded = load(entity);

		assertThat(loaded.getBigDecimal()).isEqualTo(entity.getBigDecimal());
	}

	@Test // DATACASS-280
	void shouldReadAndWriteBlob() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBlob(ByteBuffer.wrap("Hello".getBytes()));

		operations.insert(entity);
		AllPossibleTypes loaded = load(entity);

		ByteBuffer blob = loaded.getBlob();
		byte[] bytes = new byte[blob.remaining()];
		blob.get(bytes);
		assertThat(new String(bytes)).isEqualTo("Hello");
	}

	@Test // DATACASS-280
	void shouldReadAndWriteSetOfString() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setSetOfString(Collections.singleton("hello"));

		operations.insert(entity);
		AllPossibleTypes loaded = load(entity);

		assertThat(loaded.getSetOfString()).isEqualTo(entity.getSetOfString());
	}

	@Test // DATACASS-280
	void shouldReadAndWriteEmptySetOfString() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setSetOfString(new HashSet<>());

		operations.insert(entity);
		AllPossibleTypes loaded = load(entity);

		assertThat(loaded.getSetOfString()).isNull();
	}

	@Test // DATACASS-280
	void shouldReadAndWriteListOfString() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setListOfString(Collections.singletonList("hello"));

		operations.insert(entity);
		AllPossibleTypes loaded = load(entity);

		assertThat(loaded.getListOfString()).isEqualTo(entity.getListOfString());
	}

	@Test // DATACASS-280
	void shouldReadAndWriteEmptyListOfString() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setListOfString(new ArrayList<>());

		operations.insert(entity);
		AllPossibleTypes loaded = load(entity);

		assertThat(loaded.getListOfString()).isNull();
	}

	@Test // DATACASS-280
	void shouldReadAndWriteMapOfString() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setMapOfString(Collections.singletonMap("hello", "world"));

		operations.insert(entity);
		AllPossibleTypes loaded = load(entity);

		assertThat(loaded.getMapOfString()).isEqualTo(entity.getMapOfString());
	}

	@Test // DATACASS-280
	void shouldReadAndWriteEmptyMapOfString() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setMapOfString(new HashMap<>());

		operations.insert(entity);
		AllPossibleTypes loaded = load(entity);

		assertThat(loaded.getMapOfString()).isNull();
	}

	@Test // DATACASS-280
	void shouldReadAndWriteEnum() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setAnEnum(Condition.MINT);

		operations.insert(entity);
		AllPossibleTypes loaded = load(entity);

		assertThat(loaded.getAnEnum()).isEqualTo(entity.getAnEnum());
	}

	@Test // DATACASS-280
	void shouldReadAndWriteListOfEnum() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setListOfEnum(Collections.singletonList(Condition.MINT));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(entity.getId(), AllPossibleTypes.class);

		assertThat(loaded.getListOfEnum()).contains(Condition.MINT);
	}

	@Test // DATACASS-280
	void shouldReadAndWriteSetOfEnum() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setSetOfEnum(Collections.singleton(Condition.MINT));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(entity.getId(), AllPossibleTypes.class);

		assertThat(loaded.getSetOfEnum()).contains(Condition.MINT);
	}

	@Test // DATACASS-284
	void shouldReadAndWriteTupleType() {

		TupleType tupleType = DataTypes.tupleOf(DataTypes.TEXT, DataTypes.BIGINT);

		AllPossibleTypes entity = new AllPossibleTypes("1");

		entity.setTupleValue(tupleType.newValue("foo", 23L));

		operations.insert(entity);

		AllPossibleTypes loaded = operations.selectOneById(entity.getId(), AllPossibleTypes.class);

		assertThat(loaded.getTupleValue().getObject(0)).isEqualTo("foo");
		assertThat(loaded.getTupleValue().getObject(1)).isEqualTo(23L);
	}

	@Test // DATACASS-284
	void shouldReadAndWriteListOfTuples() {

		TupleType tupleType = DataTypes.tupleOf(DataTypes.TEXT, DataTypes.BIGINT);

		ListOfTuples entity = new ListOfTuples();

		entity.setId("foo");
		entity.setTuples(Arrays.asList(tupleType.newValue("foo", 23L), tupleType.newValue("bar", 42L)));

		operations.insert(entity);

		ListOfTuples loaded = operations.selectOneById(entity.getId(), ListOfTuples.class);

		assertThat(loaded.getTuples().get(0).getObject(0)).isEqualTo("foo");
		assertThat(loaded.getTuples().get(1).getObject(0)).isEqualTo("bar");
	}

	@Test // DATACASS-271
	void shouldReadAndWriteTime() {

		// writing of time is not supported with Insert/Update statements as they mix up types.
		// The only way to insert a time right now seems a PreparedStatement
		String id = "1";
		long time = 21312214L;

		operations.getCqlOperations()
				.execute(SimpleStatement.newInstance("INSERT INTO timeentity (id, time) values(?,?)", id, time));

		TimeEntity loaded = operations.selectOneById(id, TimeEntity.class);

		assertThat(loaded.getTime()).isEqualTo(time);
	}

	@Test // DATACASS-296
	void shouldReadAndWriteLocalDate() {

		AllPossibleTypes entity = new AllPossibleTypes("1");

		entity.setDate(java.time.LocalDate.of(2010, 7, 4));

		operations.insert(entity);

		AllPossibleTypes loaded = load(entity);

		assertThat(loaded.getDate()).isEqualTo(entity.getDate());
	}

	@Test // DATACASS-296
	void shouldReadAndWriteLocalDateTime() {

		AllPossibleTypes entity = new AllPossibleTypes("1");

		entity.setLocalDateTime(java.time.LocalDateTime.of(2010, 7, 4, 1, 2, 3));

		operations.insert(entity);

		AllPossibleTypes loaded = load(entity);

		assertThat(loaded.getLocalDateTime()).isEqualTo(entity.getLocalDateTime());
	}

	@Test // DATACASS-296, DATACASS-563
	void shouldReadAndWriteLocalTime() {

		assumeTrue(cassandraVersion.isGreaterThanOrEqualTo(VERSION_3_10));

		AllPossibleTypes entity = new AllPossibleTypes("1");

		entity.setTime(java.time.LocalTime.of(1, 2, 3));

		operations.insert(entity);

		AllPossibleTypes loaded = load(entity);

		assertThat(loaded.getTime()).isEqualTo(entity.getTime());
	}

	@Test // DATACASS-694, DATACASS-727
	void shouldReadLocalTimeFromDriver() {

		assumeTrue(cassandraVersion.isGreaterThanOrEqualTo(VERSION_3_10));

		AllPossibleTypes entity = new AllPossibleTypes("1");

		entity.setTime(java.time.LocalTime.of(1, 2, 3));

		operations.insert(entity);

		ResultSet resultSet = session.execute("SELECT time FROM AllPossibleTypes WHERE id = '1'");
		Row row = resultSet.one();
		assertThat(row.getLocalTime(0)).isEqualTo(entity.getTime());
	}

	@Test // DATACASS-694, DATACASS-727
	void shouldWriteLocalTimeThroughDriver() {

		assumeTrue(cassandraVersion.isGreaterThanOrEqualTo(VERSION_3_10));

		session.execute("INSERT INTO AllPossibleTypes(id,time) VALUES('1','01:02:03.000')");

		AllPossibleTypes entity = operations.selectOne("SELECT time FROM AllPossibleTypes WHERE id = '1'",
				AllPossibleTypes.class);

		assertThat(entity.getTime()).isEqualTo(LocalTime.of(1, 2, 3, 0));
	}

	@Test // DATACASS-296, DATACASS-563
	void shouldReadAndWriteJodaLocalTime() {

		assumeTrue(cassandraVersion.isGreaterThanOrEqualTo(VERSION_3_10));

		AllPossibleTypes entity = new AllPossibleTypes("1");

		entity.setJodaLocalTime(org.joda.time.LocalTime.fromMillisOfDay(50000));

		operations.insert(entity);

		AllPossibleTypes loaded = load(entity);

		assertThat(loaded.getJodaLocalTime()).isEqualTo(entity.getJodaLocalTime());
	}

	@Test // DATACASS-296
	void shouldReadAndWriteInstant() {

		AllPossibleTypes entity = new AllPossibleTypes("1");

		entity.setInstant(java.time.Instant.now());

		operations.insert(entity);

		AllPossibleTypes loaded = load(entity);

		assertThat(loaded.getInstant().truncatedTo(ChronoUnit.MILLIS))
				.isEqualTo(entity.getInstant().truncatedTo(ChronoUnit.MILLIS));
	}

	@Test // DATACASS-296
	void shouldReadAndWriteZoneId() {

		AllPossibleTypes entity = new AllPossibleTypes("1");

		entity.setZoneId(java.time.ZoneId.of("Europe/Paris"));

		operations.insert(entity);

		AllPossibleTypes loaded = load(entity);

		assertThat(loaded.getZoneId()).isEqualTo(entity.getZoneId());
	}

	@Test // DATACASS-296
	void shouldReadAndWriteJodaLocalDate() {

		AllPossibleTypes entity = new AllPossibleTypes("1");

		entity.setJodaLocalDate(new org.joda.time.LocalDate(2010, 7, 4));

		operations.insert(entity);

		AllPossibleTypes loaded = load(entity);

		assertThat(loaded.getJodaLocalDate()).isEqualTo(entity.getJodaLocalDate());
	}

	@Test // DATACASS-296, DATACASS-727
	void shouldReadAndWriteJodaDateTime() {

		AllPossibleTypes entity = new AllPossibleTypes("1");

		entity.setJodaDateTime(new org.joda.time.DateTime(2010, 7, 4, 1, 2, 3));

		operations.insert(entity);

		AllPossibleTypes loaded = load(entity);

		assertThat(loaded.getJodaDateTime()).isEqualTo(entity.getJodaDateTime());
	}

	@Test // DATACASS-296
	void shouldReadAndWriteBpLocalDate() {

		AllPossibleTypes entity = new AllPossibleTypes("1");

		entity.setBpLocalDate(org.threeten.bp.LocalDate.of(2010, 7, 4));

		operations.insert(entity);

		AllPossibleTypes loaded = load(entity);

		assertThat(loaded.getBpLocalDate()).isEqualTo(entity.getBpLocalDate());
	}

	@Test // DATACASS-296
	void shouldReadAndWriteBpLocalDateTime() {

		AllPossibleTypes entity = new AllPossibleTypes("1");

		entity.setBpLocalDateTime(org.threeten.bp.LocalDateTime.of(2010, 7, 4, 1, 2, 3));

		operations.insert(entity);

		AllPossibleTypes loaded = load(entity);

		assertThat(loaded.getBpLocalDateTime()).isEqualTo(entity.getBpLocalDateTime());
	}

	@Test // DATACASS-296, DATACASS-563
	void shouldReadAndWriteBpLocalTime() {

		assumeTrue(cassandraVersion.isGreaterThanOrEqualTo(VERSION_3_10));

		AllPossibleTypes entity = new AllPossibleTypes("1");

		entity.setBpLocalTime(org.threeten.bp.LocalTime.of(1, 2, 3));

		operations.insert(entity);

		AllPossibleTypes loaded = load(entity);

		assertThat(loaded.getBpLocalTime()).isEqualTo(entity.getBpLocalTime());
	}

	@Test // DATACASS-296, DATACASS-727
	void shouldReadAndWriteBpInstant() {

		AllPossibleTypes entity = new AllPossibleTypes("1");

		entity.setBpInstant(org.threeten.bp.Instant.now());

		operations.insert(entity);

		AllPossibleTypes loaded = load(entity);

		assertThat(loaded.getBpInstant()).isEqualTo(entity.getBpInstant());
	}

	@Test // DATACASS-296
	void shouldReadAndWriteBpZoneId() {

		AllPossibleTypes entity = new AllPossibleTypes("1");

		entity.setBpZoneId(org.threeten.bp.ZoneId.of("Europe/Paris"));

		operations.insert(entity);

		AllPossibleTypes loaded = load(entity);

		assertThat(loaded.getBpZoneId()).isEqualTo(entity.getBpZoneId());
	}

	@Test // DATACASS-429, DATACASS-727
	void shouldReadAndWriteDuration() {

		assumeTrue(cassandraVersion.isGreaterThanOrEqualTo(VERSION_3_10));

		WithDuration withDuration = new WithDuration("foo", Duration.ofHours(2), CqlDuration.newInstance(1, 2, 3));

		operations.insert(withDuration);

		WithDuration loaded = operations.selectOneById(withDuration.getId(), WithDuration.class);

		assertThat(loaded.getDuration()).isEqualTo(withDuration.getDuration());
		assertThat(loaded.getCqlDuration()).isEqualTo(withDuration.getCqlDuration());
	}

	private AllPossibleTypes load(AllPossibleTypes entity) {
		return operations.selectOneById(entity.getId(), AllPossibleTypes.class);
	}

	public enum Condition {
		MINT
	}

	@Data
	@AllArgsConstructor
	static class WithDuration {

		@Id String id;
		Duration duration;
		CqlDuration cqlDuration;
	}

	@Data
	@NoArgsConstructor
	static class ListOfTuples {

		@Id String id;
		List<TupleValue> tuples;
	}
}
