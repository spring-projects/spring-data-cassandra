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
 * see the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.test.integration.mapping.types;

import static org.assertj.core.api.Assertions.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.UUID;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.cassandra.test.integration.AbstractKeyspaceCreatingIntegrationTest;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.domain.AllPossibleTypes;
import org.springframework.data.cassandra.test.integration.support.SchemaTestUtils;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.PreparedStatement;

/**
 * Integration tests for type mapping using {@link CassandraOperations}.
 *
 * @author Mark Paluch
 * @soundtrack DJ THT meets Scarlet - Live 2 Dance (Extended Mix) (Zgin Remix)
 */
@SuppressWarnings("Since15")
public class CassandraTypeMappingIntegrationTest extends AbstractKeyspaceCreatingIntegrationTest {

	CassandraOperations operations;

	@Before
	public void before() {

		operations = new CassandraTemplate(session);

		SchemaTestUtils.potentiallyCreateTableFor(AllPossibleTypes.class, operations);
		SchemaTestUtils.potentiallyCreateTableFor(TimeEntity.class, operations);

		SchemaTestUtils.truncate(AllPossibleTypes.class, operations);
		SchemaTestUtils.truncate(TimeEntity.class, operations);
	}

	@Test // DATACASS-280
	public void shouldReadAndWriteInetAddress() throws Exception {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setInet(InetAddress.getByName("127.0.0.1"));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getInet()).isEqualTo(entity.getInet());
	}

	@Test // DATACASS-280
	public void shouldReadAndWriteUUID() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setUuid(UUID.randomUUID());

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getUuid()).isEqualTo(entity.getUuid());
	}

	@Test // DATACASS-280
	public void shouldReadAndWriteBoxedShort() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBoxedShort(Short.MAX_VALUE);

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getBoxedShort()).isEqualTo(entity.getBoxedShort());
	}

	@Test // DATACASS-280
	public void shouldReadAndWritePrimitiveShort() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setPrimitiveShort(Short.MAX_VALUE);

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getPrimitiveShort()).isEqualTo(entity.getPrimitiveShort());
	}

	@Test // DATACASS-271
	public void shouldReadAndWriteBoxedByte() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBoxedByte(Byte.MAX_VALUE);

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getBoxedByte()).isEqualTo(entity.getBoxedByte());
	}

	@Test // DATACASS-271
	public void shouldReadAndWritePrimitiveByte() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setPrimitiveByte(Byte.MAX_VALUE);

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getPrimitiveByte()).isEqualTo(entity.getPrimitiveByte());
	}

	@Test // DATACASS-280
	public void shouldReadAndWriteBoxedLong() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBoxedLong(Long.MAX_VALUE);

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getBoxedLong()).isEqualTo(entity.getBoxedLong());
	}

	@Test // DATACASS-280
	public void shouldReadAndWritePrimitiveLong() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setPrimitiveLong(Long.MAX_VALUE);

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getPrimitiveLong()).isEqualTo(entity.getPrimitiveLong());
	}

	@Test // DATACASS-280
	public void shouldReadAndWriteBoxedInteger() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBoxedInteger(Integer.MAX_VALUE);

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getBoxedInteger()).isEqualTo(entity.getBoxedInteger());
	}

	@Test // DATACASS-280
	public void shouldReadAndWritePrimitiveInteger() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setPrimitiveInteger(Integer.MAX_VALUE);

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getPrimitiveInteger()).isEqualTo(entity.getPrimitiveInteger());
	}

	@Test // DATACASS-280
	public void shouldReadAndWriteBoxedFloat() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBoxedFloat(Float.MAX_VALUE);

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getBoxedFloat()).isEqualTo(entity.getBoxedFloat());
	}

	@Test // DATACASS-280
	public void shouldReadAndWritePrimitiveFloat() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setPrimitiveFloat(Float.MAX_VALUE);

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getPrimitiveFloat()).isEqualTo(entity.getPrimitiveFloat());
	}

	@Test // DATACASS-280
	public void shouldReadAndWriteBoxedDouble() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBoxedDouble(Double.MAX_VALUE);

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getBoxedDouble()).isEqualTo(entity.getBoxedDouble());
	}

	@Test // DATACASS-280
	public void shouldReadAndWritePrimitiveDouble() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setPrimitiveDouble(Double.MAX_VALUE);

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getPrimitiveDouble()).isEqualTo(entity.getPrimitiveDouble());
	}

	@Test // DATACASS-280
	public void shouldReadAndWriteBoxedBoolean() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBoxedBoolean(Boolean.TRUE);

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getBoxedBoolean()).isEqualTo(entity.getBoxedBoolean());
	}

	@Test // DATACASS-280
	public void shouldReadAndWritePrimitiveBoolean() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setPrimitiveBoolean(Boolean.TRUE);

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.isPrimitiveBoolean()).isEqualTo(entity.isPrimitiveBoolean());
	}

	@Test // DATACASS-280, DATACASS-271
	public void shouldReadAndWriteTimestamp() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setTimestamp(new Date(1));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getTimestamp()).isEqualTo(entity.getTimestamp());
	}

	@Test // DATACASS-271
	public void shouldReadAndWriteDate() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setDate(LocalDate.fromDaysSinceEpoch(1));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getDate()).isEqualTo(entity.getDate());
	}

	@Test // DATACASS-280
	public void shouldReadAndWriteBigInteger() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBigInteger(new BigInteger("123456"));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getBigInteger()).isEqualTo(entity.getBigInteger());
	}

	@Test // DATACASS-280
	public void shouldReadAndWriteBigDecimal() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBigDecimal(new BigDecimal("123456.7890123"));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getBigDecimal()).isEqualTo(entity.getBigDecimal());
	}

	@Test // DATACASS-280
	public void shouldReadAndWriteBlob() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBlob(ByteBuffer.wrap("Hello".getBytes()));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		ByteBuffer blob = loaded.getBlob();
		byte[] bytes = new byte[blob.remaining()];
		blob.get(bytes);
		assertThat(new String(bytes)).isEqualTo("Hello");
	}

	@Test // DATACASS-280
	public void shouldReadAndWriteSetOfString() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setSetOfString(Collections.singleton("hello"));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getSetOfString()).isEqualTo(entity.getSetOfString());
	}

	@Test // DATACASS-280
	public void shouldReadAndWriteEmptySetOfString() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setSetOfString(new HashSet<String>());

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getSetOfString()).isNull();
	}

	@Test // DATACASS-280
	public void shouldReadAndWriteListOfString() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setListOfString(Collections.singletonList("hello"));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getListOfString()).isEqualTo(entity.getListOfString());
	}

	@Test // DATACASS-280
	public void shouldReadAndWriteEmptyListOfString() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setListOfString(new ArrayList<String>());

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getListOfString()).isNull();
	}

	@Test // DATACASS-280
	public void shouldReadAndWriteMapOfString() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setMapOfString(Collections.singletonMap("hello", "world"));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getMapOfString()).isEqualTo(entity.getMapOfString());
	}

	@Test // DATACASS-280
	public void shouldReadAndWriteEmptyMapOfString() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setMapOfString(new HashMap<String, String>());

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getMapOfString()).isNull();
	}

	@Test // DATACASS-280
	public void shouldReadAndWriteEnum() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setAnEnum(Condition.MINT);

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getAnEnum()).isEqualTo(entity.getAnEnum());
	}

	@Test // DATACASS-271
	public void shouldReadAndWriteTime() {

		// writing of time is not supported with Insert/Update statements as they mix up types.
		// The only way to insert a time right now seems a PreparedStatement
		String id = "1";
		long time = 21312214L;

		PreparedStatement prepare = operations.getSession().prepare("INSERT INTO timeentity (id, time) values(?,?)");
		BoundStatement boundStatement = prepare.bind(id, time);
		operations.execute(boundStatement);

		TimeEntity loaded = operations.selectOneById(TimeEntity.class, id);

		assertThat(loaded.getTime()).isEqualTo(time);
	}

	@Test // DATACASS-296
	public void shouldReadAndWriteLocalDate() throws Exception {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setLocalDate(java.time.LocalDate.of(2010, 7, 4));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getLocalDate()).isEqualTo(entity.getLocalDate());
	}

	@Test // DATACASS-296
	public void shouldReadAndWriteLocalDateTime() throws Exception {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setLocalDateTime(java.time.LocalDateTime.of(2010, 7, 4, 1, 2, 3));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getLocalDateTime()).isEqualTo(entity.getLocalDateTime());
	}

	@Test // DATACASS-296
	public void shouldReadAndWriteLocalTime() throws Exception {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setLocalTime(java.time.LocalTime.of(1, 2, 3));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getLocalTime()).isEqualTo(entity.getLocalTime());
	}

	@Test // DATACASS-296
	public void shouldReadAndWriteInstant() throws Exception {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setInstant(java.time.Instant.now());

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getInstant()).isEqualTo(entity.getInstant());
	}

	@Test // DATACASS-296
	public void shouldReadAndWriteZoneId() throws Exception {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setZoneId(java.time.ZoneId.of("Europe/Paris"));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getZoneId()).isEqualTo(entity.getZoneId());
	}

	@Test // DATACASS-296
	public void shouldReadAndWriteJodaLocalDate() throws Exception {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setJodaLocalDate(new org.joda.time.LocalDate(2010, 7, 4));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getJodaLocalDate()).isEqualTo(entity.getJodaLocalDate());
	}

	@Test // DATACASS-296
	public void shouldReadAndWriteJodaDateMidnight() throws Exception {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setJodaDateMidnight(new org.joda.time.DateMidnight(2010, 7, 4));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getJodaDateMidnight()).isEqualTo(entity.getJodaDateMidnight());
	}

	@Test // DATACASS-296
	public void shouldReadAndWriteJodaDateTime() throws Exception {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setJodaDateTime(new org.joda.time.DateTime(2010, 7, 4, 1, 2, 3));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getJodaDateTime()).isEqualTo(entity.getJodaDateTime());
	}

	@Test // DATACASS-296
	public void shouldReadAndWriteBpLocalDate() throws Exception {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBpLocalDate(org.threeten.bp.LocalDate.of(2010, 7, 4));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getBpLocalDate()).isEqualTo(entity.getBpLocalDate());
	}

	@Test // DATACASS-296
	public void shouldReadAndWriteBpLocalDateTime() throws Exception {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBpLocalDateTime(org.threeten.bp.LocalDateTime.of(2010, 7, 4, 1, 2, 3));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getBpLocalDateTime()).isEqualTo(entity.getBpLocalDateTime());
	}

	@Test // DATACASS-296
	public void shouldReadAndWriteBpLocalTime() throws Exception {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBpLocalTime(org.threeten.bp.LocalTime.of(1, 2, 3));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getBpLocalTime()).isEqualTo(entity.getBpLocalTime());
	}

	@Test // DATACASS-296
	public void shouldReadAndWriteBpInstant() throws Exception {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBpInstant(org.threeten.bp.Instant.now());

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getBpZoneId()).isEqualTo(entity.getBpZoneId());
	}

	@Test // DATACASS-296
	public void shouldReadAndWriteBpZoneId() throws Exception {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBpZoneId(org.threeten.bp.ZoneId.of("Europe/Paris"));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getBpZoneId()).isEqualTo(entity.getBpZoneId());
	}

	@Test // DATACASS-285
	@Ignore("Counter columns are not supported with Spring Data Cassandra as the value of counter columns can only be incremented/decremented, not set")
	public void shouldReadAndWriteCounter() {

		CounterEntity entity = new CounterEntity("1");
		entity.setCount(1);

		operations.update(entity);
		CounterEntity loaded = operations.selectOneById(CounterEntity.class, entity.getId());

		assertThat(loaded.getCount()).isEqualTo(entity.getCount());
	}

	public enum Condition {
		MINT;
	}
}
