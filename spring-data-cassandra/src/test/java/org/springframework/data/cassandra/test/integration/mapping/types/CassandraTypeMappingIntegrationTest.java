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

import com.datastax.driver.core.SimpleStatement;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.cassandra.test.integration.AbstractKeyspaceCreatingIntegrationTest;
import org.springframework.data.cassandra.convert.MappingCassandraConverter;
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

	/**
	 * @see DATACASS-280
	 */
	@Test
	public void shouldReadAndWriteInetAddress() throws Exception {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setInet(InetAddress.getByName("127.0.0.1"));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(entity.getId(), AllPossibleTypes.class);

		assertThat(loaded.getInet()).isEqualTo(entity.getInet());
	}

	/**
	 * @see DATACASS-280
	 */
	@Test
	public void shouldReadAndWriteUUID() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setUuid(UUID.randomUUID());

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(entity.getId(), AllPossibleTypes.class);

		assertThat(loaded.getUuid()).isEqualTo(entity.getUuid());
	}

	/**
	 * @see DATACASS-280
	 */
	@Test
	public void shouldReadAndWriteBoxedShort() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBoxedShort(Short.MAX_VALUE);

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(entity.getId(), AllPossibleTypes.class);

		assertThat(loaded.getBoxedShort()).isEqualTo(entity.getBoxedShort());
	}

	/**
	 * @see DATACASS-280
	 */
	@Test
	public void shouldReadAndWritePrimitiveShort() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setPrimitiveShort(Short.MAX_VALUE);

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(entity.getId(), AllPossibleTypes.class);

		assertThat(loaded.getPrimitiveShort()).isEqualTo(entity.getPrimitiveShort());
	}

	/**
	 * @see DATACASS-271
	 */
	@Test
	public void shouldReadAndWriteBoxedByte() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBoxedByte(Byte.MAX_VALUE);

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(entity.getId(), AllPossibleTypes.class);

		assertThat(loaded.getBoxedByte()).isEqualTo(entity.getBoxedByte());
	}

	/**
	 * @see DATACASS-271
	 */
	@Test
	public void shouldReadAndWritePrimitiveByte() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setPrimitiveByte(Byte.MAX_VALUE);

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(entity.getId(), AllPossibleTypes.class);

		assertThat(loaded.getPrimitiveByte()).isEqualTo(entity.getPrimitiveByte());
	}

	/**
	 * @see DATACASS-280
	 */
	@Test
	public void shouldReadAndWriteBoxedLong() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBoxedLong(Long.MAX_VALUE);

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(entity.getId(), AllPossibleTypes.class);

		assertThat(loaded.getBoxedLong()).isEqualTo(entity.getBoxedLong());
	}

	/**
	 * @see DATACASS-280
	 */
	@Test
	public void shouldReadAndWritePrimitiveLong() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setPrimitiveLong(Long.MAX_VALUE);

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(entity.getId(), AllPossibleTypes.class);

		assertThat(loaded.getPrimitiveLong()).isEqualTo(entity.getPrimitiveLong());
	}

	/**
	 * @see DATACASS-280
	 */
	@Test
	public void shouldReadAndWriteBoxedInteger() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBoxedInteger(Integer.MAX_VALUE);

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(entity.getId(), AllPossibleTypes.class);

		assertThat(loaded.getBoxedInteger()).isEqualTo(entity.getBoxedInteger());
	}

	/**
	 * @see DATACASS-280
	 */
	@Test
	public void shouldReadAndWritePrimitiveInteger() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setPrimitiveInteger(Integer.MAX_VALUE);

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(entity.getId(), AllPossibleTypes.class);

		assertThat(loaded.getPrimitiveInteger()).isEqualTo(entity.getPrimitiveInteger());
	}

	/**
	 * @see DATACASS-280
	 */
	@Test
	public void shouldReadAndWriteBoxedFloat() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBoxedFloat(Float.MAX_VALUE);

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(entity.getId(), AllPossibleTypes.class);

		assertThat(loaded.getBoxedFloat()).isEqualTo(entity.getBoxedFloat());
	}

	/**
	 * @see DATACASS-280
	 */
	@Test
	public void shouldReadAndWritePrimitiveFloat() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setPrimitiveFloat(Float.MAX_VALUE);

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(entity.getId(), AllPossibleTypes.class);

		assertThat(loaded.getPrimitiveFloat()).isEqualTo(entity.getPrimitiveFloat());
	}

	/**
	 * @see DATACASS-280
	 */
	@Test
	public void shouldReadAndWriteBoxedDouble() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBoxedDouble(Double.MAX_VALUE);

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(entity.getId(), AllPossibleTypes.class);

		assertThat(loaded.getBoxedDouble()).isEqualTo(entity.getBoxedDouble());
	}

	/**
	 * @see DATACASS-280
	 */
	@Test
	public void shouldReadAndWritePrimitiveDouble() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setPrimitiveDouble(Double.MAX_VALUE);

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(entity.getId(), AllPossibleTypes.class);

		assertThat(loaded.getPrimitiveDouble()).isEqualTo(entity.getPrimitiveDouble());
	}

	/**
	 * @see DATACASS-280
	 */
	@Test
	public void shouldReadAndWriteBoxedBoolean() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBoxedBoolean(Boolean.TRUE);

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(entity.getId(), AllPossibleTypes.class);

		assertThat(loaded.getBoxedBoolean()).isEqualTo(entity.getBoxedBoolean());
	}

	/**
	 * @see DATACASS-280
	 */
	@Test
	public void shouldReadAndWritePrimitiveBoolean() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setPrimitiveBoolean(Boolean.TRUE);

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(entity.getId(), AllPossibleTypes.class);

		assertThat(loaded.isPrimitiveBoolean()).isEqualTo(entity.isPrimitiveBoolean());
	}

	/**
	 * @see DATACASS-280
	 * @see DATACASS-271
	 */
	@Test
	public void shouldReadAndWriteTimestamp() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setTimestamp(new Date(1));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(entity.getId(), AllPossibleTypes.class);

		assertThat(loaded.getTimestamp()).isEqualTo(entity.getTimestamp());
	}

	/**
	 * @see DATACASS-271
	 */
	@Test
	public void shouldReadAndWriteDate() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setDate(LocalDate.fromDaysSinceEpoch(1));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(entity.getId(), AllPossibleTypes.class);

		assertThat(loaded.getDate()).isEqualTo(entity.getDate());
	}

	/**
	 * @see DATACASS-280
	 */
	@Test
	public void shouldReadAndWriteBigInteger() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBigInteger(new BigInteger("123456"));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(entity.getId(), AllPossibleTypes.class);

		assertThat(loaded.getBigInteger()).isEqualTo(entity.getBigInteger());
	}

	/**
	 * @see DATACASS-280
	 */
	@Test
	public void shouldReadAndWriteBigDecimal() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBigDecimal(new BigDecimal("123456.7890123"));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(entity.getId(), AllPossibleTypes.class);

		assertThat(loaded.getBigDecimal()).isEqualTo(entity.getBigDecimal());
	}

	/**
	 * @see DATACASS-280
	 */
	@Test
	public void shouldReadAndWriteBlob() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBlob(ByteBuffer.wrap("Hello".getBytes()));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(entity.getId(), AllPossibleTypes.class);

		ByteBuffer blob = loaded.getBlob();
		byte[] bytes = new byte[blob.remaining()];
		blob.get(bytes);
		assertThat(new String(bytes)).isEqualTo("Hello");
	}

	/**
	 * @see DATACASS-280
	 */
	@Test
	public void shouldReadAndWriteSetOfString() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setSetOfString(Collections.singleton("hello"));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(entity.getId(), AllPossibleTypes.class);

		assertThat(loaded.getSetOfString()).isEqualTo(entity.getSetOfString());
	}

	/**
	 * @see DATACASS-280
	 */
	@Test
	public void shouldReadAndWriteEmptySetOfString() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setSetOfString(new HashSet<String>());

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(entity.getId(), AllPossibleTypes.class);

		assertThat(loaded.getSetOfString()).isNull();
	}

	/**
	 * @see DATACASS-280
	 */
	@Test
	public void shouldReadAndWriteListOfString() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setListOfString(Collections.singletonList("hello"));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(entity.getId(), AllPossibleTypes.class);

		assertThat(loaded.getListOfString()).isEqualTo(entity.getListOfString());
	}

	/**
	 * @see DATACASS-280
	 */
	@Test
	public void shouldReadAndWriteEmptyListOfString() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setListOfString(new ArrayList<String>());

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(entity.getId(), AllPossibleTypes.class);

		assertThat(loaded.getListOfString()).isNull();
	}

	/**
	 * @see DATACASS-280
	 */
	@Test
	public void shouldReadAndWriteMapOfString() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setMapOfString(Collections.singletonMap("hello", "world"));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(entity.getId(), AllPossibleTypes.class);

		assertThat(loaded.getMapOfString()).isEqualTo(entity.getMapOfString());
	}

	/**
	 * @see DATACASS-280
	 */
	@Test
	public void shouldReadAndWriteEmptyMapOfString() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setMapOfString(new HashMap<String, String>());

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(entity.getId(), AllPossibleTypes.class);

		assertThat(loaded.getMapOfString()).isNull();
	}

	/**
	 * @see DATACASS-280
	 */
	@Test
	public void shouldReadAndWriteEnum() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setAnEnum(Condition.MINT);

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(entity.getId(), AllPossibleTypes.class);

		assertThat(loaded.getAnEnum()).isEqualTo(entity.getAnEnum());
	}

	/**
	 * @see DATACASS-271
	 */
	@Test
	public void shouldReadAndWriteTime() {

		// writing of time is not supported with Insert/Update statements as they mix up types.
		// The only way to insert a time right now seems a PreparedStatement
		String id = "1";
		long time = 21312214L;

		operations.getCqlOperations()
				.execute(new SimpleStatement("INSERT INTO timeentity (id, time) values(?,?)", id, time));

		TimeEntity loaded = operations.selectOneById(id, TimeEntity.class);

		assertThat(loaded.getTime()).isEqualTo(time);
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void shouldReadAndWriteLocalDate() throws Exception {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setLocalDate(java.time.LocalDate.of(2010, 7, 4));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(entity.getId(), AllPossibleTypes.class);

		assertThat(loaded.getLocalDate()).isEqualTo(entity.getLocalDate());
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void shouldReadAndWriteLocalDateTime() throws Exception {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setLocalDateTime(java.time.LocalDateTime.of(2010, 7, 4, 1, 2, 3));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(entity.getId(), AllPossibleTypes.class);

		assertThat(loaded.getLocalDateTime()).isEqualTo(entity.getLocalDateTime());
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void shouldReadAndWriteLocalTime() throws Exception {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setLocalTime(java.time.LocalTime.of(1, 2, 3));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(entity.getId(), AllPossibleTypes.class);

		assertThat(loaded.getLocalTime()).isEqualTo(entity.getLocalTime());
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void shouldReadAndWriteInstant() throws Exception {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setInstant(java.time.Instant.now());

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(entity.getId(), AllPossibleTypes.class);

		assertThat(loaded.getInstant()).isEqualTo(entity.getInstant());
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void shouldReadAndWriteZoneId() throws Exception {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setZoneId(java.time.ZoneId.of("Europe/Paris"));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(entity.getId(), AllPossibleTypes.class);

		assertThat(loaded.getZoneId()).isEqualTo(entity.getZoneId());
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void shouldReadAndWriteJodaLocalDate() throws Exception {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setJodaLocalDate(new org.joda.time.LocalDate(2010, 7, 4));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(entity.getId(), AllPossibleTypes.class);

		assertThat(loaded.getJodaLocalDate()).isEqualTo(entity.getJodaLocalDate());
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void shouldReadAndWriteJodaDateMidnight() throws Exception {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setJodaDateMidnight(new org.joda.time.DateMidnight(2010, 7, 4));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(entity.getId(), AllPossibleTypes.class);

		assertThat(loaded.getJodaDateMidnight()).isEqualTo(entity.getJodaDateMidnight());
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void shouldReadAndWriteJodaDateTime() throws Exception {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setJodaDateTime(new org.joda.time.DateTime(2010, 7, 4, 1, 2, 3));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(entity.getId(), AllPossibleTypes.class);

		assertThat(loaded.getJodaDateTime()).isEqualTo(entity.getJodaDateTime());
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void shouldReadAndWriteBpLocalDate() throws Exception {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBpLocalDate(org.threeten.bp.LocalDate.of(2010, 7, 4));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(entity.getId(), AllPossibleTypes.class);

		assertThat(loaded.getBpLocalDate()).isEqualTo(entity.getBpLocalDate());
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void shouldReadAndWriteBpLocalDateTime() throws Exception {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBpLocalDateTime(org.threeten.bp.LocalDateTime.of(2010, 7, 4, 1, 2, 3));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(entity.getId(), AllPossibleTypes.class);

		assertThat(loaded.getBpLocalDateTime()).isEqualTo(entity.getBpLocalDateTime());
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void shouldReadAndWriteBpLocalTime() throws Exception {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBpLocalTime(org.threeten.bp.LocalTime.of(1, 2, 3));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(entity.getId(), AllPossibleTypes.class);

		assertThat(loaded.getBpLocalTime()).isEqualTo(entity.getBpLocalTime());
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void shouldReadAndWriteBpInstant() throws Exception {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBpInstant(org.threeten.bp.Instant.now());

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(entity.getId(), AllPossibleTypes.class);

		assertThat(loaded.getBpZoneId()).isEqualTo(entity.getBpZoneId());
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void shouldReadAndWriteBpZoneId() throws Exception {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBpZoneId(org.threeten.bp.ZoneId.of("Europe/Paris"));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(entity.getId(), AllPossibleTypes.class);

		assertThat(loaded.getBpZoneId()).isEqualTo(entity.getBpZoneId());
	}

	/**
	 * @see DATACASS-285
	 */
	@Test
	@Ignore("Counter columns are not supported with Spring Data Cassandra as the value of counter columns can only be incremented/decremented, not set")
	public void shouldReadAndWriteCounter() {

		CounterEntity entity = new CounterEntity("1");
		entity.setCount(1);

		operations.update(entity);
		CounterEntity loaded = operations.selectOneById(entity.getId(), CounterEntity.class);

		assertThat(loaded.getCount()).isEqualTo(entity.getCount());
	}

	public enum Condition {
		MINT;
	}
}
