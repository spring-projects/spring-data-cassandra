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

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

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
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

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
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getInet(), is(equalTo(entity.getInet())));
	}

	/**
	 * @see DATACASS-280
	 */
	@Test
	public void shouldReadAndWriteUUID() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setUuid(UUID.randomUUID());

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getUuid(), is(equalTo(entity.getUuid())));
	}

	/**
	 * @see DATACASS-280
	 */
	@Test
	public void shouldReadAndWriteBoxedShort() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBoxedShort(Short.MAX_VALUE);

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getBoxedShort(), is(equalTo(entity.getBoxedShort())));
	}

	/**
	 * @see DATACASS-280
	 */
	@Test
	public void shouldReadAndWritePrimitiveShort() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setPrimitiveShort(Short.MAX_VALUE);

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getPrimitiveShort(), is(equalTo(entity.getPrimitiveShort())));
	}

	/**
	 * @see DATACASS-271
	 */
	@Test
	public void shouldReadAndWriteBoxedByte() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBoxedByte(Byte.MAX_VALUE);

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getBoxedByte(), is(equalTo(entity.getBoxedByte())));
	}

	/**
	 * @see DATACASS-271
	 */
	@Test
	public void shouldReadAndWritePrimitiveByte() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setPrimitiveByte(Byte.MAX_VALUE);

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getPrimitiveByte(), is(equalTo(entity.getPrimitiveByte())));
	}

	/**
	 * @see DATACASS-280
	 */
	@Test
	public void shouldReadAndWriteBoxedLong() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBoxedLong(Long.MAX_VALUE);

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getBoxedLong(), is(equalTo(entity.getBoxedLong())));
	}

	/**
	 * @see DATACASS-280
	 */
	@Test
	public void shouldReadAndWritePrimitiveLong() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setPrimitiveLong(Long.MAX_VALUE);

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getPrimitiveLong(), is(equalTo(entity.getPrimitiveLong())));
	}

	/**
	 * @see DATACASS-280
	 */
	@Test
	public void shouldReadAndWriteBoxedInteger() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBoxedInteger(Integer.MAX_VALUE);

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getBoxedInteger(), is(equalTo(entity.getBoxedInteger())));
	}

	/**
	 * @see DATACASS-280
	 */
	@Test
	public void shouldReadAndWritePrimitiveInteger() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setPrimitiveInteger(Integer.MAX_VALUE);

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getPrimitiveInteger(), is(equalTo(entity.getPrimitiveInteger())));
	}

	/**
	 * @see DATACASS-280
	 */
	@Test
	public void shouldReadAndWriteBoxedFloat() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBoxedFloat(Float.MAX_VALUE);

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getBoxedFloat(), is(equalTo(entity.getBoxedFloat())));
	}

	/**
	 * @see DATACASS-280
	 */
	@Test
	public void shouldReadAndWritePrimitiveFloat() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setPrimitiveFloat(Float.MAX_VALUE);

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getPrimitiveFloat(), is(equalTo(entity.getPrimitiveFloat())));
	}

	/**
	 * @see DATACASS-280
	 */
	@Test
	public void shouldReadAndWriteBoxedDouble() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBoxedDouble(Double.MAX_VALUE);

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getBoxedDouble(), is(equalTo(entity.getBoxedDouble())));
	}

	/**
	 * @see DATACASS-280
	 */
	@Test
	public void shouldReadAndWritePrimitiveDouble() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setPrimitiveDouble(Double.MAX_VALUE);

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getPrimitiveDouble(), is(equalTo(entity.getPrimitiveDouble())));
	}

	/**
	 * @see DATACASS-280
	 */
	@Test
	public void shouldReadAndWriteBoxedBoolean() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBoxedBoolean(Boolean.TRUE);

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getBoxedBoolean(), is(equalTo(entity.getBoxedBoolean())));
	}

	/**
	 * @see DATACASS-280
	 */
	@Test
	public void shouldReadAndWritePrimitiveBoolean() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setPrimitiveBoolean(Boolean.TRUE);

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.isPrimitiveBoolean(), is(equalTo(entity.isPrimitiveBoolean())));
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
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getTimestamp(), is(equalTo(entity.getTimestamp())));
	}

	/**
	 * @see DATACASS-271
	 */
	@Test
	public void shouldReadAndWriteDate() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setDate(LocalDate.fromDaysSinceEpoch(1));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getDate(), is(equalTo(entity.getDate())));
	}

	/**
	 * @see DATACASS-280
	 */
	@Test
	public void shouldReadAndWriteBigInteger() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBigInteger(new BigInteger("123456"));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getBigInteger(), is(equalTo(entity.getBigInteger())));
	}

	/**
	 * @see DATACASS-280
	 */
	@Test
	public void shouldReadAndWriteBigDecimal() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBigDecimal(new BigDecimal("123456.7890123"));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getBigDecimal(), is(equalTo(entity.getBigDecimal())));
	}

	/**
	 * @see DATACASS-280
	 */
	@Test
	public void shouldReadAndWriteBlob() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBlob(ByteBuffer.wrap("Hello".getBytes()));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		ByteBuffer blob = loaded.getBlob();
		byte[] bytes = new byte[blob.remaining()];
		blob.get(bytes);
		assertThat(new String(bytes), is(equalTo("Hello")));
	}

	/**
	 * @see DATACASS-280
	 */
	@Test
	public void shouldReadAndWriteSetOfString() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setSetOfString(Collections.singleton("hello"));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getSetOfString(), is(equalTo(entity.getSetOfString())));
	}

	/**
	 * @see DATACASS-280
	 */
	@Test
	public void shouldReadAndWriteEmptySetOfString() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setSetOfString(new HashSet<String>());

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getSetOfString(), is(nullValue()));
	}

	/**
	 * @see DATACASS-280
	 */
	@Test
	public void shouldReadAndWriteListOfString() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setListOfString(Collections.singletonList("hello"));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getListOfString(), is(equalTo(entity.getListOfString())));
	}

	/**
	 * @see DATACASS-280
	 */
	@Test
	public void shouldReadAndWriteEmptyListOfString() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setListOfString(new ArrayList<String>());

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getListOfString(), is(nullValue()));
	}

	/**
	 * @see DATACASS-280
	 */
	@Test
	public void shouldReadAndWriteMapOfString() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setMapOfString(Collections.singletonMap("hello", "world"));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getMapOfString(), is(equalTo(entity.getMapOfString())));
	}

	/**
	 * @see DATACASS-280
	 */
	@Test
	public void shouldReadAndWriteEmptyMapOfString() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setMapOfString(new HashMap<String, String>());

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getMapOfString(), is(nullValue()));
	}

	/**
	 * @see DATACASS-280
	 */
	@Test
	public void shouldReadAndWriteEnum() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setAnEnum(Condition.MINT);

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getAnEnum(), is(equalTo(entity.getAnEnum())));
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

		PreparedStatement prepare = operations.getSession()
				.prepare("INSERT INTO timeentity (id, time) values(?,?)");
		BoundStatement boundStatement = prepare.bind(id, time);
		operations.execute(boundStatement);

		TimeEntity loaded = operations.selectOneById(TimeEntity.class, id);

		assertThat(loaded.getTime(), is(equalTo(time)));
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void shouldReadAndWriteLocalDate() throws Exception {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setLocalDate(java.time.LocalDate.of(2010, 7, 4));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getLocalDate(), is(equalTo(entity.getLocalDate())));
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void shouldReadAndWriteLocalDateTime() throws Exception {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setLocalDateTime(java.time.LocalDateTime.of(2010, 7, 4, 1, 2, 3));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getLocalDateTime(), is(equalTo(entity.getLocalDateTime())));
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void shouldReadAndWriteLocalTime() throws Exception {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setLocalTime(java.time.LocalTime.of(1, 2, 3));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getLocalTime(), is(equalTo(entity.getLocalTime())));
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void shouldReadAndWriteInstant() throws Exception {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setInstant(java.time.Instant.now());

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getInstant(), is(equalTo(entity.getInstant())));
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void shouldReadAndWriteZoneId() throws Exception {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setZoneId(java.time.ZoneId.of("Europe/Paris"));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getZoneId(), is(equalTo(entity.getZoneId())));
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void shouldReadAndWriteJodaLocalDate() throws Exception {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setJodaLocalDate(new org.joda.time.LocalDate(2010, 7, 4));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getJodaLocalDate(), is(equalTo(entity.getJodaLocalDate())));
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void shouldReadAndWriteJodaDateMidnight() throws Exception {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setJodaDateMidnight(new org.joda.time.DateMidnight(2010, 7, 4));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getJodaDateMidnight(), is(equalTo(entity.getJodaDateMidnight())));
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void shouldReadAndWriteJodaDateTime() throws Exception {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setJodaDateTime(new org.joda.time.DateTime(2010, 7, 4, 1, 2, 3));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getJodaDateTime(), is(equalTo(entity.getJodaDateTime())));
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void shouldReadAndWriteBpLocalDate() throws Exception {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBpLocalDate(org.threeten.bp.LocalDate.of(2010, 7, 4));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getBpLocalDate(), is(equalTo(entity.getBpLocalDate())));
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void shouldReadAndWriteBpLocalDateTime() throws Exception {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBpLocalDateTime(org.threeten.bp.LocalDateTime.of(2010, 7, 4, 1, 2, 3));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getBpLocalDateTime(), is(equalTo(entity.getBpLocalDateTime())));
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void shouldReadAndWriteBpLocalTime() throws Exception {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBpLocalTime(org.threeten.bp.LocalTime.of(1, 2, 3));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getBpLocalTime(), is(equalTo(entity.getBpLocalTime())));
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void shouldReadAndWriteBpInstant() throws Exception {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBpInstant(org.threeten.bp.Instant.now());

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getBpZoneId(), is(equalTo(entity.getBpZoneId())));
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void shouldReadAndWriteBpZoneId() throws Exception {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBpZoneId(org.threeten.bp.ZoneId.of("Europe/Paris"));

		operations.insert(entity);
		AllPossibleTypes loaded = operations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getBpZoneId(), is(equalTo(entity.getBpZoneId())));
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
		CounterEntity loaded = operations.selectOneById(CounterEntity.class, entity.getId());

		assertThat(loaded.getCount(), is(equalTo(entity.getCount())));
	}

	public enum Condition {
		MINT;
	}
}
