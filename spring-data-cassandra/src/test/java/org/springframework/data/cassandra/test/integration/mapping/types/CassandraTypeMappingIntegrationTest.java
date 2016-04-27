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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.mapping.CassandraType;
import org.springframework.data.cassandra.mapping.PrimaryKey;
import org.springframework.data.cassandra.mapping.Table;
import org.springframework.data.cassandra.test.integration.support.AbstractSpringDataEmbeddedCassandraIntegrationTest;
import org.springframework.data.cassandra.test.integration.support.IntegrationTestConfig;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.datastax.driver.core.DataType.Name;

/**
 * Integration tests for type mapping using {@link CassandraOperations}.
 *
 * @author Mark Paluch
 * @soundtrack DJ THT meets Scarlet - Live 2 Dance (Extended Mix) (Zgin Remix)
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class CassandraTypeMappingIntegrationTest extends AbstractSpringDataEmbeddedCassandraIntegrationTest {

	@Configuration
	public static class Config extends IntegrationTestConfig {

		@Override
		public String[] getEntityBasePackages() {
			return new String[] { AllPossibleTypes.class.getPackage().getName() };
		}
	}

	@Autowired CassandraOperations cassandraOperations;

	@Before
	public void setUp() {
		cassandraOperations.deleteAll(AllPossibleTypes.class);
	}

	/**
	 * see DATACASS-280.
	 */
	@Test
	public void shouldReadAndWriteInetAddress() throws Exception {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setInet(InetAddress.getByName("127.0.0.1"));

		cassandraOperations.insert(entity);
		AllPossibleTypes loaded = cassandraOperations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getInet(), is(equalTo(entity.getInet())));
	}

	/**
	 * see DATACASS-280.
	 */
	@Test
	public void shouldReadAndWriteUUID() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setUuid(UUID.randomUUID());

		cassandraOperations.insert(entity);
		AllPossibleTypes loaded = cassandraOperations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getUuid(), is(equalTo(entity.getUuid())));
	}

	/**
	 * see DATACASS-280.
	 */
	@Test
	public void shouldReadAndWriteBoxedLongShort() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBoxedShort(Short.MAX_VALUE);

		cassandraOperations.insert(entity);
		AllPossibleTypes loaded = cassandraOperations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getBoxedShort(), is(equalTo(entity.getBoxedShort())));
	}

	/**
	 * see DATACASS-280.
	 */
	@Test
	public void shouldReadAndWritePrimitiveShort() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setPrimitiveShort(Short.MAX_VALUE);

		cassandraOperations.insert(entity);
		AllPossibleTypes loaded = cassandraOperations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getPrimitiveShort(), is(equalTo(entity.getPrimitiveShort())));
	}

	/**
	 * see DATACASS-280.
	 */
	@Test
	public void shouldReadAndWriteBoxedLong() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBoxedLong(Long.MAX_VALUE);

		cassandraOperations.insert(entity);
		AllPossibleTypes loaded = cassandraOperations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getBoxedLong(), is(equalTo(entity.getBoxedLong())));
	}

	/**
	 * see DATACASS-280.
	 */
	@Test
	public void shouldReadAndWritePrimitiveLong() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setPrimitiveLong(Long.MAX_VALUE);

		cassandraOperations.insert(entity);
		AllPossibleTypes loaded = cassandraOperations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getPrimitiveLong(), is(equalTo(entity.getPrimitiveLong())));
	}

	/**
	 * see DATACASS-280.
	 */
	@Test
	public void shouldReadAndWriteBoxedInteger() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBoxedInteger(Integer.MAX_VALUE);

		cassandraOperations.insert(entity);
		AllPossibleTypes loaded = cassandraOperations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getBoxedInteger(), is(equalTo(entity.getBoxedInteger())));
	}

	/**
	 * see DATACASS-280.
	 */
	@Test
	public void shouldReadAndWritePrimitiveInteger() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setPrimitiveInteger(Integer.MAX_VALUE);

		cassandraOperations.insert(entity);
		AllPossibleTypes loaded = cassandraOperations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getPrimitiveInteger(), is(equalTo(entity.getPrimitiveInteger())));
	}

	/**
	 * see DATACASS-280.
	 */
	@Test
	public void shouldReadAndWriteBoxedFloat() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBoxedFloat(Float.MAX_VALUE);

		cassandraOperations.insert(entity);
		AllPossibleTypes loaded = cassandraOperations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getBoxedFloat(), is(equalTo(entity.getBoxedFloat())));
	}

	/**
	 * see DATACASS-280.
	 */
	@Test
	public void shouldReadAndWritePrimitiveFloat() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setPrimitiveFloat(Float.MAX_VALUE);

		cassandraOperations.insert(entity);
		AllPossibleTypes loaded = cassandraOperations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getPrimitiveFloat(), is(equalTo(entity.getPrimitiveFloat())));
	}

	/**
	 * see DATACASS-280.
	 */
	@Test
	public void shouldReadAndWriteBoxedDouble() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBoxedDouble(Double.MAX_VALUE);

		cassandraOperations.insert(entity);
		AllPossibleTypes loaded = cassandraOperations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getBoxedDouble(), is(equalTo(entity.getBoxedDouble())));
	}

	/**
	 * see DATACASS-280.
	 */
	@Test
	public void shouldReadAndWritePrimitiveDouble() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setPrimitiveDouble(Double.MAX_VALUE);

		cassandraOperations.insert(entity);
		AllPossibleTypes loaded = cassandraOperations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getPrimitiveDouble(), is(equalTo(entity.getPrimitiveDouble())));
	}

	/**
	 * see DATACASS-280.
	 */
	@Test
	public void shouldReadAndWriteBoxedBoolean() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBoxedBoolean(Boolean.TRUE);

		cassandraOperations.insert(entity);
		AllPossibleTypes loaded = cassandraOperations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getBoxedBoolean(), is(equalTo(entity.getBoxedBoolean())));
	}

	/**
	 * see DATACASS-280.
	 */
	@Test
	public void shouldReadAndWritePrimitiveBoolean() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setPrimitiveBoolean(Boolean.TRUE);

		cassandraOperations.insert(entity);
		AllPossibleTypes loaded = cassandraOperations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.isPrimitiveBoolean(), is(equalTo(entity.isPrimitiveBoolean())));
	}

	/**
	 * see DATACASS-280.
	 */
	@Test
	public void shouldReadAndWriteDate() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setTimestamp(new Date(1));

		cassandraOperations.insert(entity);
		AllPossibleTypes loaded = cassandraOperations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getTimestamp(), is(equalTo(entity.getTimestamp())));
	}

	/**
	 * see DATACASS-280.
	 */
	@Test
	public void shouldReadAndWriteBigInteger() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBigInteger(new BigInteger("123456"));

		cassandraOperations.insert(entity);
		AllPossibleTypes loaded = cassandraOperations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getBigInteger(), is(equalTo(entity.getBigInteger())));
	}

	/**
	 * see DATACASS-280.
	 */
	@Test
	public void shouldReadAndWriteBigDecimal() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBigDecimal(new BigDecimal("123456.7890123"));

		cassandraOperations.insert(entity);
		AllPossibleTypes loaded = cassandraOperations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getBigDecimal(), is(equalTo(entity.getBigDecimal())));
	}

	/**
	 * see DATACASS-280.
	 */
	@Test
	public void shouldReadAndWriteBlob() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setBlob(ByteBuffer.wrap("Hello".getBytes()));

		cassandraOperations.insert(entity);
		AllPossibleTypes loaded = cassandraOperations.selectOneById(AllPossibleTypes.class, entity.getId());

		ByteBuffer blob = loaded.getBlob();
		byte[] bytes = new byte[blob.remaining()];
		blob.get(bytes);
		assertThat(new String(bytes), is(equalTo("Hello")));
	}

	/**
	 * see DATACASS-280.
	 */
	@Test
	public void shouldReadAndWriteSetOfString() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setSetOfString(Collections.singleton("hello"));

		cassandraOperations.insert(entity);
		AllPossibleTypes loaded = cassandraOperations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getSetOfString(), is(equalTo(entity.getSetOfString())));
	}

	/**
	 * see DATACASS-280.
	 */
	@Test
	public void shouldReadAndWriteEmptySetOfString() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setSetOfString(new HashSet<String>());

		cassandraOperations.insert(entity);
		AllPossibleTypes loaded = cassandraOperations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getSetOfString(), is(nullValue()));
	}

	/**
	 * see DATACASS-280.
	 */
	@Test
	public void shouldReadAndWriteListOfString() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setListOfString(Collections.singletonList("hello"));

		cassandraOperations.insert(entity);
		AllPossibleTypes loaded = cassandraOperations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getListOfString(), is(equalTo(entity.getListOfString())));
	}

	/**
	 * see DATACASS-280.
	 */
	@Test
	public void shouldReadAndWriteEmptyListOfString() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setListOfString(new ArrayList<String>());

		cassandraOperations.insert(entity);
		AllPossibleTypes loaded = cassandraOperations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getListOfString(), is(nullValue()));
	}

	/**
	 * see DATACASS-280.
	 */
	@Test
	public void shouldReadAndWriteMapOfString() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setMapOfString(Collections.singletonMap("hello", "world"));

		cassandraOperations.insert(entity);
		AllPossibleTypes loaded = cassandraOperations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getMapOfString(), is(equalTo(entity.getMapOfString())));
	}

	/**
	 * see DATACASS-280.
	 */
	@Test
	public void shouldReadAndWriteEmptyMapOfString() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setMapOfString(new HashMap<String, String>());

		cassandraOperations.insert(entity);
		AllPossibleTypes loaded = cassandraOperations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getMapOfString(), is(nullValue()));
	}

	/**
	 * see DATACASS-280.
	 */
	@Test
	public void shouldReadAndWriteEnum() {

		AllPossibleTypes entity = new AllPossibleTypes("1");
		entity.setAnEnum(Condition.MINT);

		cassandraOperations.insert(entity);
		AllPossibleTypes loaded = cassandraOperations.selectOneById(AllPossibleTypes.class, entity.getId());

		assertThat(loaded.getAnEnum(), is(equalTo(entity.getAnEnum())));
	}

	@Table
	@Data
	@NoArgsConstructor
	@RequiredArgsConstructor
	public static class AllPossibleTypes {

		@PrimaryKey @NonNull private String id;

		private InetAddress inet;

		@CassandraType(type = Name.UUID) private UUID uuid;

		@CassandraType(type = Name.INT) private Number justNumber;

		@CassandraType(type = Name.INT) private Short boxedShort;
		@CassandraType(type = Name.INT) private short primitiveShort;

		@CassandraType(type = Name.BIGINT) private Long boxedLong;
		@CassandraType(type = Name.BIGINT) private long primitiveLong;

		private Integer boxedInteger;
		private int primitiveInteger;

		private Float boxedFloat;
		private float primitiveFloat;

		private Double boxedDouble;
		private double primitiveDouble;

		private Boolean boxedBoolean;
		private boolean primitiveBoolean;

		private Date timestamp;
		private BigDecimal bigDecimal;
		private BigInteger bigInteger;
		private ByteBuffer blob;

		private Set<String> setOfString;
		private List<String> listOfString;
		private Map<String, String> mapOfString;

		private Condition anEnum;

	}

	public static enum Condition {
		MINT, USED;
	}
}
