/*
 * Copyright 2017-2018 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;

import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.UUID;

import org.junit.Test;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.DataType.Name;

/**
 * Unit tests for {@link CassandraSimpleTypeHolder}.
 *
 * @author Mark Paluch
 */
public class CassandraSimpleTypeHolderUnitTests {

	@Test // DATACASS-488
	public void shouldResolveTypeNamesForAllPrimaryTypes() {

		EnumSet<Name> excluded = EnumSet.of(Name.CUSTOM, Name.MAP, Name.SET, Name.LIST, Name.UDT, Name.TUPLE);

		for (Name name : Name.values()) {

			if (excluded.contains(name)) {
				continue;
			}

			assertThat(CassandraSimpleTypeHolder.getDataTypeFor(name)).as("SimpleType for %s", name).isNotNull();
		}
	}

	@Test // DATACASS-128
	public void mapStringToVarchar() {

		assertThat(CassandraSimpleTypeHolder.getDataTypeFor(Name.VARCHAR)).isSameAs(DataType.varchar());
		assertThat(CassandraSimpleTypeHolder.getDataTypeFor(Name.TEXT)).isSameAs(DataType.text());
		assertThat(CassandraSimpleTypeHolder.getDataTypeFor(Name.ASCII)).isSameAs(DataType.ascii());

		assertThat(CassandraSimpleTypeHolder.getDataTypeFor(String.class)).isSameAs(DataType.text());
	}

	@Test // DATACASS-128
	public void mapLongToBigint() {

		assertThat(CassandraSimpleTypeHolder.getDataTypeFor(Name.BIGINT)).isSameAs(DataType.bigint());
		assertThat(CassandraSimpleTypeHolder.getDataTypeFor(Name.COUNTER)).isSameAs(DataType.counter());

		assertThat(CassandraSimpleTypeHolder.getDataTypeFor(Long.class)).isSameAs(DataType.bigint());
	}

	@Test // DATACASS-128
	public void mapByteBufferToBlob() {

		assertThat(CassandraSimpleTypeHolder.getDataTypeFor(Name.BLOB)).isSameAs(DataType.blob());
		assertThat(CassandraSimpleTypeHolder.getDataTypeFor(Name.CUSTOM)).isNull();

		assertThat(CassandraSimpleTypeHolder.getDataTypeFor(ByteBuffer.class)).isSameAs(DataType.blob());
	}

	@Test // DATACASS-128
	public void mapUuidToUuid() {

		assertThat(CassandraSimpleTypeHolder.getDataTypeFor(Name.UUID)).isSameAs(DataType.uuid());
		assertThat(CassandraSimpleTypeHolder.getDataTypeFor(Name.TIMEUUID)).isSameAs(DataType.timeuuid());

		assertThat(CassandraSimpleTypeHolder.getDataTypeFor(UUID.class)).isSameAs(DataType.uuid());
	}
}
