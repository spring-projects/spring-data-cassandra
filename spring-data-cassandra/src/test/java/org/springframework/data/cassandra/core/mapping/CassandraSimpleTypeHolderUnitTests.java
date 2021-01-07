/*
 * Copyright 2017-2021 the original author or authors.
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
package org.springframework.data.cassandra.core.mapping;

import static org.assertj.core.api.Assertions.*;

import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import org.springframework.data.cassandra.core.mapping.CassandraType.Name;

import com.datastax.oss.driver.api.core.type.DataTypes;

/**
 * Unit tests for {@link CassandraSimpleTypeHolder}.
 *
 * @author Mark Paluch
 */
class CassandraSimpleTypeHolderUnitTests {

	@Test // DATACASS-488
	void shouldResolveTypeNamesForAllPrimaryTypes() {

		EnumSet<Name> excluded = EnumSet.of(Name.MAP, Name.SET, Name.LIST, Name.UDT, Name.TUPLE);

		for (Name name : Name.values()) {

			if (excluded.contains(name)) {
				continue;
			}

			assertThat(CassandraSimpleTypeHolder.getDataTypeFor(name)).as("SimpleType for %s", name).isNotNull();
		}
	}

	@Test // DATACASS-128
	void mapStringToVarchar() {

		assertThat(CassandraSimpleTypeHolder.getDataTypeFor(Name.VARCHAR)).isSameAs(DataTypes.TEXT);
		assertThat(CassandraSimpleTypeHolder.getDataTypeFor(Name.TEXT)).isSameAs(DataTypes.TEXT);
		assertThat(CassandraSimpleTypeHolder.getDataTypeFor(Name.ASCII)).isSameAs(DataTypes.ASCII);

		assertThat(CassandraSimpleTypeHolder.getDataTypeFor(String.class)).isSameAs(DataTypes.TEXT);
	}

	@Test // DATACASS-128
	void mapLongToBigint() {

		assertThat(CassandraSimpleTypeHolder.getDataTypeFor(Name.BIGINT)).isSameAs(DataTypes.BIGINT);
		assertThat(CassandraSimpleTypeHolder.getDataTypeFor(Name.COUNTER)).isSameAs(DataTypes.COUNTER);

		assertThat(CassandraSimpleTypeHolder.getDataTypeFor(Long.class)).isSameAs(DataTypes.BIGINT);
	}

	@Test // DATACASS-128
	void mapByteBufferToBlob() {

		assertThat(CassandraSimpleTypeHolder.getDataTypeFor(Name.BLOB)).isSameAs(DataTypes.BLOB);

		assertThat(CassandraSimpleTypeHolder.getDataTypeFor(ByteBuffer.class)).isSameAs(DataTypes.BLOB);
	}

	@Test // DATACASS-128
	void mapUuidToUuid() {

		assertThat(CassandraSimpleTypeHolder.getDataTypeFor(Name.UUID)).isSameAs(DataTypes.UUID);
		assertThat(CassandraSimpleTypeHolder.getDataTypeFor(Name.TIMEUUID)).isSameAs(DataTypes.TIMEUUID);

		assertThat(CassandraSimpleTypeHolder.getDataTypeFor(UUID.class)).isSameAs(DataTypes.UUID);
	}
}
