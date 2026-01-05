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
package org.springframework.data.cassandra.core.cql.generator;

import static org.assertj.core.api.Assertions.*;

import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.data.cassandra.core.cql.keyspace.AlterTableSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.SpecificationBuilder;
import org.springframework.data.cassandra.core.cql.keyspace.TableOption;
import org.springframework.data.cassandra.core.cql.keyspace.TableOption.CachingOption;
import org.springframework.data.cassandra.core.cql.keyspace.TableOption.KeyCachingOption;

import com.datastax.oss.driver.api.core.type.DataTypes;

/**
 * Unit tests for {@link AlterTableCqlGenerator}.
 *
 * @author Matthew T. Adams
 * @author David Webb
 * @author Mark Paluch
 * @author Seungho Kang
 */
class AlterTableCqlGeneratorUnitTests {

	@Test // DATACASS-192
	void alterTableAlterColumnType() {

		AlterTableSpecification spec = SpecificationBuilder.alterTable("addamsFamily").alter("lastKnownLocation",
				DataTypes.UUID);

		assertThat(toCql(spec)).isEqualTo("ALTER TABLE addamsfamily ALTER lastknownlocation TYPE uuid;");
	}

	@Test // DATACASS-192
	void alterTableAlterListColumnType() {

		AlterTableSpecification spec = SpecificationBuilder.alterTable("addamsFamily").alter("lastKnownLocation",
				DataTypes.listOf(DataTypes.ASCII));

		assertThat(toCql(spec)).isEqualTo("ALTER TABLE addamsfamily ALTER lastknownlocation TYPE list<ascii>;");
	}

	@Test // DATACASS-192
	void alterTableAddColumn() {

		AlterTableSpecification spec = SpecificationBuilder.alterTable("addamsFamily").add("gravesite", DataTypes.TEXT);

		assertThat(toCql(spec)).isEqualTo("ALTER TABLE addamsfamily ADD gravesite text;");
	}

	@Test // DATACASS-192
	void alterTableAddListColumn() {

		AlterTableSpecification spec = SpecificationBuilder.alterTable("users").add("top_places",
				DataTypes.listOf(DataTypes.ASCII));

		assertThat(toCql(spec)).isEqualTo("ALTER TABLE users ADD top_places list<ascii>;");
	}

	@Test // DATACASS-192
	void alterTableDropColumn() {

		AlterTableSpecification spec = SpecificationBuilder.alterTable("addamsFamily").drop("gender");

		assertThat(toCql(spec)).isEqualTo("ALTER TABLE addamsfamily DROP gender;");
	}

	@Test // DATACASS-192
	void alterTableRenameColumn() {

		AlterTableSpecification spec = SpecificationBuilder.alterTable("addamsFamily").rename("firstname", "lastname");

		assertThat(toCql(spec)).isEqualTo("ALTER TABLE addamsfamily RENAME firstname TO lastname;");
	}

	@Test // DATACASS-192
	void alterTableAddCommentAndTableOption() {

		AlterTableSpecification spec = SpecificationBuilder.alterTable("addamsFamily")
				.with(TableOption.READ_REPAIR_CHANCE, 0.2f).with(TableOption.COMMENT, "A most excellent and useful table");

		assertThat(toCql(spec)).isEqualTo(
				"ALTER TABLE addamsfamily WITH read_repair_chance = 0.2 AND comment = 'A most excellent and useful table';");
	}

	@Test // DATACASS-192
	void alterTableAddColumnAndComment() {

		AlterTableSpecification spec = SpecificationBuilder.alterTable("addamsFamily")
				.add("top_places", DataTypes.listOf(DataTypes.ASCII)).add("other", DataTypes.listOf(DataTypes.ASCII))
				.with(TableOption.COMMENT, "A most excellent and useful table");

		assertThat(toCql(spec)).isEqualTo(
				"ALTER TABLE addamsfamily ADD top_places list<ascii> ADD other list<ascii> WITH comment = 'A most excellent and useful table';");
	}

	@Test // DATACASS-192
	void alterTableAddCaching() {

		Map<Object, Object> cachingMap = new LinkedHashMap<>();
		cachingMap.put(CachingOption.KEYS, KeyCachingOption.NONE);
		cachingMap.put(CachingOption.ROWS_PER_PARTITION, "15");

		AlterTableSpecification spec = SpecificationBuilder.alterTable("users").with(TableOption.CACHING, cachingMap);

		assertThat(toCql(spec))
				.isEqualTo("ALTER TABLE users WITH caching = { 'keys' : 'none', 'rows_per_partition' : '15' };");
	}

	@Test // GH-1584
	void alterTableSetDefaultTimeToLive() {

		AlterTableSpecification spec = SpecificationBuilder.alterTable("users")
				.with(TableOption.DEFAULT_TIME_TO_LIVE, 3600)
				.with(TableOption.CDC)
				.with(TableOption.SPECULATIVE_RETRY, "90percentile")
				.with(TableOption.MEMTABLE_FLUSH_PERIOD_IN_MS, 1000L)
				.with(TableOption.CRC_CHECK_CHANCE, 0.9)
				.with(TableOption.MIN_INDEX_INTERVAL, 128L)
				.with(TableOption.MAX_INDEX_INTERVAL, 2048L)
				.with(TableOption.READ_REPAIR, "BLOCKING");

		assertThat(toCql(spec))
				.isEqualTo("ALTER TABLE users WITH default_time_to_live = 3600 AND cdc = true AND speculative_retry = '90percentile' AND memtable_flush_period_in_ms = 1000 AND crc_check_chance = 0.9 AND min_index_interval = 128 AND max_index_interval = 2048 AND read_repair = 'BLOCKING';");
	}

	private String toCql(AlterTableSpecification spec) {
		return CqlGenerator.toCql(spec);
	}
}
