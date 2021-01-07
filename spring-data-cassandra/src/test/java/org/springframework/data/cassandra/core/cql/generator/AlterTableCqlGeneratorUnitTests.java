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
package org.springframework.data.cassandra.core.cql.generator;

import static org.assertj.core.api.Assertions.*;

import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.springframework.data.cassandra.core.cql.keyspace.AlterTableSpecification;
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
 */
class AlterTableCqlGeneratorUnitTests {

	@Test // DATACASS-192
	void alterTableAlterColumnType() {

		AlterTableSpecification spec = AlterTableSpecification.alterTable("addamsFamily").alter("lastKnownLocation",
				DataTypes.UUID);

		assertThat(toCql(spec)).isEqualTo("ALTER TABLE addamsfamily ALTER lastknownlocation TYPE uuid;");
	}

	@Test // DATACASS-192
	void alterTableAlterListColumnType() {

		AlterTableSpecification spec = AlterTableSpecification.alterTable("addamsFamily").alter("lastKnownLocation",
				DataTypes.listOf(DataTypes.ASCII));

		assertThat(toCql(spec)).isEqualTo("ALTER TABLE addamsfamily ALTER lastknownlocation TYPE list<ascii>;");
	}

	@Test // DATACASS-192
	void alterTableAddColumn() {

		AlterTableSpecification spec = AlterTableSpecification.alterTable("addamsFamily").add("gravesite", DataTypes.TEXT);

		assertThat(toCql(spec)).isEqualTo("ALTER TABLE addamsfamily ADD gravesite text;");
	}

	@Test // DATACASS-192
	void alterTableAddListColumn() {

		AlterTableSpecification spec = AlterTableSpecification.alterTable("users").add("top_places",
				DataTypes.listOf(DataTypes.ASCII));

		assertThat(toCql(spec)).isEqualTo("ALTER TABLE users ADD top_places list<ascii>;");
	}

	@Test // DATACASS-192
	void alterTableDropColumn() {

		AlterTableSpecification spec = AlterTableSpecification.alterTable("addamsFamily").drop("gender");

		assertThat(toCql(spec)).isEqualTo("ALTER TABLE addamsfamily DROP gender;");
	}

	@Test // DATACASS-192
	void alterTableRenameColumn() {

		AlterTableSpecification spec = AlterTableSpecification.alterTable("addamsFamily").rename("firstname", "lastname");

		assertThat(toCql(spec)).isEqualTo("ALTER TABLE addamsfamily RENAME firstname TO lastname;");
	}

	@Test // DATACASS-192
	void alterTableAddCommentAndTableOption() {

		AlterTableSpecification spec = AlterTableSpecification.alterTable("addamsFamily")
				.with(TableOption.READ_REPAIR_CHANCE, 0.2f).with(TableOption.COMMENT, "A most excellent and useful table");

		assertThat(toCql(spec)).isEqualTo(
				"ALTER TABLE addamsfamily WITH read_repair_chance = 0.2 AND comment = 'A most excellent and useful table';");
	}

	@Test // DATACASS-192
	void alterTableAddColumnAndComment() {

		AlterTableSpecification spec = AlterTableSpecification.alterTable("addamsFamily")
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

		AlterTableSpecification spec = AlterTableSpecification.alterTable("users").with(TableOption.CACHING, cachingMap);

		assertThat(toCql(spec))
				.isEqualTo("ALTER TABLE users WITH caching = { 'keys' : 'none', 'rows_per_partition' : '15' };");
	}

	private String toCql(AlterTableSpecification spec) {
		return new AlterTableCqlGenerator(spec).toCql();
	}
}
