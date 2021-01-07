/*
 * Copyright 2018-2021 the original author or authors.
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
package org.springframework.data.cassandra.core;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.select.Select;

/**
 * Unit tests for {@link EntityQueryUtils}.
 *
 * @author Mark Paluch
 */
class EntityQueryUtilsUnitTests {

	@Test // DATACASS-106
	void shouldRetrieveTableNameFromSelect() {

		Select select = QueryBuilder.selectFrom("ks", "tbl").all().where();

		CqlIdentifier tableName = EntityQueryUtils.getTableName(select.build());

		assertThat(tableName).isEqualTo(CqlIdentifier.fromInternal("tbl"));
	}

	@Test // DATACASS-642
	void shouldRetrieveQuotedTableNameFromSelect() {

		Select select = QueryBuilder.selectFrom(CqlIdentifier.fromCql("\"table\"")).all().where();

		CqlIdentifier tableName = EntityQueryUtils.getTableName(select.build());

		assertThat(tableName).isEqualTo(CqlIdentifier.fromInternal("table"));
	}

	@Test // DATACASS-106
	void shouldRetrieveTableNameFromSimpleStatement() {

		assertThat(EntityQueryUtils.getTableName(SimpleStatement.newInstance("SELECT * FROM table")))
				.isEqualTo(CqlIdentifier.fromInternal("table"));
		assertThat(EntityQueryUtils.getTableName(SimpleStatement.newInstance("SELECT * FROM foo.table where")))
				.isEqualTo(CqlIdentifier.fromInternal("table"));
	}

	@Test // DATACASS-106
	void shouldRetrieveQuotedTableNameFromSimpleStatement() {

		CqlIdentifier tableName = EntityQueryUtils.getTableName(SimpleStatement.newInstance("SELECT * from \"table\""));

		assertThat(tableName).isEqualTo(CqlIdentifier.fromInternal("table"));
	}
}
