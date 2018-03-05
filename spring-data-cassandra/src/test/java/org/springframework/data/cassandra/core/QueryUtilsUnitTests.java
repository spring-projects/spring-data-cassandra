/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.cassandra.core;

import static org.assertj.core.api.Assertions.*;

import org.junit.Test;
import org.springframework.data.cassandra.core.cql.CqlIdentifier;

import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

/**
 * Unit tests for {@link QueryUtils}.
 *
 * @author Mark Paluch
 */
public class QueryUtilsUnitTests {

	@Test // DATACASS-106
	public void shouldRetrieveTableNameFromSelect() {

		Select select = QueryBuilder.select().from("keyspace", "table");

		CqlIdentifier tableName = QueryUtils.getTableName(select);

		assertThat(tableName).isEqualTo(CqlIdentifier.of("table"));
	}

	@Test // DATACASS-106
	public void shouldRetrieveTableNameFromSimpleStatement() {

		assertThat(QueryUtils.getTableName(new SimpleStatement("SELECT * FROM table")))
				.isEqualTo(CqlIdentifier.of("table"));
		assertThat(QueryUtils.getTableName(new SimpleStatement("SELECT * FROM foo.table where")))
				.isEqualTo(CqlIdentifier.of("table"));
	}

	@Test // DATACASS-106
	public void shouldRetrieveQuotedTableNameFromSimpleStatement() {

		CqlIdentifier tableName = QueryUtils.getTableName(new SimpleStatement("SELECT * from \"table\""));

		assertThat(tableName).isEqualTo(CqlIdentifier.of("table"));
	}
}
