/*
 * Copyright 2018-2019 the original author or authors.
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

import org.junit.Test;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.cql.CqlIdentifier;
import org.springframework.data.cassandra.domain.User;

import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

/**
 * Unit tests for {@link EntityQueryUtils}.
 *
 * @author Mark Paluch
 */
public class EntityQueryUtilsUnitTests {

	private final MappingCassandraConverter converter = new MappingCassandraConverter();

	@Test // DATACASS-106
	public void shouldRetrieveTableNameFromSelect() {

		Select select = QueryBuilder.select().from("keyspace", "table");

		CqlIdentifier tableName = EntityQueryUtils.getTableName(select);

		assertThat(tableName).isEqualTo(CqlIdentifier.of("table"));
	}

	@Test // DATACASS-106
	public void shouldRetrieveTableNameFromSimpleStatement() {

		assertThat(EntityQueryUtils.getTableName(new SimpleStatement("SELECT * FROM table")))
				.isEqualTo(CqlIdentifier.of("table"));
		assertThat(EntityQueryUtils.getTableName(new SimpleStatement("SELECT * FROM foo.table where")))
				.isEqualTo(CqlIdentifier.of("table"));
	}

	@Test // DATACASS-106
	public void shouldRetrieveQuotedTableNameFromSimpleStatement() {

		CqlIdentifier tableName = EntityQueryUtils.getTableName(new SimpleStatement("SELECT * from \"table\""));

		assertThat(tableName).isEqualTo(CqlIdentifier.of("table"));
	}

	@Test // DATACASS-569
	public void shouldCreateInsertQuery() {

		User user = new User("heisenberg", "Walter", "White");
		Insert insert = EntityQueryUtils.createInsertQuery("user", user, InsertOptions.builder().withIfNotExists().build(),
				converter, converter.getMappingContext().getRequiredPersistentEntity(User.class));

		assertThat(insert.toString())
				.isEqualTo("INSERT INTO user (firstname,id,lastname) VALUES ('Walter','heisenberg','White') IF NOT EXISTS;");
	}

	@Test // DATACASS-606
	public void shouldConsiderDeleteIfExists() {

		User user = new User("heisenberg", "Walter", "White");

		DeleteOptions options = DeleteOptions.builder().withIfExists().build();

		Delete delete = EntityQueryUtils.createDeleteQuery("foo", user, options, converter);

		assertThat(delete.toString()).isEqualTo("DELETE FROM foo WHERE id='heisenberg' IF EXISTS;");
	}
}
