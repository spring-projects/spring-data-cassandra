/*
 * Copyright 2017 the original author or authors.
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
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.convert.QueryMapper;
import org.springframework.data.cassandra.core.query.Columns;
import org.springframework.data.cassandra.core.query.Criteria;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.cassandra.domain.Group;
import org.springframework.data.domain.Sort;

import com.datastax.driver.core.Statement;

/**
 * Unit tests for {@link StatementFactory}.
 *
 * @author Mark Paluch
 */
public class StatementFactoryUnitTests {

	CassandraConverter converter = new MappingCassandraConverter();

	QueryMapper queryMapper = new QueryMapper(converter);

	StatementFactory statementFactory = new StatementFactory(queryMapper);

	@Test // DATACASS-343
	public void shouldMapSimpleSelectQuery() {

		Statement select = statementFactory.select(new Query(),
				converter.getMappingContext().getPersistentEntity(Group.class));

		assertThat(select.toString()).isEqualTo("SELECT * FROM group;");
	}

	@Test // DATACASS-343
	public void shouldMapSelectQueryWithColumnsAndCriteria() {

		Query query = Query.from(Criteria.where("foo").is("bar"));
		query.with(Columns.from("age").exclude("id"));

		Statement select = statementFactory.select(query, converter.getMappingContext().getPersistentEntity(Group.class));

		assertThat(select.toString()).isEqualTo("SELECT age,email FROM group WHERE foo='bar';");
	}

	@Test // DATACASS-343
	public void shouldMapSelectQueryWithTtlColumns() {

		Query query = new Query();
		query.with(Columns.empty().ttl("email").exclude("age").exclude("id"));

		Statement select = statementFactory.select(query, converter.getMappingContext().getPersistentEntity(Group.class));

		assertThat(select.toString()).isEqualTo("SELECT TTL(email) FROM group;");
	}

	@Test // DATACASS-343
	public void shouldMapSelectQueryWithSortLimitAndAllowFiltering() {

		Query query = new Query();
		query.with(new Sort("id.hashPrefix")).limit(10).withAllowFiltering();

		Statement select = statementFactory.select(query, converter.getMappingContext().getPersistentEntity(Group.class));

		assertThat(select.toString()).isEqualTo("SELECT * FROM group ORDER BY hash_prefix ASC LIMIT 10 ALLOW FILTERING;");
	}

	@Test // DATACASS-343
	public void shouldMapDeleteQueryWithColumns() {

		Query query = new Query();
		query.with(Columns.from("age").exclude("id"));

		Statement delete = statementFactory.delete(query, converter.getMappingContext().getPersistentEntity(Group.class));

		assertThat(delete.toString()).isEqualTo("DELETE age,email FROM group;");
	}

	@Test // DATACASS-343
	public void shouldMapDeleteQueryWithTtlColumns() {

		Query query = Query.from(Criteria.where("foo").is("bar"));

		Statement delete = statementFactory.delete(query, converter.getMappingContext().getPersistentEntity(Group.class));

		assertThat(delete.toString()).isEqualTo("DELETE FROM group WHERE foo='bar';");
	}
}
