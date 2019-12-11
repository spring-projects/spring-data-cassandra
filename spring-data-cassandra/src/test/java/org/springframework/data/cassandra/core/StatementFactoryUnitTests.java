/*
 * Copyright 2017-2020 the original author or authors.
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

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.convert.UpdateMapper;
import org.springframework.data.cassandra.core.cql.util.StatementBuilder;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.query.Columns;
import org.springframework.data.cassandra.core.query.Criteria;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.cassandra.core.query.Update;
import org.springframework.data.cassandra.domain.Group;
import org.springframework.data.domain.Sort;

import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.select.Select;

/**
 * Unit tests for {@link StatementFactory}.
 *
 * @author Mark Paluch
 */
public class StatementFactoryUnitTests {

	CassandraConverter converter = new MappingCassandraConverter();

	UpdateMapper updateMapper = new UpdateMapper(converter);

	StatementFactory statementFactory = new StatementFactory(updateMapper, updateMapper);

	CassandraPersistentEntity<?> groupEntity = converter.getMappingContext().getRequiredPersistentEntity(Group.class);
	CassandraPersistentEntity<?> personEntity = converter.getMappingContext().getRequiredPersistentEntity(Person.class);

	@Test // DATACASS-343
	public void shouldMapSimpleSelectQuery() {

		StatementBuilder<Select> select = statementFactory.select(Query.empty(),
				converter.getMappingContext().getRequiredPersistentEntity(Group.class));

		assertThat(select.build().getQuery()).isEqualTo("SELECT * FROM group");
	}

	@Test // DATACASS-343
	public void shouldMapSelectQueryWithColumnsAndCriteria() {

		Query query = Query.query(Criteria.where("foo").is("bar")).columns(Columns.from("age"));

		StatementBuilder<Select> select = statementFactory.select(query, groupEntity);

		assertThat(select.build().getQuery()).isEqualTo("SELECT age FROM group WHERE foo='bar'");
	}

	@Test // DATACASS-549
	public void shouldMapSelectQueryNotEquals() {

		Query query = Query.query(Criteria.where("foo").ne("bar")).columns(Columns.from("age"));

		StatementBuilder<Select> select = statementFactory.select(query, groupEntity);

		assertThat(select.build().getQuery()).isEqualTo("SELECT age FROM group WHERE foo!='bar'");
	}

	@Test // DATACASS-549
	public void shouldMapSelectQueryIsNotNull() {

		System.out.println(QueryBuilder.selectFrom("foo").column("age").build().getQuery());
		Query query = Query.query(Criteria.where("foo").isNotNull()).columns(Columns.from("age"));

		StatementBuilder<Select> select = statementFactory.select(query, groupEntity);

		assertThat(select.build().getQuery()).isEqualTo("SELECT age FROM group WHERE foo IS NOT NULL");
	}

	@Test // DATACASS-343
	public void shouldMapSelectQueryWithTtlColumns() {

		Query query = Query.empty().columns(Columns.empty().ttl("email"));

		StatementBuilder<Select> select = statementFactory.select(query,
				converter.getMappingContext().getRequiredPersistentEntity(Group.class));

		assertThat(select.build().getQuery()).isEqualTo("SELECT ttl(email) FROM group");
	}

	@Test // DATACASS-343
	public void shouldMapSelectQueryWithSortLimitAndAllowFiltering() {

		Query query = Query.empty().sort(Sort.by("id.hashPrefix")).limit(10).withAllowFiltering();

		StatementBuilder<Select> select = statementFactory.select(query,
				converter.getMappingContext().getRequiredPersistentEntity(Group.class));

		assertThat(select.build().getQuery())
				.isEqualTo("SELECT * FROM group ORDER BY hash_prefix ASC LIMIT 10 ALLOW FILTERING");
	}

	@Test // DATACASS-343
	public void shouldMapDeleteQueryWithColumns() {

		Query query = Query.empty().columns(Columns.from("age"));

		StatementBuilder<Delete> delete = statementFactory.delete(query,
				converter.getMappingContext().getRequiredPersistentEntity(Group.class));

		assertThat(delete.build().getQuery()).isEqualTo("DELETE age FROM group");
	}

	@Test // DATACASS-343
	public void shouldMapDeleteQueryWithTtlColumns() {

		Query query = Query.query(Criteria.where("foo").is("bar"));

		StatementBuilder<Delete> delete = statementFactory.delete(query,
				converter.getMappingContext().getRequiredPersistentEntity(Group.class));

		assertThat(delete.build().getQuery()).isEqualTo("DELETE FROM group WHERE foo='bar'");
	}

	@Test // DATACASS-343
	public void shouldCreateSetUpdate() {

		Query query = Query.query(Criteria.where("foo").is("bar"));

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> update = statementFactory.update(query,
				Update.empty().set("firstName", "baz").set("boo", "baa"), personEntity);

		assertThat(update.build().getQuery()).isEqualTo("UPDATE person SET first_name='baz', boo='baa' WHERE foo='bar'");
	}

	@Test // DATACASS-343
	public void shouldCreateSetAtIndexUpdate() {

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> update = statementFactory
				.update(Query.empty(), Update.empty().set("list").atIndex(10).to("Euro"), personEntity);

		assertThat(update.build().getQuery()).isEqualTo("UPDATE person SET list[10]='Euro'");
	}

	@Test // DATACASS-343
	public void shouldCreateSetAtKeyUpdate() {

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> update = statementFactory
				.update(Query.empty(), Update.empty().set("map").atKey("baz").to("Euro"), personEntity);

		assertThat(update.build().getQuery()).isEqualTo("UPDATE person SET map['baz']='Euro'");
	}

	@Test // DATACASS-343
	public void shouldAddToMap() {

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> update = statementFactory
				.update(Query.empty(), Update.empty().addTo("map").entry("foo", "Euro"), personEntity);

		assertThat(update.build().getQuery()).isEqualTo("UPDATE person SET map+={'foo':'Euro'}");
	}

	@Test // DATACASS-343
	public void shouldPrependAllToList() {

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> update = statementFactory
				.update(Query.empty(), Update.empty().addTo("list").prependAll("foo", "Euro"), personEntity);

		assertThat(update.build().getQuery()).isEqualTo("UPDATE person SET list=['foo','Euro']+list");
	}

	@Test // DATACASS-343
	public void shouldAppendAllToList() {

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> update = statementFactory
				.update(Query.empty(), Update.empty().addTo("list").appendAll("foo", "Euro"), personEntity);

		assertThat(update.build().getQuery()).isEqualTo("UPDATE person SET list+=['foo','Euro']");
	}

	@Test // DATACASS-343
	public void shouldRemoveFromList() {

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> update = statementFactory
				.update(Query.empty(), Update.empty().remove("list", "Euro"), personEntity);

		assertThat(update.build().getQuery()).isEqualTo("UPDATE person SET list-=['Euro']");
	}

	@Test // DATACASS-343
	public void shouldClearList() {

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> update = statementFactory
				.update(Query.empty(), Update.empty().clear("list"), personEntity);

		assertThat(update.build().getQuery()).isEqualTo("UPDATE person SET list=[]");
	}

	@Test // DATACASS-343
	public void shouldAddAllToSet() {

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> update = statementFactory
				.update(Query.empty(), Update.empty().addTo("set").appendAll("foo", "Euro"), personEntity);

		assertThat(update.build().getQuery()).isEqualTo("UPDATE person SET set_col+={'foo','Euro'}");
	}

	@Test // DATACASS-343
	public void shouldRemoveFromSet() {

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> update = statementFactory
				.update(Query.empty(), Update.empty().remove("set", "Euro"), personEntity);

		assertThat(update.build().getQuery()).isEqualTo("UPDATE person SET set_col-={'Euro'}");
	}

	@Test // DATACASS-343
	public void shouldClearSet() {

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> update = statementFactory
				.update(Query.empty(), Update.empty().clear("set"), personEntity);

		assertThat(update.build().getQuery()).isEqualTo("UPDATE person SET set_col={}");
	}

	@Test // DATACASS-343
	public void shouldCreateIncrementUpdate() {

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> update = statementFactory
				.update(Query.empty(), Update.empty().increment("number"), personEntity);

		assertThat(update.build().getQuery()).isEqualTo("UPDATE person SET number+=1");
	}

	@Test // DATACASS-343
	public void shouldCreateDecrementUpdate() {

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> update = statementFactory
				.update(Query.empty(), Update.empty().decrement("number"), personEntity);

		assertThat(update.build().getQuery()).isEqualTo("UPDATE person SET number-=1");
	}

	@Test // DATACASS-569
	public void shouldCreateSetUpdateIfExists() {

		Query query = Query.query(Criteria.where("foo").is("bar"))
				.queryOptions(UpdateOptions.builder().withIfExists().build());

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> update = statementFactory.update(query,
				Update.empty().set("firstName", "baz"), personEntity);

		assertThat(update.build().getQuery()).isEqualTo("UPDATE person SET first_name='baz' WHERE foo='bar' IF EXISTS");
	}

	@Test // DATACASS-656
	public void shouldCreateSetUpdateIfCondition() {

		Query query = Query.query(Criteria.where("foo").is("bar"))
				.queryOptions(UpdateOptions.builder().ifCondition(Criteria.where("foo").is("baz")).build());

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> update = statementFactory.update(query,
				Update.empty().set("firstName", "baz"), personEntity);

		assertThat(update.build().getQuery()).isEqualTo("UPDATE person SET first_name='baz' WHERE foo='bar' IF foo='baz'");
	}

	@Test // DATACASS-512
	public void shouldCreateCountQuery() {

		Query query = Query.query(Criteria.where("foo").is("bar"));

		StatementBuilder<Select> count = statementFactory.count(query,
				converter.getMappingContext().getRequiredPersistentEntity(Group.class));

		assertThat(count.build().getQuery()).isEqualTo("SELECT count(1) FROM group WHERE foo='bar'");
	}

	static class Person {

		@Id String id;

		List<String> list;
		@Column("set_col") Set<String> set;
		Map<String, String> map;

		Integer number;

		@Column("first_name") String firstName;
	}
}
