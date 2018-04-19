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
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.query.Columns;
import org.springframework.data.cassandra.core.query.Criteria;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.cassandra.core.query.Update;
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

	UpdateMapper updateMapper = new UpdateMapper(converter);

	StatementFactory statementFactory = new StatementFactory(updateMapper, updateMapper);

	CassandraPersistentEntity<?> groupEntity = converter.getMappingContext().getRequiredPersistentEntity(Group.class);
	CassandraPersistentEntity<?> personEntity = converter.getMappingContext().getRequiredPersistentEntity(Person.class);

	@Test // DATACASS-343
	public void shouldMapSimpleSelectQuery() {

		Statement select = statementFactory.select(Query.empty(),
				converter.getMappingContext().getRequiredPersistentEntity(Group.class));

		assertThat(select.toString()).isEqualTo("SELECT * FROM group;");
	}

	@Test // DATACASS-343
	public void shouldMapSelectQueryWithColumnsAndCriteria() {

		Query query = Query.query(Criteria.where("foo").is("bar")).columns(Columns.from("age"));

		Statement select = statementFactory.select(query, groupEntity);

		assertThat(select.toString()).isEqualTo("SELECT age FROM group WHERE foo='bar';");
	}

	@Test // DATACASS-549
	public void shouldMapSelectQueryNotEquals() {

		Query query = Query.query(Criteria.where("foo").ne("bar")).columns(Columns.from("age"));

		Statement select = statementFactory.select(query, groupEntity);

		assertThat(select.toString()).isEqualTo("SELECT age FROM group WHERE foo!='bar';");
	}

	@Test // DATACASS-549
	public void shouldMapSelectQueryIsNotNull() {

		Query query = Query.query(Criteria.where("foo").isNotNull()).columns(Columns.from("age"));

		Statement select = statementFactory.select(query, groupEntity);

		assertThat(select.toString()).isEqualTo("SELECT age FROM group WHERE foo IS NOT NULL;");
	}

	@Test // DATACASS-343
	public void shouldMapSelectQueryWithTtlColumns() {

		Query query = Query.empty().columns(Columns.empty().ttl("email"));

		Statement select = statementFactory.select(query,
				converter.getMappingContext().getRequiredPersistentEntity(Group.class));

		assertThat(select.toString()).isEqualTo("SELECT TTL(email) FROM group;");
	}

	@Test // DATACASS-343
	public void shouldMapSelectQueryWithSortLimitAndAllowFiltering() {

		Query query = Query.empty().sort(Sort.by("id.hashPrefix")).limit(10).withAllowFiltering();

		Statement select = statementFactory.select(query,
				converter.getMappingContext().getRequiredPersistentEntity(Group.class));

		assertThat(select.toString()).isEqualTo("SELECT * FROM group ORDER BY hash_prefix ASC LIMIT 10 ALLOW FILTERING;");
	}

	@Test // DATACASS-343
	public void shouldMapDeleteQueryWithColumns() {

		Query query = Query.empty().columns(Columns.from("age"));

		Statement delete = statementFactory.delete(query,
				converter.getMappingContext().getRequiredPersistentEntity(Group.class));

		assertThat(delete.toString()).isEqualTo("DELETE age FROM group;");
	}

	@Test // DATACASS-343
	public void shouldMapDeleteQueryWithTtlColumns() {

		Query query = Query.query(Criteria.where("foo").is("bar"));

		Statement delete = statementFactory.delete(query,
				converter.getMappingContext().getRequiredPersistentEntity(Group.class));

		assertThat(delete.toString()).isEqualTo("DELETE FROM group WHERE foo='bar';");
	}

	@Test // DATACASS-343
	public void shouldCreateSetUpdate() {

		Query query = Query.query(Criteria.where("foo").is("bar"));

		Statement update = statementFactory.update(query, Update.empty().set("firstName", "baz").set("boo", "baa"),
				personEntity);

		assertThat(update.toString()).isEqualTo("UPDATE person SET first_name='baz',boo='baa' WHERE foo='bar';");
	}

	@Test // DATACASS-343
	public void shouldCreateSetAtIndexUpdate() {

		Statement update = statementFactory.update(Query.empty(), Update.empty().set("list").atIndex(10).to("Euro"),
				personEntity);

		assertThat(update.toString()).isEqualTo("UPDATE person SET list[10]='Euro';");
	}

	@Test // DATACASS-343
	public void shouldCreateSetAtKeyUpdate() {

		Statement update = statementFactory.update(Query.empty(), Update.empty().set("map").atKey("baz").to("Euro"),
				personEntity);

		assertThat(update.toString()).isEqualTo("UPDATE person SET map['baz']='Euro';");
	}

	@Test // DATACASS-343
	public void shouldAddToMap() {

		Statement update = statementFactory.update(Query.empty(), Update.empty().addTo("map").entry("foo", "Euro"),
				personEntity);

		assertThat(update.toString()).isEqualTo("UPDATE person SET map=map+{'foo':'Euro'};");
	}

	@Test // DATACASS-343
	public void shouldPrependAllToList() {

		Statement update = statementFactory.update(Query.empty(), Update.empty().addTo("list").prependAll("foo", "Euro"),
				personEntity);

		assertThat(update.toString()).isEqualTo("UPDATE person SET list=['foo','Euro']+list;");
	}

	@Test // DATACASS-343
	public void shouldAppendAllToList() {

		Statement update = statementFactory.update(Query.empty(), Update.empty().addTo("list").appendAll("foo", "Euro"),
				personEntity);

		assertThat(update.toString()).isEqualTo("UPDATE person SET list=list+['foo','Euro'];");
	}

	@Test // DATACASS-343
	public void shouldRemoveFromList() {

		Statement update = statementFactory.update(Query.empty(), Update.empty().remove("list", "Euro"), personEntity);

		assertThat(update.toString()).isEqualTo("UPDATE person SET list=list-['Euro'];");
	}

	@Test // DATACASS-343
	public void shouldClearList() {

		Statement update = statementFactory.update(Query.empty(), Update.empty().clear("list"), personEntity);

		assertThat(update.toString()).isEqualTo("UPDATE person SET list=[];");
	}

	@Test // DATACASS-343
	public void shouldAddAllToSet() {

		Statement update = statementFactory.update(Query.empty(), Update.empty().addTo("set").appendAll("foo", "Euro"),
				personEntity);

		assertThat(update.toString()).isEqualTo("UPDATE person SET set_col=set_col+{'foo','Euro'};");
	}

	@Test // DATACASS-343
	public void shouldRemoveFromSet() {

		Statement update = statementFactory.update(Query.empty(), Update.empty().remove("set", "Euro"), personEntity);

		assertThat(update.toString()).isEqualTo("UPDATE person SET set_col=set_col-{'Euro'};");
	}

	@Test // DATACASS-343
	public void shouldClearSet() {

		Statement update = statementFactory.update(Query.empty(), Update.empty().clear("set"), personEntity);

		assertThat(update.toString()).isEqualTo("UPDATE person SET set_col={};");
	}

	@Test // DATACASS-343
	public void shouldCreateIncrementUpdate() {

		Statement update = statementFactory.update(Query.empty(), Update.empty().increment("number"), personEntity);

		assertThat(update.toString()).isEqualTo("UPDATE person SET number=number+1;");
	}

	@Test // DATACASS-343
	public void shouldCreateDecrementUpdate() {

		Statement update = statementFactory.update(Query.empty(), Update.empty().decrement("number"), personEntity);

		assertThat(update.toString()).isEqualTo("UPDATE person SET number=number-1;");
	}

	@Test // DATACASS-512
	public void shouldCreateCountQuery() {

		Query query = Query.query(Criteria.where("foo").is("bar"));

		Statement count = statementFactory.count(query,
				converter.getMappingContext().getRequiredPersistentEntity(Group.class));

		assertThat(count.toString()).isEqualTo("SELECT COUNT(1) FROM group WHERE foo='bar';");
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
