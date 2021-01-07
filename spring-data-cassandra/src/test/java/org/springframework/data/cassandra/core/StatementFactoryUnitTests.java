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
package org.springframework.data.cassandra.core;

import static org.assertj.core.api.Assertions.*;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.convert.UpdateMapper;
import org.springframework.data.cassandra.core.cql.QueryOptions;
import org.springframework.data.cassandra.core.cql.WriteOptions;
import org.springframework.data.cassandra.core.cql.util.StatementBuilder;
import org.springframework.data.cassandra.core.cql.util.StatementBuilder.ParameterHandling;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.query.Columns;
import org.springframework.data.cassandra.core.query.Criteria;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.cassandra.core.query.Update;
import org.springframework.data.cassandra.domain.Group;
import org.springframework.data.domain.Sort;

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.datastax.oss.driver.api.querybuilder.select.Select;

/**
 * Unit tests for {@link StatementFactory}.
 *
 * @author Mark Paluch
 */
class StatementFactoryUnitTests {

	private CassandraConverter converter = new MappingCassandraConverter();

	private UpdateMapper updateMapper = new UpdateMapper(converter);

	private StatementFactory statementFactory = new StatementFactory(updateMapper, updateMapper);

	private CassandraPersistentEntity<?> groupEntity = converter.getMappingContext()
			.getRequiredPersistentEntity(Group.class);
	private CassandraPersistentEntity<?> personEntity = converter.getMappingContext()
			.getRequiredPersistentEntity(Person.class);

	@Test // DATACASS-343
	void shouldMapSimpleSelectQuery() {

		StatementBuilder<Select> select = statementFactory.select(Query.empty(),
				converter.getMappingContext().getRequiredPersistentEntity(Group.class));

		assertThat(select.build(ParameterHandling.INLINE).getQuery()).isEqualTo("SELECT * FROM group");
	}

	@Test // DATACASS-708
	void selectShouldApplyQueryOptions() {

		QueryOptions queryOptions = QueryOptions.builder() //
				.executionProfile("foo") //
				.serialConsistencyLevel(DefaultConsistencyLevel.QUORUM) //
				.build();

		StatementBuilder<Select> select = statementFactory.select(Query.empty().queryOptions(queryOptions),
				converter.getMappingContext().getRequiredPersistentEntity(Group.class));

		SimpleStatement statement = select.build();
		assertThat(statement.getExecutionProfileName()).isEqualTo("foo");
		assertThat(statement.getSerialConsistencyLevel()).isEqualTo(DefaultConsistencyLevel.QUORUM);
	}

	@Test // DATACASS-343
	void shouldMapSelectQueryWithColumnsAndCriteria() {

		Query query = Query.query(Criteria.where("foo").is("bar")).columns(Columns.from("age"));

		StatementBuilder<Select> select = statementFactory.select(query, groupEntity);

		assertThat(select.build(ParameterHandling.INLINE).getQuery()).isEqualTo("SELECT age FROM group WHERE foo='bar'");
	}

	@Test // DATACASS-549
	void shouldMapSelectQueryNotEquals() {

		Query query = Query.query(Criteria.where("foo").ne("bar")).columns(Columns.from("age"));

		StatementBuilder<Select> select = statementFactory.select(query, groupEntity);

		assertThat(select.build(ParameterHandling.INLINE).getQuery()).isEqualTo("SELECT age FROM group WHERE foo!='bar'");
	}

	@Test // DATACASS-549
	void shouldMapSelectQueryIsNotNull() {

		Query query = Query.query(Criteria.where("foo").isNotNull()).columns(Columns.from("age"));

		StatementBuilder<Select> select = statementFactory.select(query, groupEntity);

		assertThat(select.build(ParameterHandling.INLINE).getQuery())
				.isEqualTo("SELECT age FROM group WHERE foo IS NOT NULL");
	}

	@Test // DATACASS-343
	void shouldMapSelectQueryWithTtlColumns() {

		Query query = Query.empty().columns(Columns.empty().ttl("email"));

		StatementBuilder<Select> select = statementFactory.select(query,
				converter.getMappingContext().getRequiredPersistentEntity(Group.class));

		assertThat(select.build(ParameterHandling.INLINE).getQuery()).isEqualTo("SELECT ttl(email) FROM group");
	}

	@Test // DATACASS-343
	void shouldMapSelectQueryWithSortLimitAndAllowFiltering() {

		Query query = Query.empty().sort(Sort.by("id.hashPrefix")).limit(10).withAllowFiltering();

		StatementBuilder<Select> select = statementFactory.select(query,
				converter.getMappingContext().getRequiredPersistentEntity(Group.class));

		assertThat(select.build(ParameterHandling.INLINE).getQuery())
				.isEqualTo("SELECT * FROM group ORDER BY hash_prefix ASC LIMIT 10 ALLOW FILTERING");
	}

	@Test // DATACASS-343
	void shouldMapDeleteQueryWithColumns() {

		Query query = Query.empty().columns(Columns.from("age"));

		StatementBuilder<Delete> delete = statementFactory.delete(query,
				converter.getMappingContext().getRequiredPersistentEntity(Group.class));

		assertThat(delete.build(ParameterHandling.INLINE).getQuery()).isEqualTo("DELETE age FROM group");
	}

	@Test // DATACASS-343
	void shouldMapDeleteQueryWithTimestampColumns() {

		DeleteOptions options = DeleteOptions.builder().timestamp(1234).build();
		Query query = Query.query(Criteria.where("foo").is("bar")).queryOptions(options);

		StatementBuilder<Delete> delete = statementFactory.delete(query,
				converter.getMappingContext().getRequiredPersistentEntity(Group.class));

		assertThat(delete.build(ParameterHandling.INLINE).getQuery())
				.isEqualTo("DELETE FROM group USING TIMESTAMP 1234 WHERE foo='bar'");
	}

	@Test // DATACASS-708
	void deleteShouldApplyQueryOptions() {

		Person person = new Person();
		person.id = "foo";

		QueryOptions queryOptions = QueryOptions.builder() //
				.executionProfile("foo") //
				.serialConsistencyLevel(DefaultConsistencyLevel.QUORUM) //
				.build();

		StatementBuilder<Delete> delete = statementFactory.delete(Query.empty().queryOptions(queryOptions),
				converter.getMappingContext().getRequiredPersistentEntity(Group.class));

		SimpleStatement statement = delete.build();
		assertThat(statement.getExecutionProfileName()).isEqualTo("foo");
		assertThat(statement.getSerialConsistencyLevel()).isEqualTo(DefaultConsistencyLevel.QUORUM);
	}

	@Test // DATACASS-656
	void shouldCreateInsert() {

		Person person = new Person();
		person.id = "foo";

		StatementBuilder<RegularInsert> insert = statementFactory.insert(person, WriteOptions.empty());

		assertThat(insert.build(ParameterHandling.INLINE).getQuery()).isEqualTo("INSERT INTO person (id) VALUES ('foo')");
	}

	@Test // DATACASS-708
	void insertShouldApplyQueryOptions() {

		Person person = new Person();
		person.id = "foo";

		WriteOptions queryOptions = WriteOptions.builder() //
				.executionProfile("foo") //
				.serialConsistencyLevel(DefaultConsistencyLevel.QUORUM) //
				.build();

		StatementBuilder<RegularInsert> insert = statementFactory.insert(person, queryOptions);

		SimpleStatement statement = insert.build();
		assertThat(statement.getExecutionProfileName()).isEqualTo("foo");
		assertThat(statement.getSerialConsistencyLevel()).isEqualTo(DefaultConsistencyLevel.QUORUM);
	}

	@Test // DATACASS-656
	void shouldCreateInsertIfNotExists() {

		InsertOptions options = InsertOptions.builder().withIfNotExists().build();
		Person person = new Person();
		person.id = "foo";

		StatementBuilder<RegularInsert> insert = statementFactory.insert(person, options);

		assertThat(insert.build(ParameterHandling.INLINE).getQuery())
				.isEqualTo("INSERT INTO person (id) VALUES ('foo') IF NOT EXISTS");
	}

	@Test // DATACASS-656
	void shouldCreateSetInsertNulls() {

		InsertOptions options = InsertOptions.builder().withInsertNulls().build();
		Person person = new Person();
		person.id = "foo";

		StatementBuilder<RegularInsert> insert = statementFactory.insert(person, options);

		assertThat(insert.build(ParameterHandling.INLINE).getQuery()).isEqualTo(
				"INSERT INTO person (first_name,id,list,map,number,set_col) VALUES (NULL,'foo',NULL,NULL,NULL,NULL)");
	}

	@Test // DATACASS-656
	void shouldCreateSetInsertWithTtl() {

		WriteOptions options = WriteOptions.builder().ttl(Duration.ofMinutes(1)).build();
		Person person = new Person();
		person.id = "foo";

		StatementBuilder<RegularInsert> insert = statementFactory.insert(person, options);

		assertThat(insert.build(ParameterHandling.INLINE).getQuery())
				.isEqualTo("INSERT INTO person (id) VALUES ('foo') USING TTL 60");
	}

	@Test // DATACASS-656
	void shouldCreateSetInsertWithTimestamp() {

		WriteOptions options = WriteOptions.builder().timestamp(1234).build();
		Person person = new Person();
		person.id = "foo";

		StatementBuilder<RegularInsert> insert = statementFactory.insert(person, options);

		assertThat(insert.build(ParameterHandling.INLINE).getQuery())
				.isEqualTo("INSERT INTO person (id) VALUES ('foo') USING TIMESTAMP 1234");
	}

	@Test // DATACASS-343
	void shouldCreateSetUpdate() {

		Query query = Query.query(Criteria.where("foo").is("bar"));

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> update = statementFactory.update(query,
				Update.empty().set("firstName", "baz").set("boo", "baa"), personEntity);

		assertThat(update.build(ParameterHandling.INLINE).getQuery())
				.isEqualTo("UPDATE person SET first_name='baz', boo='baa' WHERE foo='bar'");
	}

	@Test // DATACASS-656
	void shouldCreateSetUpdateWithTtl() {

		WriteOptions options = WriteOptions.builder().ttl(Duration.ofMinutes(1)).build();
		Query query = Query.query(Criteria.where("foo").is("bar")).queryOptions(options);

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> update = statementFactory.update(query,
				Update.empty().set("firstName", "baz"), personEntity);

		assertThat(update.build(ParameterHandling.INLINE).getQuery())
				.isEqualTo("UPDATE person USING TTL 60 SET first_name='baz' WHERE foo='bar'");
	}

	@Test // DATACASS-656
	void shouldCreateSetUpdateWithTimestamp() {

		WriteOptions options = WriteOptions.builder().timestamp(1234).build();
		Query query = Query.query(Criteria.where("foo").is("bar")).queryOptions(options);

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> update = statementFactory.update(query,
				Update.empty().set("firstName", "baz"), personEntity);

		assertThat(update.build(ParameterHandling.INLINE).getQuery())
				.isEqualTo("UPDATE person USING TIMESTAMP 1234 SET first_name='baz' WHERE foo='bar'");
	}

	@Test // DATACASS-343, DATACASS-712
	void shouldCreateSetAtIndexUpdate() {

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> update = statementFactory
				.update(Query.empty(), Update.empty().set("list").atIndex(10).to("Euro"), personEntity);

		assertThat(update.build(ParameterHandling.INLINE).getQuery()).isEqualTo("UPDATE person SET list[10]='Euro'");
	}

	@Test // DATACASS-343
	void shouldCreateSetAtKeyUpdate() {

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> update = statementFactory
				.update(Query.empty(), Update.empty().set("map").atKey("baz").to("Euro"), personEntity);

		assertThat(update.build(ParameterHandling.INLINE).getQuery()).isEqualTo("UPDATE person SET map['baz']='Euro'");
	}

	@Test // DATACASS-343
	void shouldAddToMap() {

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> update = statementFactory
				.update(Query.empty(), Update.empty().addTo("map").entry("foo", "Euro"), personEntity);

		assertThat(update.build(ParameterHandling.INLINE).getQuery()).isEqualTo("UPDATE person SET map=map+{'foo':'Euro'}");
	}

	@Test // DATACASS-343
	void shouldPrependAllToList() {

		Update update = Update.empty().addTo("list").prependAll("foo", "Euro");

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> updateStatementBuilder = statementFactory
				.update(Query.empty(), update, personEntity);

		assertThat(updateStatementBuilder.build(ParameterHandling.INLINE).getQuery())
				.isEqualTo("UPDATE person SET list=['foo','Euro']+list");
	}

	@Test // DATACASS-343
	void shouldAppendAllToList() {

		Update update = Update.empty().addTo("list").appendAll("foo", "Euro");

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> updateStatementBuilder = statementFactory
				.update(Query.empty(), update, personEntity);

		assertThat(updateStatementBuilder.build(ParameterHandling.INLINE).getQuery())
				.isEqualTo("UPDATE person SET list=list+['foo','Euro']");
	}

	@Test // DATACASS-343
	void shouldRemoveFromList() {

		Update update = Update.empty().remove("list", "Euro");

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> updateStatementBuilder = statementFactory
				.update(Query.empty(), update, personEntity);

		assertThat(updateStatementBuilder.build(ParameterHandling.INLINE).getQuery())
				.isEqualTo("UPDATE person SET list=list-['Euro']");
	}

	@Test // DATACASS-343
	void shouldClearList() {

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> update = statementFactory
				.update(Query.empty(), Update.empty().clear("list"), personEntity);

		assertThat(update.build(ParameterHandling.INLINE).getQuery()).isEqualTo("UPDATE person SET list=[]");
	}

	@Test // DATACASS-343
	void shouldAddAllToSet() {

		Update update = Update.empty().addTo("set").appendAll("foo", "Euro");

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> updateStatementBuilder = statementFactory
				.update(Query.empty(), update, personEntity);

		assertThat(updateStatementBuilder.build(ParameterHandling.INLINE).getQuery())
				.isEqualTo("UPDATE person SET set_col=set_col+{'foo','Euro'}");
	}

	@Test // DATACASS-343
	void shouldRemoveFromSet() {

		Update update = Update.empty().remove("set", "Euro");

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> updateStatementBuilder = statementFactory
				.update(Query.empty(), update, personEntity);

		assertThat(updateStatementBuilder.build(ParameterHandling.INLINE).getQuery())
				.isEqualTo("UPDATE person SET set_col=set_col-{'Euro'}");
	}

	@Test // DATACASS-343
	void shouldClearSet() {

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> update = statementFactory
				.update(Query.empty(), Update.empty().clear("set"), personEntity);

		assertThat(update.build(ParameterHandling.INLINE).getQuery()).isEqualTo("UPDATE person SET set_col={}");
	}

	@Test // DATACASS-343
	void shouldCreateIncrementUpdate() {

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> update = statementFactory
				.update(Query.empty(), Update.empty().increment("number"), personEntity);

		assertThat(update.build(ParameterHandling.INLINE).getQuery()).isEqualTo("UPDATE person SET number+=1");
	}

	@Test // DATACASS-735
	void shouldCreateIncrementLongUpdate() {

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> update = statementFactory
				.update(Query.empty(), Update.empty().increment("number", Long.MAX_VALUE), personEntity);

		assertThat(update.build(ParameterHandling.INLINE).getQuery())
				.isEqualTo("UPDATE person SET number+=" + Long.MAX_VALUE);
	}

	@Test // DATACASS-343
	void shouldCreateDecrementUpdate() {

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> update = statementFactory
				.update(Query.empty(), Update.empty().decrement("number"), personEntity);

		assertThat(update.build(ParameterHandling.INLINE).getQuery()).isEqualTo("UPDATE person SET number-=1");
	}

	@Test // DATACASS-735
	void shouldCreateDecrementLongUpdate() {

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> update = statementFactory
				.update(Query.empty(), Update.empty().decrement("number", Long.MAX_VALUE), personEntity);

		assertThat(update.build(ParameterHandling.INLINE).getQuery())
				.isEqualTo("UPDATE person SET number-=" + Long.MAX_VALUE);
	}

	@Test // DATACASS-569
	void shouldCreateSetUpdateIfExists() {

		Query query = Query.query(Criteria.where("foo").is("bar"))
				.queryOptions(UpdateOptions.builder().withIfExists().build());

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> update = statementFactory.update(query,
				Update.empty().set("firstName", "baz"), personEntity);

		assertThat(update.build(ParameterHandling.INLINE).getQuery())
				.isEqualTo("UPDATE person SET first_name='baz' WHERE foo='bar' IF EXISTS");
	}

	@Test // DATACASS-656
	void shouldCreateSetUpdateIfCondition() {

		Query query = Query.query(Criteria.where("foo").is("bar"))
				.queryOptions(UpdateOptions.builder().ifCondition(Criteria.where("foo").is("baz")).build());

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> update = statementFactory.update(query,
				Update.empty().set("firstName", "baz"), personEntity);

		assertThat(update.build(ParameterHandling.INLINE).getQuery())
				.isEqualTo("UPDATE person SET first_name='baz' WHERE foo='bar' IF foo='baz'");
	}

	@Test // DATACASS-708
	void updateShouldApplyQueryOptions() {

		UpdateOptions queryOptions = UpdateOptions.builder() //
				.executionProfile("foo") //
				.serialConsistencyLevel(DefaultConsistencyLevel.QUORUM) //
				.build();

		Query query = Query.query(Criteria.where("foo").is("bar")).queryOptions(queryOptions);

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> update = statementFactory.update(query,
				Update.empty().set("firstName", "baz"), personEntity);

		SimpleStatement statement = update.build();
		assertThat(statement.getExecutionProfileName()).isEqualTo("foo");
		assertThat(statement.getSerialConsistencyLevel()).isEqualTo(DefaultConsistencyLevel.QUORUM);
	}

	@Test // DATACASS-656
	void shouldCreateSetUpdateFromObject() {

		Person person = new Person();
		person.id = "foo";
		person.firstName = "bar";

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> update = statementFactory.update(person,
				WriteOptions.empty());

		assertThat(update.build(ParameterHandling.INLINE).getQuery())
				.isEqualTo("UPDATE person SET first_name='bar', list=NULL, map=NULL, number=NULL, set_col=NULL WHERE id='foo'");
	}

	@Test // DATACASS-656
	void shouldCreateSetUpdateFromObjectIfExists() {

		UpdateOptions options = UpdateOptions.builder().withIfExists().build();
		Person person = new Person();
		person.id = "foo";
		person.firstName = "bar";

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> update = statementFactory.update(person,
				options);

		assertThat(update.build(ParameterHandling.INLINE).getQuery()).endsWith("IF EXISTS");
	}

	@Test // DATACASS-656
	void shouldCreateSetUpdateFromObjectIfCondition() {

		UpdateOptions options = UpdateOptions.builder().ifCondition(Criteria.where("foo").is("bar")).build();
		Person person = new Person();
		person.id = "foo";
		person.firstName = "bar";

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> update = statementFactory.update(person,
				options);

		assertThat(update.build(ParameterHandling.INLINE).getQuery()).endsWith("IF foo='bar'");
	}

	@Test // DATACASS-656
	void shouldCreateSetUpdateFromObjectWithTtl() {

		WriteOptions options = WriteOptions.builder().ttl(Duration.ofMinutes(1)).build();
		Person person = new Person();
		person.id = "foo";

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> update = statementFactory.update(person,
				options);

		assertThat(update.build(ParameterHandling.INLINE).getQuery()).startsWith("UPDATE person USING TTL 60 SET");
	}

	@Test // DATACASS-656
	void shouldCreateSetUpdateFromObjectWithTimestamp() {

		WriteOptions options = WriteOptions.builder().timestamp(1234).build();
		Person person = new Person();
		person.id = "foo";

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> update = statementFactory.update(person,
				options);

		assertThat(update.build(ParameterHandling.INLINE).getQuery()).startsWith("UPDATE person USING TIMESTAMP 1234 SET");
	}

	@Test // DATACASS-656
	void shouldCreateSetUpdateFromObjectWithEmptyCollections() {

		Person person = new Person();
		person.id = "foo";
		person.set = Collections.emptySet();
		person.list = Collections.emptyList();

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> update = statementFactory.update(person,
				WriteOptions.empty());

		assertThat(update.build(ParameterHandling.INLINE).getQuery())
				.isEqualTo("UPDATE person SET first_name=NULL, list=[], map=NULL, number=NULL, set_col={} WHERE id='foo'");
	}

	@Test // DATACASS-708
	void updateObjectShouldApplyQueryOptions() {

		WriteOptions queryOptions = WriteOptions.builder() //
				.executionProfile("foo") //
				.serialConsistencyLevel(DefaultConsistencyLevel.QUORUM) //
				.build();

		Person person = new Person();
		person.id = "foo";
		person.set = Collections.emptySet();
		person.list = Collections.emptyList();

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> update = statementFactory.update(person,
				queryOptions);

		SimpleStatement statement = update.build();
		assertThat(statement.getExecutionProfileName()).isEqualTo("foo");
		assertThat(statement.getSerialConsistencyLevel()).isEqualTo(DefaultConsistencyLevel.QUORUM);
	}

	@Test // DATACASS-512
	void shouldCreateCountQuery() {

		Query query = Query.query(Criteria.where("foo").is("bar"));

		StatementBuilder<Select> count = statementFactory.count(query,
				converter.getMappingContext().getRequiredPersistentEntity(Group.class));

		assertThat(count.build(ParameterHandling.INLINE).getQuery())
				.isEqualTo("SELECT count(1) FROM group WHERE foo='bar'");
	}

	@SuppressWarnings("unused")
	static class Person {

		@Id private String id;

		Integer number;

		private List<String> list;

		Map<String, String> map;

		@Column("set_col") private Set<String> set;

		@Column("first_name") private String firstName;
	}
}
