/*
 * Copyright 2017-2025 the original author or authors.
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
import static org.springframework.data.cassandra.core.query.Criteria.*;
import static org.springframework.data.domain.Sort.Direction.*;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.convert.UpdateMapper;
import org.springframework.data.cassandra.core.cql.QueryOptions;
import org.springframework.data.cassandra.core.cql.WriteOptions;
import org.springframework.data.cassandra.core.cql.util.StatementBuilder;
import org.springframework.data.cassandra.core.cql.util.StatementBuilder.ParameterHandling;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.VectorType;
import org.springframework.data.cassandra.core.query.Columns;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.cassandra.core.query.Update;
import org.springframework.data.cassandra.core.query.VectorSort;
import org.springframework.data.cassandra.domain.Group;
import org.springframework.data.domain.Limit;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Vector;
import org.springframework.data.projection.EntityProjection;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.data.CqlVector;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.internal.core.type.codec.registry.DefaultCodecRegistry;

/**
 * Unit tests for {@link StatementFactory}.
 *
 * @author Mark Paluch
 * @author Sam Lightfoot
 * @author Jeongjun Min
 */
class StatementFactoryUnitTests {

	private MappingCassandraConverter converter = new MappingCassandraConverter();

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

	@Test // GH-1590
	void shouldMapExistsQuery() {

		StatementBuilder<Select> select = statementFactory.selectExists(Query.empty(),
				EntityProjection.nonProjecting(Group.class),
				converter.getMappingContext().getRequiredPersistentEntity(Group.class), CqlIdentifier.fromCql("group"));

		assertThat(select.build(ParameterHandling.INLINE).getQuery())
				.isEqualTo("SELECT groupname,hash_prefix,username FROM group LIMIT 1");
	}

	@Test // GH-1590
	void shouldRenderMappedFields() {

		statementFactory.setProjectionFunction(StatementFactory.ProjectionFunction.mappedProperties());

		StatementBuilder<Select> select = statementFactory.select(Query.empty(),
				converter.getMappingContext().getRequiredPersistentEntity(Group.class));

		assertThat(select.build(ParameterHandling.INLINE).getQuery())
				.isEqualTo("SELECT groupname,hash_prefix,username,email,age FROM group");
	}

	@Test // GH-1275
	void shouldConsiderKeyspaceForSelect() {

		statementFactory.setKeyspaceProvider((entity, tableName) -> CqlIdentifier.fromCql("ks_" + tableName));

		StatementBuilder<Select> select = statementFactory.select(Query.empty(),
				converter.getMappingContext().getRequiredPersistentEntity(Group.class));

		assertThat(select.build(ParameterHandling.INLINE).getQuery()).isEqualTo("SELECT * FROM ks_group.group");
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

		Query query = Query.query(where("foo").is("bar")).columns(Columns.from("age"));

		StatementBuilder<Select> select = statementFactory.select(query, groupEntity);

		assertThat(select.build(ParameterHandling.INLINE).getQuery()).isEqualTo("SELECT age FROM group WHERE foo='bar'");
	}

	@Test // DATACASS-549
	void shouldMapSelectQueryNotEquals() {

		Query query = Query.query(where("foo").ne("bar")).columns(Columns.from("age"));

		StatementBuilder<Select> select = statementFactory.select(query, groupEntity);

		assertThat(select.build(ParameterHandling.INLINE).getQuery()).isEqualTo("SELECT age FROM group WHERE foo!='bar'");
	}

	@Test // DATACASS-549
	void shouldMapSelectQueryIsNotNull() {

		Query query = Query.query(where("foo").isNotNull()).columns(Columns.from("age"));

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

	@Test // GH-1008
	void shouldMapSelectQueryWithSort() {

		Query query = Query.empty().sort(Sort.by(DESC, "email", "age"));

		StatementBuilder<Select> select = statementFactory.select(query,
				converter.getMappingContext().getRequiredPersistentEntity(Group.class));

		assertThat(select.build(ParameterHandling.INLINE).getQuery())
				.isEqualTo("SELECT * FROM group ORDER BY email DESC,age DESC");
	}

	@Test // GH-1401
	void shouldMapSelectQueryWithLimit() {

		Query query = Query.query(where("email").is("e@mail")).limit(Limit.of(10));

		StatementBuilder<Select> select = statementFactory.select(query,
				converter.getMappingContext().getRequiredPersistentEntity(Group.class));

		assertThat(select.build(ParameterHandling.INLINE).getQuery())
				.isEqualTo("SELECT * FROM group WHERE email='e@mail' LIMIT 10");

		SimpleStatement statement = select.build(ParameterHandling.BY_INDEX);
		assertThat(statement.getQuery()).isEqualTo("SELECT * FROM group WHERE email=? LIMIT ?");
		assertThat(statement.getPositionalValues()).containsExactly("e@mail", 10);
	}

	@Test // DATACASS-343
	void shouldMapSelectQueryWithSortByEmbeddedLimitAndAllowFiltering() {

		Query query = Query.empty().sort(Sort.by(DESC, "id.hashPrefix", "id.username"));

		StatementBuilder<Select> select = statementFactory.select(query,
				converter.getMappingContext().getRequiredPersistentEntity(Group.class));

		assertThat(select.build(ParameterHandling.INLINE).getQuery())
				.isEqualTo("SELECT * FROM group ORDER BY hash_prefix DESC,username DESC");
	}

	@Test // DATACASS-343
	void shouldMapSelectQueryWithLimitAndAllowFiltering() {

		Query query = Query.empty().limit(10).withAllowFiltering();

		StatementBuilder<Select> select = statementFactory.select(query,
				converter.getMappingContext().getRequiredPersistentEntity(Group.class));

		assertThat(select.build(ParameterHandling.INLINE).getQuery())
				.isEqualTo("SELECT * FROM group LIMIT 10 ALLOW FILTERING");
	}

	@Test // GH-1172
	void shouldMapSelectInQueryAsInlineValue() {

		StatementBuilder<Select> select = statementFactory.select(Query.query(where("foo").in("bar")), groupEntity);

		assertThat(select.build(ParameterHandling.INLINE).getQuery()).isEqualTo("SELECT * FROM group WHERE foo IN ('bar')");

		select = statementFactory.select(Query.query(where("foo").in("bar", "baz")), groupEntity);

		assertThat(select.build(ParameterHandling.INLINE).getQuery())
				.isEqualTo("SELECT * FROM group WHERE foo IN ('bar','baz')");
	}

	@Test // GH-1172
	void shouldMapSelectInQueryAsByIndexValue() {

		StatementBuilder<Select> select = statementFactory.select(Query.query(where("foo").in("bar")), groupEntity);
		SimpleStatement statement = select.build(ParameterHandling.BY_INDEX);

		assertThat(statement.getQuery()).isEqualTo("SELECT * FROM group WHERE foo IN ?");
		assertThat(statement.getPositionalValues()).containsOnly(Collections.singletonList("bar"));

		select = statementFactory.select(Query.query(where("foo").in("bar", "baz")), groupEntity);
		statement = select.build(ParameterHandling.BY_INDEX);

		assertThat(statement.getQuery()).isEqualTo("SELECT * FROM group WHERE foo IN ?");
		assertThat(statement.getPositionalValues()).containsOnly(Arrays.asList("bar", "baz"));
	}

	@Test // GH-1172
	void shouldMapSelectInQueryAsByNamedValue() {

		StatementBuilder<Select> select = statementFactory.select(Query.query(where("foo").in("bar")), groupEntity);
		SimpleStatement statement = select.build(ParameterHandling.BY_NAME);

		assertThat(statement.getQuery()).isEqualTo("SELECT * FROM group WHERE foo IN :p0");
		assertThat(statement.getNamedValues()).hasSize(1).containsEntry(CqlIdentifier.fromCql("p0"),
				Collections.singletonList("bar"));

		select = statementFactory.select(Query.query(where("foo").in("bar", "baz")), groupEntity);
		statement = select.build(ParameterHandling.BY_NAME);

		assertThat(statement.getQuery()).isEqualTo("SELECT * FROM group WHERE foo IN :p0");
		assertThat(statement.getNamedValues()).hasSize(1).containsEntry(CqlIdentifier.fromCql("p0"),
				(Arrays.asList("bar", "baz")));
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
		Query query = Query.query(where("foo").is("bar")).queryOptions(options);

		StatementBuilder<Delete> delete = statementFactory.delete(query,
				converter.getMappingContext().getRequiredPersistentEntity(Group.class));

		assertThat(delete.build(ParameterHandling.INLINE).getQuery())
				.isEqualTo("DELETE FROM group USING TIMESTAMP 1234 WHERE foo='bar'");
	}

	@Test // GH-1401
	void deleteByQueryWithOptionsShouldRenderBindMarkers() {

		DeleteOptions options = DeleteOptions.builder().timestamp(1234).build();
		Query query = Query.query(where("foo").is("bar")).queryOptions(options);

		StatementBuilder<Delete> delete = statementFactory.delete(query,
				converter.getMappingContext().getRequiredPersistentEntity(Group.class));

		SimpleStatement statement = delete.build(ParameterHandling.BY_INDEX);

		assertThat(statement.getQuery()).isEqualTo("DELETE FROM group USING TIMESTAMP ? WHERE foo=?");
		assertThat(statement.getPositionalValues()).containsExactly(1234L, "bar");
	}

	@Test // GH-1401
	void deleteByEntityWithOptionsShouldRenderBindMarkers() {

		Person person = new Person();
		person.id = "foo";

		DeleteOptions options = DeleteOptions.builder().timestamp(1234).build();

		StatementBuilder<Delete> delete = statementFactory.delete(person, options, converter,
				CqlIdentifier.fromCql("person"));

		SimpleStatement statement = delete.build(ParameterHandling.BY_INDEX);

		assertThat(statement.getQuery()).isEqualTo("DELETE FROM person USING TIMESTAMP ? WHERE id=?");
		assertThat(statement.getPositionalValues()).containsExactly(1234L, "foo");
	}

	@Test // GH-1275
	void shouldConsiderKeyspaceForDeleteByEntity() {

		statementFactory.setKeyspaceProvider((entity, tableName) -> CqlIdentifier.fromCql("ks_" + tableName));

		Person person = new Person();
		person.id = "foo";

		StatementBuilder<Delete> delete = statementFactory.delete(person, DeleteOptions.empty(), converter,
				CqlIdentifier.fromCql("person"));

		SimpleStatement statement = delete.build(ParameterHandling.BY_INDEX);

		assertThat(statement.getQuery()).isEqualTo("DELETE FROM ks_person.person WHERE id=?");
	}

	@Test // GH-1275
	void shouldConsiderKeyspaceForDelete() {

		statementFactory.setKeyspaceProvider((entity, tableName) -> CqlIdentifier.fromCql("ks_" + tableName));
		StatementBuilder<Delete> delete = statementFactory.delete(Query.query(where("foo").is("bar")), groupEntity);

		SimpleStatement statement = delete.build(ParameterHandling.BY_INDEX);

		assertThat(statement.getQuery()).isEqualTo("DELETE FROM ks_group.group WHERE foo=?");
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

	@Test // GH-1275
	void shouldConsiderKeyspaceForInsert() {

		statementFactory.setKeyspaceProvider((entity, tableName) -> CqlIdentifier.fromCql("ks_" + tableName));

		Person person = new Person();
		person.id = "foo";

		StatementBuilder<RegularInsert> insert = statementFactory.insert(person, WriteOptions.empty());

		assertThat(insert.build(ParameterHandling.INLINE).getQuery())
				.isEqualTo("INSERT INTO ks_person.person (id) VALUES ('foo')");
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
		assertThat(statement.getQuery()).isEqualTo("INSERT INTO person (id) VALUES (?)");
		assertThat(statement.getExecutionProfileName()).isEqualTo("foo");
		assertThat(statement.getSerialConsistencyLevel()).isEqualTo(DefaultConsistencyLevel.QUORUM);
	}

	@Test // GH-1401, GH-1535
	void insertWithOptionsShouldRenderBindMarkers() {

		Person person = new Person();
		person.id = "foo";

		WriteOptions queryOptions = WriteOptions.builder() //
				.ttl(10).timestamp(1234).build();

		StatementBuilder<RegularInsert> insert = statementFactory.insert(person, queryOptions);

		SimpleStatement statement = insert.build(ParameterHandling.BY_INDEX);

		assertThat(statement.getQuery()).isEqualTo("INSERT INTO person (id) VALUES (?) USING TIMESTAMP ? AND TTL ?");
		assertThat(statement.getPositionalValues()).containsExactly("foo", 1234L, 10);

		queryOptions = WriteOptions.builder() //
				.ttl(Duration.ZERO).timestamp(1234).build();

		insert = statementFactory.insert(person, queryOptions);

		statement = insert.build(ParameterHandling.BY_INDEX);

		assertThat(statement.getQuery()).isEqualTo("INSERT INTO person (id) VALUES (?) USING TIMESTAMP ? AND TTL ?");
		assertThat(statement.getPositionalValues()).containsExactly("foo", 1234L, 0);
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
				"INSERT INTO person (id,number,list,map,set_col,first_name) VALUES ('foo',NULL,NULL,NULL,NULL,NULL)");
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

		Query query = Query.query(where("foo").is("bar"));

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> update = statementFactory.update(query,
				Update.empty().set("firstName", "baz").set("boo", "baa"), personEntity);

		assertThat(update.build(ParameterHandling.INLINE).getQuery())
				.isEqualTo("UPDATE person SET first_name='baz', boo='baa' WHERE foo='bar'");
	}

	@Test // GH-1275
	void shouldConsiderKeyspaceForCreateSetUpdate() {

		statementFactory.setKeyspaceProvider((entity, tableName) -> CqlIdentifier.fromCql("ks_" + tableName));

		Query query = Query.query(where("foo").is("bar"));

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> update = statementFactory.update(query,
				Update.empty().set("firstName", "baz").set("boo", "baa"), personEntity);

		assertThat(update.build(ParameterHandling.INLINE).getQuery())
				.isEqualTo("UPDATE ks_person.person SET first_name='baz', boo='baa' WHERE foo='bar'");
	}

	@Test // DATACASS-656
	void shouldCreateSetUpdateWithTtl() {

		WriteOptions options = WriteOptions.builder().ttl(Duration.ofMinutes(1)).build();
		Query query = Query.query(where("foo").is("bar")).queryOptions(options);

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> update = statementFactory.update(query,
				Update.empty().set("firstName", "baz"), personEntity);

		assertThat(update.build(ParameterHandling.INLINE).getQuery())
				.isEqualTo("UPDATE person USING TTL 60 SET first_name='baz' WHERE foo='bar'");
	}

	@Test // DATACASS-656
	void shouldCreateSetUpdateWithTimestamp() {

		WriteOptions options = WriteOptions.builder().timestamp(1234).build();
		Query query = Query.query(where("foo").is("bar")).queryOptions(options);

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> update = statementFactory.update(query,
				Update.empty().set("firstName", "baz"), personEntity);

		assertThat(update.build(ParameterHandling.INLINE).getQuery())
				.isEqualTo("UPDATE person USING TIMESTAMP 1234 SET first_name='baz' WHERE foo='bar'");
	}

	@Test // GH-1401
	void updateWithOptionsShouldRenderBindMarker() {

		WriteOptions options = WriteOptions.builder().ttl(Duration.ofMinutes(1)).timestamp(1234).build();
		Query query = Query.query(where("foo").is("bar")).queryOptions(options);

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> update = statementFactory.update(query,
				Update.empty().set("firstName", "baz"), personEntity);

		SimpleStatement statement = update.build(ParameterHandling.BY_INDEX);
		assertThat(statement.getQuery())
				.isEqualTo("UPDATE person USING TIMESTAMP ? AND TTL ? SET first_name=? WHERE foo=?");

		assertThat(statement.getPositionalValues()).containsExactly(1234L, 60, "baz", "bar");
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

	@Test // #1007
	void shouldRemoveFromMap() {

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> update = statementFactory
				.update(Query.empty(), Update.empty().removeFrom("map").value("foo"), personEntity);

		assertThat(update.build(ParameterHandling.INLINE).getQuery()).isEqualTo("UPDATE person SET map=map-{'foo'}");

		update = statementFactory.update(Query.empty(), Update.empty().removeFrom("map").values("foo", "bar"),
				personEntity);

		assertThat(update.build(ParameterHandling.INLINE).getQuery()).isEqualTo("UPDATE person SET map=map-{'foo','bar'}");
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

		assertThat(update.build(ParameterHandling.INLINE).getQuery()).isEqualTo("UPDATE person SET number=number+1");
	}

	@Test // DATACASS-735
	void shouldCreateIncrementLongUpdate() {

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> update = statementFactory
				.update(Query.empty(), Update.empty().increment("number", Long.MAX_VALUE), personEntity);

		assertThat(update.build(ParameterHandling.INLINE).getQuery())
				.isEqualTo("UPDATE person SET number=number+" + Long.MAX_VALUE);
	}

	@Test // DATACASS-343
	void shouldCreateDecrementUpdate() {

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> update = statementFactory
				.update(Query.empty(), Update.empty().decrement("number"), personEntity);

		assertThat(update.build(ParameterHandling.INLINE).getQuery()).isEqualTo("UPDATE person SET number=number-1");
	}

	@Test // DATACASS-735
	void shouldCreateDecrementLongUpdate() {

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> update = statementFactory
				.update(Query.empty(), Update.empty().decrement("number", Long.MAX_VALUE), personEntity);

		assertThat(update.build(ParameterHandling.INLINE).getQuery())
				.isEqualTo("UPDATE person SET number=number-" + Long.MAX_VALUE);
	}

	@Test // DATACASS-569
	void shouldCreateSetUpdateIfExists() {

		Query query = Query.query(where("foo").is("bar")).queryOptions(UpdateOptions.builder().withIfExists().build());

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> update = statementFactory.update(query,
				Update.empty().set("firstName", "baz"), personEntity);

		assertThat(update.build(ParameterHandling.INLINE).getQuery())
				.isEqualTo("UPDATE person SET first_name='baz' WHERE foo='bar' IF EXISTS");
	}

	@Test // DATACASS-656
	void shouldCreateSetUpdateIfCondition() {

		Query query = Query.query(where("foo").is("bar"))
				.queryOptions(UpdateOptions.builder().ifCondition(where("foo").is("baz")).build());

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

		Query query = Query.query(where("foo").is("bar")).queryOptions(queryOptions);

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
				.isEqualTo("UPDATE person SET number=NULL, list=NULL, map=NULL, set_col=NULL, first_name='bar' WHERE id='foo'");
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

		UpdateOptions options = UpdateOptions.builder().ifCondition(where("foo").is("bar")).build();
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
				.isEqualTo("UPDATE person SET number=NULL, list=[], map=NULL, set_col={}, first_name=NULL WHERE id='foo'");
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

		Query query = Query.query(where("foo").is("bar"));

		StatementBuilder<Select> count = statementFactory.count(query,
				converter.getMappingContext().getRequiredPersistentEntity(Group.class));

		assertThat(count.build(ParameterHandling.INLINE).getQuery())
				.isEqualTo("SELECT count(1) FROM group WHERE foo='bar'");
	}

	@Test // GH-1114
	void shouldConsiderCodecRegistry() {

		DefaultCodecRegistry cr = new DefaultCodecRegistry("foo");
		cr.register(new TypeCodec<MyString>() {
			@Override
			public GenericType<MyString> getJavaType() {
				return GenericType.of(MyString.class);
			}

			@Override
			public DataType getCqlType() {
				return DataTypes.TEXT;
			}

			@Override
			public ByteBuffer encode(MyString value, ProtocolVersion protocolVersion) {
				return null;
			}

			@Override
			public MyString decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
				return null;
			}

			@Override
			public String format(MyString value) {
				return "'" + value.value() + "'";
			}

			@Override
			public MyString parse(String value) {
				return new MyString(value);
			}
		});

		converter.setCodecRegistry(cr);

		Query query = Query.query(where("foo").is(new MyString("bar")));

		StatementBuilder<Select> count = statementFactory.count(query,
				converter.getMappingContext().getRequiredPersistentEntity(Group.class));

		assertThat(count.build(ParameterHandling.INLINE).getQuery())
				.isEqualTo("SELECT count(1) FROM group WHERE foo='bar'");
	}

	@Test // GH-1504
	void shouldUpdateCqlVector() {

		WithVector withVector = new WithVector();
		withVector.id = "foo";
		withVector.vector = CqlVector.newInstance(1.2f, 1.3f);
		withVector.array = new float[] { 2.2f, 2.3f };

		SimpleStatement statement = statementFactory.update(withVector, WriteOptions.empty())
				.build(ParameterHandling.BY_NAME);

		assertThat(statement.getQuery()).isEqualTo("UPDATE withvector SET vector=:p0, array=:p1 WHERE id=:p2");

		assertThat(statement.getNamedValues().get(CqlIdentifier.fromCql("p0"))).isInstanceOf(CqlVector.class).hasToString("[1.2, 1.3]");
		assertThat(statement.getNamedValues().get(CqlIdentifier.fromCql("p1"))).isInstanceOf(CqlVector.class).hasToString("[2.2, 2.3]");
	}

	@Test // GH-1504
	void shouldQueryVector() {

		Query query = Query.empty()
				.sort(VectorSort.ann("vector", Vector.of(1.2f, 1.3f)));

		SimpleStatement statement = statementFactory.select(query, converter.getMappingContext().getRequiredPersistentEntity(WithVector.class))
				.build(ParameterHandling.BY_NAME);

		assertThat(statement.getQuery()).isEqualTo("SELECT * FROM withvector ORDER BY vector ANN OF [1.2, 1.3]");
		assertThat(statement.getNamedValues()).isEmpty();
	}

	@Test // GH-1504
	void shouldRenderSimilaritySelector() {

		Vector vector = Vector.of(0.2f, 0.15f, 0.3f, 0.2f, 0.05f);

		Columns columns = Columns.empty().include("comment").select("vector", it -> it.similarity(vector).cosine());

		Query query = Query.select(columns).sort(VectorSort.ann("vector", Vector.of(1.2f, 1.3f)));

		SimpleStatement statement = statementFactory
				.select(query, converter.getMappingContext().getRequiredPersistentEntity(WithVector.class))
				.build(ParameterHandling.BY_NAME);

		assertThat(statement.getQuery()).isEqualTo(
				"SELECT comment,similarity_cosine(vector,[0.2, 0.15, 0.3, 0.2, 0.05]) AS vector FROM withvector ORDER BY vector ANN OF [1.2, 1.3]");
		assertThat(statement.getNamedValues()).isEmpty();
	}

	@Test // GH-1525
	void shouldCreateUpdateWithMultipleOperationsOnSameColumnDifferentKeys() {

		Update update = Update.empty().set("map").atKey("key1").to("value1").set("map").atKey("key2").to("value2");

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> updateStatementBuilder = statementFactory
			.update(Query.empty(), update, personEntity);

		String cql = updateStatementBuilder.build(ParameterHandling.INLINE).getQuery();

		assertThat(cql).isEqualTo("UPDATE person SET map['key1']='value1', map['key2']='value2'");
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

	record MyString(String value) {

	}

	static class WithVector {

		@Id String id;

		@VectorType(dimensions = 12) CqlVector<Number> vector;
		@VectorType(dimensions = 12) float[] array;
	}
}
