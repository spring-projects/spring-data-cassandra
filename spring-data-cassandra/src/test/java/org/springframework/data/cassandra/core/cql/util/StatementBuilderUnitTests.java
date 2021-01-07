/*
 * Copyright 2019-2021 the original author or authors.
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
package org.springframework.data.cassandra.core.cql.util;

import static org.assertj.core.api.Assertions.*;

import java.util.Collections;

import org.junit.jupiter.api.Test;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;

/**
 * Unit tests for {@link StatementBuilder}.
 *
 * @author Mark Paluch
 */
class StatementBuilderUnitTests {

	@Test // DATACASS-656
	void shouldCreateSimpleStatement() {

		SimpleStatement statement = StatementBuilder.of(QueryBuilder.selectFrom("person").all()).build();

		assertThat(statement.getQuery()).isEqualTo("SELECT * FROM person");
	}

	@Test // DATACASS-656
	void shouldApplyBuilderFunction() {

		SimpleStatement statement = StatementBuilder.of(QueryBuilder.selectFrom("person").all())
				.apply(select -> select.orderBy("foo", ClusteringOrder.ASC)).build();

		assertThat(statement.getQuery()).isEqualTo("SELECT * FROM person ORDER BY foo ASC");
	}

	@Test // DATACASS-656
	void shouldApplyBindFunction() {

		SimpleStatement statement = StatementBuilder.of(QueryBuilder.selectFrom("person").all())
				.bind((select, factory) -> select.where(Relation.column("foo").isEqualTo(factory.create("bar"))))
				.build(StatementBuilder.ParameterHandling.INLINE);

		assertThat(statement.getQuery()).isEqualTo("SELECT * FROM person WHERE foo='bar'");
	}

	@Test // DATACASS-656
	void shouldBindByIndex() {

		SimpleStatement statement = StatementBuilder.of(QueryBuilder.selectFrom("person").all())
				.bind((select, factory) -> select.where(Relation.column("foo").isEqualTo(factory.create("bar"))))
				.build(StatementBuilder.ParameterHandling.BY_INDEX);

		assertThat(statement.getQuery()).isEqualTo("SELECT * FROM person WHERE foo=?");
		assertThat(statement.getPositionalValues()).containsOnly("bar");
	}

	@Test // DATACASS-656
	void shouldBindList() {

		SimpleStatement statement = StatementBuilder.of(QueryBuilder.selectFrom("person").all())
				.bind((select, factory) -> select
						.where(Relation.column("foo").isEqualTo(factory.create(Collections.singletonList("value")))))
				.build(StatementBuilder.ParameterHandling.INLINE);

		assertThat(statement.getQuery()).isEqualTo("SELECT * FROM person WHERE foo=['value']");
	}

	@Test // DATACASS-656
	void shouldBindSet() {

		SimpleStatement statement = StatementBuilder.of(QueryBuilder.selectFrom("person").all())
				.bind((select, factory) -> select
						.where(Relation.column("foo").isEqualTo(factory.create(Collections.singleton("value")))))
				.build(StatementBuilder.ParameterHandling.INLINE);

		assertThat(statement.getQuery()).isEqualTo("SELECT * FROM person WHERE foo={'value'}");
	}

	@Test // DATACASS-656
	void shouldBindMap() {

		SimpleStatement statement = StatementBuilder.of(QueryBuilder.selectFrom("person").all())
				.bind((select, factory) -> select
						.where(Relation.column("foo").isEqualTo(factory.create(Collections.singletonMap("key", "value")))))
				.build(StatementBuilder.ParameterHandling.INLINE);

		assertThat(statement.getQuery()).isEqualTo("SELECT * FROM person WHERE foo={'key':'value'}");
	}

	@Test // DATACASS-656
	void shouldBindByName() {

		SimpleStatement statement = StatementBuilder.of(QueryBuilder.selectFrom("person").all())
				.bind((select, factory) -> select.where(Relation.column("foo").isEqualTo(factory.create("bar"))))
				.build(StatementBuilder.ParameterHandling.BY_NAME);

		assertThat(statement.getQuery()).isEqualTo("SELECT * FROM person WHERE foo=:p0");
		assertThat(statement.getNamedValues()).containsEntry(CqlIdentifier.fromCql("p0"), "bar");
	}

	@Test // DATACASS-656
	void shouldApplyFunctionsInOrder() {

		SimpleStatement statement = StatementBuilder.of(QueryBuilder.selectFrom("person").all())
				.bind((select, factory) -> select.where(Relation.column("foo").isEqualTo(factory.create("bar"))))
				.apply(select -> select.orderBy("one", ClusteringOrder.ASC))
				.bind((select, factory) -> select.where(Relation.column("bar").isEqualTo(factory.create("baz"))))
				.apply(select -> select.orderBy("two", ClusteringOrder.ASC)).build(StatementBuilder.ParameterHandling.INLINE);

		assertThat(statement.getQuery())
				.isEqualTo("SELECT * FROM person WHERE foo='bar' AND bar='baz' ORDER BY one ASC,two ASC");
	}

	@Test // DATACASS-656
	void shouldNotifyOnBuild() {

		SimpleStatement statement = StatementBuilder.of(QueryBuilder.selectFrom("person").all())
				.onBuild(statementBuilder -> statementBuilder.addPositionalValue("foo"))
				.build(StatementBuilder.ParameterHandling.BY_NAME);

		assertThat(statement.getQuery()).isEqualTo("SELECT * FROM person");
		assertThat(statement.getPositionalValues()).hasSize(1);

	}

	@Test // DATACASS-708
	void shouldTransformBuiltStatement() {

		SimpleStatement statement = StatementBuilder.of(QueryBuilder.selectFrom("person").all())
				.transform(statement1 -> statement1.setExecutionProfileName("foo"))
				.build(StatementBuilder.ParameterHandling.BY_NAME);

		assertThat(statement.getQuery()).isEqualTo("SELECT * FROM person");
		assertThat(statement.getExecutionProfileName()).isEqualTo("foo");
	}
}
