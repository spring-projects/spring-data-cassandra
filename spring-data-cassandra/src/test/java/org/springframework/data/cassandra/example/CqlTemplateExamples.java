/*
 * Copyright 2020-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https:://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.example;

import java.util.List;

import org.junit.jupiter.api.Test;

import org.springframework.data.cassandra.core.cql.CqlTemplate;
import org.springframework.data.cassandra.core.cql.RowMapper;
import org.springframework.data.cassandra.core.cql.generator.DropTableCqlGenerator;
import org.springframework.data.cassandra.core.cql.keyspace.DropTableSpecification;

import com.datastax.oss.driver.api.core.cql.Row;

/**
 * @author Mark Paluch
 */
//@formatter:off
public class CqlTemplateExamples {

	private CqlTemplate cqlTemplate = null;

	void examples() {
		// tag::rowCount[]
		int rowCount = cqlTemplate.queryForObject("SELECT COUNT(*) FROM t_actor", Integer.class);
		// end::rowCount[]

		// tag::countOfActorsNamedJoe[]
		int countOfActorsNamedJoe = cqlTemplate.queryForObject(
				"SELECT COUNT(*) FROM t_actor WHERE first_name = ?", Integer.class, "Joe");
		// end::countOfActorsNamedJoe[]

		// tag::lastName[]
		String lastName = cqlTemplate.queryForObject(
				"SELECT last_name FROM t_actor WHERE id = ?",
				String.class, 1212L);
		// end::lastName[]

		// tag::rowMapper[]
		Actor actor = cqlTemplate.queryForObject("SELECT first_name, last_name FROM t_actor WHERE id = ?",
				new RowMapper<Actor>() {
					public Actor mapRow(Row row, int rowNum) {
						Actor actor = new Actor();
						actor.setFirstName(row.getString("first_name"));
						actor.setLastName(row.getString("last_name"));
						return actor;
					}
				}, 1212L);
		// end::rowMapper[]

		// tag::listOfRowMapper[]
		List<Actor> actors = cqlTemplate.query(
				"SELECT first_name, last_name FROM t_actor",
				new RowMapper<Actor>() {
					public Actor mapRow(Row row, int rowNum) {
						Actor actor = new Actor();
						actor.setFirstName(row.getString("first_name"));
						actor.setLastName(row.getString("last_name"));
						return actor;
					}
				});
		// end::listOfRowMapper[]

		// tag::preparedStatement[]
		List<String> lastNames = cqlTemplate.query(
				session -> session.prepare("SELECT last_name FROM t_actor WHERE id = ?"),
				ps -> ps.bind(1212L),
				(row, rowNum) -> row.getString(0));
		// end::preparedStatement[]
	}

	// tag::findAllActors[]
	List<Actor> findAllActors() {
		return cqlTemplate.query("SELECT first_name, last_name FROM t_actor", ActorMapper.INSTANCE);
	}

	enum ActorMapper implements RowMapper<Actor> {

		INSTANCE;

		public Actor mapRow(Row row, int rowNum) {
			Actor actor = new Actor();
			actor.setFirstName(row.getString("first_name"));
			actor.setLastName(row.getString("last_name"));
			return actor;
		}
	}
	// end::findAllActors[]

	@Test
	void prepared() {
		long actorId = 1;

		// tag::insert[]
		cqlTemplate.execute(
				"INSERT INTO t_actor (first_name, last_name) VALUES (?, ?)",
				"Leonor", "Watling");
		// end::insert[]

		// tag::update[]
		cqlTemplate.execute(
				"UPDATE t_actor SET last_name = ? WHERE id = ?",
				"Banjo", 5276L);
		// end::update[]

		// tag::delete[]
		cqlTemplate.execute(
				"DELETE FROM t_actor WHERE id = ?",
				5276L);
		// end::delete[]
	}

	@Test
	void other() {
		// tag::other[]
		cqlTemplate.execute("CREATE TABLE test_table (id uuid primary key, event text)");

		DropTableSpecification dropper = DropTableSpecification.dropTable("test_table");
		String cql = DropTableCqlGenerator.toCql(dropper);

		cqlTemplate.execute(cql);
		// end::other[]
	}

	static class Actor {

		void setFirstName(String first_name) {

		}

		void setLastName(String last_name) {}
	}
}
