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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.junit.jupiter.api.Test;

import org.springframework.data.cassandra.core.cql.ReactiveCqlTemplate;
import org.springframework.data.cassandra.core.cql.RowMapper;

import com.datastax.oss.driver.api.core.cql.Row;

/**
 * @author Mark Paluch
 */
//@formatter:off
public class ReactiveCqlTemplateExamples {

	private ReactiveCqlTemplate reactiveCqlTemplate = null;

	void examples() {
		// tag::rowCount[]
		Mono<Integer> rowCount = reactiveCqlTemplate.queryForObject("SELECT COUNT(*) FROM t_actor", Integer.class);
		// end::rowCount[]

		// tag::countOfActorsNamedJoe[]
		Mono<Integer> countOfActorsNamedJoe = reactiveCqlTemplate.queryForObject(
			"SELECT COUNT(*) FROM t_actor WHERE first_name = ?", Integer.class, "Joe");
		// end::countOfActorsNamedJoe[]

		// tag::lastName[]
		Mono<String> lastName = reactiveCqlTemplate.queryForObject(
			"SELECT last_name FROM t_actor WHERE id = ?",
			String.class, 1212L);
		// end::lastName[]

		// tag::rowMapper[]
		Mono<Actor> actor = reactiveCqlTemplate.queryForObject(
			"SELECT first_name, last_name FROM t_actor WHERE id = ?",
			new RowMapper<Actor>() {
				public Actor mapRow(Row row, int rowNum) {
					Actor actor = new Actor();
					actor.setFirstName(row.getString("first_name"));
					actor.setLastName(row.getString("last_name"));
					return actor;
				}},
			1212L);
		// end::rowMapper[]

		// tag::listOfRowMapper[]
		Flux<Actor> actors = reactiveCqlTemplate.query(
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
	}

	// tag::findAllActors[]
	Flux<Actor> findAllActors() {
		return reactiveCqlTemplate.query("SELECT first_name, last_name FROM t_actor", ActorMapper.INSTANCE);
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
	void insert() {

		// tag::insert[]
		Mono<Boolean> applied = reactiveCqlTemplate.execute(
			"INSERT INTO t_actor (first_name, last_name) VALUES (?, ?)",
			"Leonor", "Watling");
		// end::insert[]
	}

	@Test
	void update() {
		// tag::update[]
		Mono<Boolean> applied = reactiveCqlTemplate.execute(
			"UPDATE t_actor SET last_name = ? WHERE id = ?",
			"Banjo", 5276L);
		// end::update[]
	}

	@Test
	void delete() {
		long actorId = 1;

		// tag::delete[]
		Mono<Boolean> applied = reactiveCqlTemplate.execute(
			"DELETE FROM actor WHERE id = ?",
			actorId);
		// end::delete[]
	}

	static class Actor {

		void setFirstName(String first_name) {

		}

		void setLastName(String last_name) {}
	}
}
