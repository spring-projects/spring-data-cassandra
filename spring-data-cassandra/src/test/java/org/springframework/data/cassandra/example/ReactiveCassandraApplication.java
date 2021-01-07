/*
 * Copyright 2020-2021 the original author or authors.
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
// tag::file[]
package org.springframework.data.cassandra.example;

import reactor.core.publisher.Mono;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.data.cassandra.core.ReactiveCassandraOperations;
import org.springframework.data.cassandra.core.ReactiveCassandraTemplate;
import org.springframework.data.cassandra.core.cql.session.DefaultBridgedReactiveSession;
import org.springframework.data.cassandra.core.query.Criteria;
import org.springframework.data.cassandra.core.query.Query;

import com.datastax.oss.driver.api.core.CqlSession;

public class ReactiveCassandraApplication {

	private static final Logger LOGGER = LoggerFactory.getLogger(ReactiveCassandraApplication.class);

	private static Person newPerson(String name, int age) {
		return new Person(UUID.randomUUID().toString(), name, age);
	}

	public static void main(String[] args) {

		CqlSession cqlSession = CqlSession.builder().withKeyspace("mykeyspace").build();

		 ReactiveCassandraOperations template = new ReactiveCassandraTemplate(new DefaultBridgedReactiveSession(cqlSession));

    Mono<Person> jonDoe = template.insert(newPerson("Jon Doe", 40));

    jonDoe.flatMap(it -> template.selectOne(Query.query(Criteria.where("id").is(it.getId())), Person.class))
        .doOnNext(it -> LOGGER.info(it.toString()))
        .then(template.truncate(Person.class))
        .block();

		cqlSession.close();
	}

}
// end::file[]
