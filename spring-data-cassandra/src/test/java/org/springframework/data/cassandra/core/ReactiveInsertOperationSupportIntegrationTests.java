/*
 * Copyright 2018 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;

import lombok.Data;

import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import org.junit.Before;
import org.junit.Test;

import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.cql.CqlIdentifier;
import org.springframework.data.cassandra.core.cql.session.DefaultBridgedReactiveSession;
import org.springframework.data.cassandra.core.mapping.Indexed;
import org.springframework.data.cassandra.core.mapping.Table;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTest;

/**
 * Integration tests for {@link ReactiveInsertOperationSupport}.
 *
 * @author Mark Paluch
 */
public class ReactiveInsertOperationSupportIntegrationTests extends AbstractKeyspaceCreatingIntegrationTest {

	CassandraAdminTemplate admin;

	ReactiveCassandraTemplate template;

	Person han;
	Person luke;

	@Before
	public void setUp() {

		admin = new CassandraAdminTemplate(session, new MappingCassandraConverter());
		template = new ReactiveCassandraTemplate(new DefaultBridgedReactiveSession(session));

		admin.dropTable(true, CqlIdentifier.of("person"));
		admin.createTable(true, CqlIdentifier.of("person"), Person.class, Collections.emptyMap());

		initPersons();
	}


	private void initPersons() {

		han = new Person();
		han.firstname = "han";
		han.lastname = "solo";
		han.id = "id-1";

		luke = new Person();
		luke.firstname = "luke";
		luke.lastname = "skywalker";
		luke.id = "id-2";
	}

	@Test(expected = IllegalArgumentException.class) // DATACASS-485
	public void domainTypeIsRequired() {
		this.template.insert((Class) null);
	}

	@Test(expected = IllegalArgumentException.class) // DATACASS-485
	public void optionsIsRequiredOnSet() {
		this.template.insert(Person.class).withOptions(null);
	}

	@Test(expected = IllegalArgumentException.class) // DATACASS-485
	public void tableIsRequiredOnSet() {
		this.template.insert(Person.class).inTable((String) null);
	}

	@Test // DATACASS-485
	public void insertOne() {

		Mono<WriteResult> writeResult = this.template.insert(Person.class).inTable("person").one(han);

		StepVerifier.create(writeResult.map(WriteResult::wasApplied)).expectNext(true).verifyComplete();
		StepVerifier.create(template.selectOneById(han.id, Person.class)).expectNext(han).verifyComplete();
	}

	@Test // DATACASS-485
	public void insertOneWithOptions() {

		this.template.insert(Person.class).inTable("person").one(han);

		Mono<WriteResult> writeResult = this.template
				.insert(Person.class).inTable("person")
				.withOptions(InsertOptions.builder().withIfNotExists().build())
				.one(han);

		StepVerifier.create(writeResult).assertNext(it -> assertThat(it.wasApplied()).isTrue()).verifyComplete();
		StepVerifier.create(template.selectOneById(han.id, Person.class)).expectNext(han).verifyComplete();
	}

	@Data
	@Table
	static class Person {
		@Id String id;
		@Indexed String firstname;
		@Indexed String lastname;
	}
}
