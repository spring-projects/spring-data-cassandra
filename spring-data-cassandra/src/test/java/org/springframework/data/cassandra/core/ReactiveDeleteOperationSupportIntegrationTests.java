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

import static org.springframework.data.cassandra.core.query.Criteria.where;
import static org.springframework.data.cassandra.core.query.Query.query;

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
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.Indexed;
import org.springframework.data.cassandra.core.mapping.Table;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTest;

/**
 * Integration tests for {@link ExecutableDeleteOperationSupport}.
 *
 * @author Mark Paluch
 */
public class ReactiveDeleteOperationSupportIntegrationTests extends AbstractKeyspaceCreatingIntegrationTest {

	CassandraAdminTemplate admin;

	ReactiveCassandraTemplate template;

	Person han;
	Person luke;

	@Before
	public void setUp() {

		admin = new CassandraAdminTemplate(session, new MappingCassandraConverter());
		template = new ReactiveCassandraTemplate(new DefaultBridgedReactiveSession(session));

		admin.dropTable(true, CqlIdentifier.of("person"));
		admin.createTable(true, CqlIdentifier.of("person"), ExecutableInsertOperationSupportIntegrationTests.Person.class,
				Collections.emptyMap());

		han = new Person();
		han.firstname = "han";
		han.id = "id-1";

		luke = new Person();
		luke.firstname = "luke";
		luke.id = "id-2";

		admin.insert(han);
		admin.insert(luke);
	}

	@Test // DATACASS-485
	public void removeAllMatching() {

		Mono<WriteResult> writeResult = this.template
				.delete(Person.class)
				.matching(query(where("id").is(han.id)))
				.all();

		StepVerifier.create(writeResult.map(WriteResult::wasApplied)).expectNext(true).verifyComplete();
	}

	@Test // DATACASS-485
	public void removeAllMatchingWithAlternateDomainTypeAndCollection() {

		Mono<WriteResult> writeResult = this.template
				.delete(Jedi.class).inTable("person")
				.matching(query(where("id").in(han.id, luke.id)))
				.all();

		StepVerifier.create(writeResult.map(WriteResult::wasApplied)).expectNext(true).verifyComplete();
		StepVerifier.create(template.select(Query.empty(), Person.class)).verifyComplete();
	}

	@Data
	@Table
	static class Person {
		@Id String id;
		@Indexed String firstname;
	}

	@Data
	static class Jedi {
		@Column("firstname") String name;
	}
}
