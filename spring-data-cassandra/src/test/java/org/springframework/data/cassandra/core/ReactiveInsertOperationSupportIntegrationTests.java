/*
 * Copyright 2018-2021 the original author or authors.
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

import lombok.Data;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Collections;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.cql.session.DefaultBridgedReactiveSession;
import org.springframework.data.cassandra.core.mapping.Indexed;
import org.springframework.data.cassandra.core.mapping.Table;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTests;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * Integration tests for {@link ReactiveInsertOperationSupport}.
 *
 * @author Mark Paluch
 */
class ReactiveInsertOperationSupportIntegrationTests extends AbstractKeyspaceCreatingIntegrationTests {

	private CassandraAdminTemplate admin;

	private ReactiveCassandraTemplate template;

	private Person han;
	private Person luke;

	@BeforeEach
	void setUp() {

		admin = new CassandraAdminTemplate(session, new MappingCassandraConverter());
		template = new ReactiveCassandraTemplate(new DefaultBridgedReactiveSession(session));

		admin.dropTable(true, CqlIdentifier.fromCql("person"));
		admin.createTable(true, CqlIdentifier.fromCql("person"), Person.class, Collections.emptyMap());

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

	@Test // DATACASS-485
	void domainTypeIsRequired() {
		assertThatIllegalArgumentException().isThrownBy(() -> this.template.insert((Class) null));
	}

	@Test // DATACASS-485
	void optionsIsRequiredOnSet() {
		assertThatIllegalArgumentException().isThrownBy(() -> this.template.insert(Person.class).withOptions(null));
	}

	@Test // DATACASS-485
	void tableIsRequiredOnSet() {
		assertThatIllegalArgumentException().isThrownBy(() -> this.template.insert(Person.class).inTable((String) null));
	}

	@Test // DATACASS-485, DATACASS-573
	void insertOne() {

		Mono<EntityWriteResult<Person>> writeResult = this.template.insert(Person.class).inTable("person").one(han);

		writeResult.as(StepVerifier::create).consumeNextWith(actual -> {

			assertThat(actual.wasApplied()).isTrue();
			assertThat(actual.getEntity()).isSameAs(han);
		}).verifyComplete();

		template.selectOneById(han.id, Person.class).as(StepVerifier::create).expectNext(han).verifyComplete();
	}

	@Test // DATACASS-485, DATACASS-573
	void insertOneWithOptions() {

		this.template.insert(Person.class).inTable("person").one(han);

		Mono<EntityWriteResult<Person>> writeResult = this.template.insert(Person.class).inTable("person")
				.withOptions(InsertOptions.builder().withIfNotExists().build()).one(han);

		writeResult.as(StepVerifier::create).assertNext(it -> assertThat(it.wasApplied()).isTrue()).verifyComplete();
		template.selectOneById(han.id, Person.class).as(StepVerifier::create).expectNext(han).verifyComplete();
	}

	@Data
	@Table
	static class Person {
		@Id String id;
		@Indexed String firstname;
		@Indexed String lastname;
	}
}
