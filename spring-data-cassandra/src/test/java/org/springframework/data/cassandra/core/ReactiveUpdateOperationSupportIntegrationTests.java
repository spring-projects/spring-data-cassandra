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
import static org.springframework.data.cassandra.core.query.Criteria.*;
import static org.springframework.data.cassandra.core.query.Query.*;
import static org.springframework.data.cassandra.core.query.Update.*;

import lombok.Data;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Collections;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.cql.session.DefaultBridgedReactiveSession;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.Indexed;
import org.springframework.data.cassandra.core.mapping.Table;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTests;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * Integration tests for {@link ReactiveUpdateOperationSupport}.
 *
 * @author Mark Paluch
 */
class ReactiveUpdateOperationSupportIntegrationTests extends AbstractKeyspaceCreatingIntegrationTests {

	private CassandraAdminTemplate admin;

	private ReactiveCassandraTemplate template;

	private Person han;
	private Person luke;

	@BeforeEach
	void setUp() {

		admin = new CassandraAdminTemplate(session, new MappingCassandraConverter());
		template = new ReactiveCassandraTemplate(new DefaultBridgedReactiveSession(session));

		admin.dropTable(true, CqlIdentifier.fromCql("person"));
		admin.createTable(false, CqlIdentifier.fromCql("person"), Person.class, Collections.emptyMap());

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
	void domainTypeIsRequired() {
		assertThatIllegalArgumentException().isThrownBy(() -> this.template.update(null));
	}

	@Test // DATACASS-485
	void queryIsRequired() {
		assertThatIllegalArgumentException().isThrownBy(() -> this.template.update(Person.class).matching(null));
	}

	@Test // DATACASS-485
	void tableIsRequiredOnSet() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> this.template.update(Person.class).inTable((CqlIdentifier) null));
	}

	@Test // DATACASS-485
	void updateAllMatching() {

		Mono<WriteResult> writeResult = this.template.update(Person.class).matching(queryHan())
				.apply(update("firstname", "Han"));

		writeResult.map(WriteResult::wasApplied).as(StepVerifier::create).expectNext(true).verifyComplete();
	}

	@Test // DATACASS-485
	void updateWithDifferentDomainClassAndCollection() {

		Mono<WriteResult> writeResult = this.template.update(Jedi.class).inTable("person")
				.matching(query(where("id").is(han.getId()))).apply(update("name", "Han"));

		writeResult.map(WriteResult::wasApplied).as(StepVerifier::create).expectNext(true).verifyComplete();

		assertThat(this.admin.selectOne(queryHan(), Person.class)).isNotEqualTo(han)
				.hasFieldOrPropertyWithValue("firstname", "Han");
	}

	private Query queryHan() {
		return query(where("id").is(han.getId()));
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
