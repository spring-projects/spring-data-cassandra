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

import java.util.Collections;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.Indexed;
import org.springframework.data.cassandra.core.mapping.Table;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTests;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * Integration tests for {@link ExecutableUpdateOperationSupport}.
 *
 * @author Mark Paluch
 */
class ExecutableUpdateOperationSupportIntegrationTests extends AbstractKeyspaceCreatingIntegrationTests {

	private CassandraAdminTemplate template;

	private Person han;
	private Person luke;

	@BeforeEach
	void setUp() {

		template = new CassandraAdminTemplate(session, new MappingCassandraConverter());
		template.dropTable(true, CqlIdentifier.fromCql("person"));
		template.createTable(false, CqlIdentifier.fromCql("person"), Person.class, Collections.emptyMap());

		han = new Person();
		han.firstname = "han";
		han.id = "id-1";

		luke = new Person();
		luke.firstname = "luke";
		luke.id = "id-2";

		template.insert(han);
		template.insert(luke);
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

		WriteResult updateResult = this.template.update(Person.class).matching(queryHan())
				.apply(update("firstname", "Han"));

		assertThat(updateResult).isNotNull();
		assertThat(updateResult.wasApplied()).isTrue();
		assertThat(this.template.selectOne(queryLuke(), Person.class)).isEqualTo(luke);
	}

	@Test // DATACASS-485
	void updateWithDifferentDomainClassAndCollection() {

		WriteResult updateResult = this.template.update(Jedi.class).inTable("person")
				.matching(query(where("id").is(han.getId()))).apply(update("name", "Han"));

		assertThat(updateResult).isNotNull();
		assertThat(updateResult.wasApplied()).isTrue();
		assertThat(this.template.selectOne(queryHan(), Person.class)).isNotEqualTo(han)
				.hasFieldOrPropertyWithValue("firstname", "Han");
	}

	private Query queryHan() {
		return queryPerson(han);
	}

	private Query queryLuke() {
		return queryPerson(luke);
	}

	private Query queryPerson(Person person) {
		return query(where("id").is(person.getId()));
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
