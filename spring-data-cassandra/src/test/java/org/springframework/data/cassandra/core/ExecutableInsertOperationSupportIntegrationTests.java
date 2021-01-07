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

import java.util.Collections;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.mapping.Indexed;
import org.springframework.data.cassandra.core.mapping.Table;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTests;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * Integration tests for {@link ExecutableInsertOperationSupport}.
 *
 * @author Mark Paluch
 */
class ExecutableInsertOperationSupportIntegrationTests extends AbstractKeyspaceCreatingIntegrationTests {

	private CassandraAdminTemplate template;

	private Person han;
	private Person luke;

	@BeforeEach
	void setUp() {

		template = new CassandraAdminTemplate(session, new MappingCassandraConverter());
		template.dropTable(true, CqlIdentifier.fromCql("person"));
		template.createTable(true, CqlIdentifier.fromCql("person"), Person.class, Collections.emptyMap());

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
	void tableIsRequiredOnSet() {
		assertThatIllegalArgumentException().isThrownBy(() -> this.template.insert(Person.class).inTable((String) null));
	}

	@Test // DATACASS-485
	void optionsIsRequiredOnSet() {
		assertThatIllegalArgumentException().isThrownBy(() -> this.template.insert(Person.class).withOptions(null));
	}

	@Test // DATACASS-485
	void insertOne() {

		WriteResult insertResult = this.template.insert(Person.class).inTable("person").one(han);

		assertThat(insertResult.wasApplied()).isTrue();
		assertThat(this.template.selectOneById(han.id, Person.class)).isEqualTo(han);
	}

	@Test // DATACASS-485
	void insertOneWithOptions() {

		this.template.insert(Person.class).inTable("person").one(han);

		WriteResult insertResult = this.template.insert(Person.class).inTable("person")
				.withOptions(InsertOptions.builder().withIfNotExists().build()).one(han);

		assertThat(insertResult.wasApplied()).isFalse();
		assertThat(template.selectOneById(han.id, Person.class)).isEqualTo(han);
	}

	@Data
	@Table
	static class Person {
		@Id String id;
		@Indexed String firstname;
		@Indexed String lastname;
	}
}
