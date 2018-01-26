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

import static org.assertj.core.api.Assertions.*;

import lombok.Data;

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.cql.CqlIdentifier;
import org.springframework.data.cassandra.core.mapping.Indexed;
import org.springframework.data.cassandra.core.mapping.Table;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTest;

/**
 * Integration tests for {@link ExecutableInsertOperationSupport}.
 *
 * @author Mark Paluch
 */
public class ExecutableInsertOperationSupportTests extends AbstractKeyspaceCreatingIntegrationTest {

	CassandraAdminTemplate template;

	Person han;
	Person luke;

	@Before
	public void setUp() {

		template = new CassandraAdminTemplate(session, new MappingCassandraConverter());
		template.dropTable(true, CqlIdentifier.of("person"));
		template.createTable(true, CqlIdentifier.of("person"), Person.class, Collections.emptyMap());

		initPersons();
	}

	@Test(expected = IllegalArgumentException.class) // DATACASS-485
	public void domainTypeIsRequired() {
		template.insert((Class) null);
	}

	@Test(expected = IllegalArgumentException.class) // DATACASS-485
	public void tableIsRequiredOnSet() {
		template.insert(Person.class).inTable((String) null);
	}

	@Test(expected = IllegalArgumentException.class) // DATACASS-485
	public void optionsIsRequiredOnSet() {
		template.insert(Person.class).withOptions(null);
	}

	@Test // DATACASS-485
	public void insertOne() {

		WriteResult writeResult = template.insert(Person.class).inTable("person").one(han);

		assertThat(writeResult.wasApplied()).isTrue();
		assertThat(template.selectOneById(han.id, Person.class)).isEqualTo(han);
	}

	@Test // DATACASS-485
	public void insertOneWithOptions() {

		template.insert(Person.class).inTable("person").one(han);

		WriteResult writeResult = template.insert(Person.class).inTable("person")
				.withOptions(InsertOptions.builder().withIfNotExists().build()).one(han);

		assertThat(writeResult.wasApplied()).isFalse();
		assertThat(template.selectOneById(han.id, Person.class)).isEqualTo(han);
	}

	@Data
	@Table
	static class Person {

		@Id String id;
		@Indexed String firstname;
		@Indexed String lastname;
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
}
