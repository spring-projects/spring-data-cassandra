/*
 * Copyright 2020-2025 the original author or authors.
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
package org.springframework.data.cassandra.core.mapping;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;

import org.springframework.data.annotation.Id;
import org.springframework.data.core.TypeInformation;
import org.springframework.util.StringUtils;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * Unit tests for {@link NamingStrategy}.
 *
 * @author Mark Paluch
 */
class NamingStrategyUnitTests {

	private CassandraMappingContext context = new CassandraMappingContext();

	@Test
	void getTableNameGeneratesTableName() {

		BasicCassandraPersistentEntity<TableNameHolderThingy> entity = new BasicCassandraPersistentEntity<>(
				TypeInformation.of(TableNameHolderThingy.class));

		assertThat(entity.getTableName().asCql(true)).isEqualTo("tablenameholderthingy");
	}

	@Test
	void getTableNameGeneratesQuotedTableName() {

		BasicCassandraPersistentEntity<TableNameHolderThingy> entity = new BasicCassandraPersistentEntity<>(
				TypeInformation.of(TableNameHolderThingy.class));
		entity.setNamingStrategy(NamingStrategy.SNAKE_CASE.transform(it -> "\"" + StringUtils.capitalize(it) + "\""));

		assertThat(entity.getTableName().asCql(true)).isEqualTo("\"Table_name_holder_thingy\"");

		entity = new BasicCassandraPersistentEntity<>(TypeInformation.of(TableNameHolderThingy.class));
		entity.setNamingStrategy(NamingStrategy.SNAKE_CASE.transform(String::toUpperCase));

		assertThat(entity.getTableName().asCql(true)).isEqualTo("\"TABLE_NAME_HOLDER_THINGY\"");
	}

	@Test
	void atTableIsCaseSensitive() {

		BasicCassandraPersistentEntity<ProvidedTableName> entity = new BasicCassandraPersistentEntity<>(
				TypeInformation.of(ProvidedTableName.class));

		assertThat(entity.getTableName().asCql(true)).isEqualTo("\"iAmProvided\"");
	}

	@Test
	void atTableWithQuotedNameShouldRetainQuotes() {

		BasicCassandraPersistentEntity<QuotedTableName> entity = new BasicCassandraPersistentEntity<>(
				TypeInformation.of(QuotedTableName.class));

		assertThat(entity.getTableName().asCql(true)).isEqualTo("\"IAmQuoted\"");
	}

	@Test // DATACASS-84
	void shouldDeriveTableName() {

		BasicCassandraPersistentEntity<?> entity = context.getRequiredPersistentEntity(PersonTable.class);

		assertThat(entity.getTableName()).isEqualTo(CqlIdentifier.fromCql("persontable"));
	}

	@Test // DATACASS-84
	void shouldDeriveUserDefinedTypeName() {

		BasicCassandraPersistentEntity<?> entity = context.getRequiredPersistentEntity(MyUserType.class);

		assertThat(entity.getTableName()).isEqualTo(CqlIdentifier.fromCql("myusertype"));
	}

	@Test // DATACASS-84
	void shouldDeriveColumnName() {

		BasicCassandraPersistentEntity<?> entity = context.getRequiredPersistentEntity(PersonTable.class);

		assertThat(entity.getRequiredIdProperty().getColumnName()).isEqualTo(CqlIdentifier.fromCql("firstname"));
	}

	private static class PersonTable {

		@Id String firstName;
	}

	@Table("messages")
	static class Message {}

	static class TableNameHolderThingy {}

	@Table("iAmProvided")
	private static class ProvidedTableName {}

	@Table("\"IAmQuoted\"")
	private static class QuotedTableName {}

	@UserDefinedType
	private static class MyUserType {

		String firstName;
	}

}
