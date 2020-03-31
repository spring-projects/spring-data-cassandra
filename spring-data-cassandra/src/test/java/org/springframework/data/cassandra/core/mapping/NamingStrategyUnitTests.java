/*
 * Copyright 2020 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;

import org.springframework.data.annotation.Id;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * Unit tests for {@link NamingStrategy}.
 *
 * @author Mark Paluch
 */
public class NamingStrategyUnitTests {

	CassandraMappingContext context = new CassandraMappingContext();

	@Before
	public void before() {
		context.setUserTypeResolver(typeName -> {
			throw new IllegalStateException("");
		});
		context.setNamingStrategy(NamingStrategy.INSTANCE);
	}

	@Test // DATACASS-84
	public void shouldDeriveTableName() {

		BasicCassandraPersistentEntity<?> entity = context.getRequiredPersistentEntity(PersonTable.class);

		assertThat(entity.getTableName()).isEqualTo(CqlIdentifier.fromCql("persontable"));
	}

	@Test // DATACASS-84
	public void shouldDeriveUserDefinedTypeName() {

		BasicCassandraPersistentEntity<?> entity = context.getRequiredPersistentEntity(MyUserType.class);

		assertThat(entity.getTableName()).isEqualTo(CqlIdentifier.fromCql("myusertype"));
	}

	@Test // DATACASS-84
	public void shouldDeriveColumnName() {

		BasicCassandraPersistentEntity<?> entity = context.getRequiredPersistentEntity(PersonTable.class);

		assertThat(entity.getRequiredIdProperty().getColumnName()).isEqualTo(CqlIdentifier.fromCql("firstname"));
	}

	@Test // DATACASS-84
	public void shouldDeriveCaseSensitiveTableName() {

		BasicCassandraPersistentEntity<?> entity = context.getRequiredPersistentEntity(QuotedPersonTable.class);

		assertThat(entity.getTableName()).isEqualTo(CqlIdentifier.fromInternal("QuotedPersonTable"));
	}

	@Test // DATACASS-84
	public void shouldDeriveCaseSensitiveUserDefinedTypeName() {

		BasicCassandraPersistentEntity<?> entity = context.getRequiredPersistentEntity(QuotedMyUserType.class);

		assertThat(entity.getTableName()).isEqualTo(CqlIdentifier.fromInternal("QuotedMyUserType"));
	}

	@Test // DATACASS-84
	public void shouldDeriveCaseSensitiveColumnName() {

		BasicCassandraPersistentEntity<?> entity = context.getRequiredPersistentEntity(QuotedPersonTable.class);

		assertThat(entity.getRequiredIdProperty().getColumnName()).isEqualTo(CqlIdentifier.fromInternal("firstName"));
	}

	@Test // DATACASS-84
	public void shouldApplyTransformedNamingStrategy() {

		context.setNamingStrategy(NamingStrategy.INSTANCE.transform(String::toUpperCase));

		BasicCassandraPersistentEntity<?> entity = context.getRequiredPersistentEntity(QuotedPersonTable.class);

		assertThat(entity.getTableName()).isEqualTo(CqlIdentifier.fromInternal("QUOTEDPERSONTABLE"));
		assertThat(entity.getRequiredIdProperty().getColumnName()).isEqualTo(CqlIdentifier.fromInternal("FIRSTNAME"));
	}

	static class PersonTable {

		@Id String firstName;
	}

	@UserDefinedType
	static class MyUserType {

		String firstName;
	}

	@Table(forceQuote = true)
	static class QuotedPersonTable {

		@Id @PrimaryKey(forceQuote = true) String firstName;
	}

	@UserDefinedType(forceQuote = true)
	static class QuotedMyUserType {

		String firstName;
	}
}
