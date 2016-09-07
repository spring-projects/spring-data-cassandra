/*
 * Copyright 2016 the original author or authors.
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
package org.springframework.data.cassandra.mapping;

import static org.assertj.core.api.Assertions.*;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.cassandra.core.cql.CqlIdentifier;
import org.springframework.data.util.ClassTypeInformation;

/**
 * Unit tests for {@link CassandraUserTypePersistentEntity}.
 * 
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class CassandraUserTypePersistentEntityUnitTests {

	@Mock CassandraMappingContext mappingContextMock;
	@Mock UserTypeResolver userTypeResolverMock;

	/**
	 * @see DATACASS-172
	 */
	@Test
	public void isUserDefinedTypeShouldReportTrue() {

		CassandraUserTypePersistentEntity<MappedUdt> type = getEntity(MappedUdt.class);

		assertThat(type.isUserDefinedType()).isTrue();
	}

	/**
	 * @see DATACASS-172
	 */
	@Test
	public void getTableNameShouldReturnDefaultName() {

		CassandraUserTypePersistentEntity<MappedUdt> type = getEntity(MappedUdt.class);

		assertThat(type.getTableName()).isEqualTo(CqlIdentifier.cqlId("mappedudt"));
		assertThat(type.getTableName()).isEqualTo(CqlIdentifier.cqlId("Mappedudt"));
	}

	/**
	 * @see DATACASS-172
	 */
	@Test
	public void getTableNameShouldReturnDefinedName() {

		CassandraUserTypePersistentEntity<WithName> type = getEntity(WithName.class);

		assertThat(type.getTableName()).isEqualTo(CqlIdentifier.cqlId("withname"));
		assertThat(type.getTableName()).isEqualTo(CqlIdentifier.cqlId("Withname"));
	}

	/**
	 * @see DATACASS-172
	 */
	@Test
	public void getTableNameShouldReturnDefinedNameUsingForceQuote() {

		CassandraUserTypePersistentEntity<WithForceQuote> type = getEntity(WithForceQuote.class);

		assertThat(type.getTableName()).isNotEqualTo(CqlIdentifier.cqlId("upperCase", true));
		assertThat(type.getTableName()).isEqualTo(CqlIdentifier.cqlId("UpperCase", true));
	}

	private <T> CassandraUserTypePersistentEntity<T> getEntity(Class<T> entityClass) {
		return new CassandraUserTypePersistentEntity<T>(ClassTypeInformation.from(entityClass), mappingContextMock, null,
				userTypeResolverMock);
	}

	@UserDefinedType
	static class MappedUdt {}

	@UserDefinedType("withname")
	static class WithName {}

	@UserDefinedType(value = "UpperCase", forceQuote = true)
	static class WithForceQuote {}
}
