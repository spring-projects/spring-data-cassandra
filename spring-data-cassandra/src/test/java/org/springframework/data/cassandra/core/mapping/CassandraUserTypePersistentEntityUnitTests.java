/*
 * Copyright 2016-2021 the original author or authors.
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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.core.annotation.AliasFor;
import org.springframework.data.util.ClassTypeInformation;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * Unit tests for {@link CassandraUserTypePersistentEntity}.
 *
 * @author Mark Paluch
 */
@ExtendWith(MockitoExtension.class)
class CassandraUserTypePersistentEntityUnitTests {

	@Test // DATACASS-172
	void isUserDefinedTypeShouldReportTrue() {

		CassandraUserTypePersistentEntity<MappedUdt> type = getEntity(MappedUdt.class);

		assertThat(type.isUserDefinedType()).isTrue();
	}

	@Test // DATACASS-172
	void getTableNameShouldReturnDefaultName() {

		CassandraUserTypePersistentEntity<MappedUdt> type = getEntity(MappedUdt.class);

		assertThat(type.getTableName()).isEqualTo(CqlIdentifier.fromCql("mappedudt"));
	}

	@Test // DATACASS-172
	void getTableNameShouldReturnDefinedName() {

		CassandraUserTypePersistentEntity<WithName> type = getEntity(WithName.class);

		assertThat(type.getTableName()).isEqualTo(CqlIdentifier.fromCql("withname"));
	}

	@Test // DATACASS-172
	void getTableNameShouldReturnDefinedNameUsingForceQuote() {

		CassandraUserTypePersistentEntity<WithForceQuote> type = getEntity(WithForceQuote.class);

		assertThat(type.getTableName()).isEqualTo(CqlIdentifier.fromInternal("UpperCase"));
	}

	@Test // DATACASS-259
	void shouldConsiderComposedUserDefinedTypeAnnotation() {

		CassandraUserTypePersistentEntity<TypeWithComposedAnnotation> type = getEntity(TypeWithComposedAnnotation.class);

		assertThat(type.getTableName()).isEqualTo(CqlIdentifier.fromCql("mytype"));
	}

	private <T> CassandraUserTypePersistentEntity<T> getEntity(Class<T> entityClass) {
		return new CassandraUserTypePersistentEntity<>(ClassTypeInformation.from(entityClass), null);
	}

	@UserDefinedType
	private static class MappedUdt {}

	@UserDefinedType("withname")
	private static class WithName {}

	@UserDefinedType(value = "UpperCase", forceQuote = true)
	private static class WithForceQuote {}

	@Retention(RetentionPolicy.RUNTIME)
	@Target({ ElementType.TYPE })
	@UserDefinedType(forceQuote = true)
	private @interface ComposedUserDefinedTypeAnnotation {

		@AliasFor(annotation = UserDefinedType.class)
		String value() default "mytype";
	}

	@ComposedUserDefinedTypeAnnotation()
	private static class TypeWithComposedAnnotation {}
}
