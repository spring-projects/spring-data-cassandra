/*
 * Copyright 2016-present the original author or authors.
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
package org.springframework.data.cassandra.core.cql.keyspace;

import org.springframework.lang.Nullable;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * Object to configure a {@code CREATE TYPE} specification.
 *
 * @author Fabio J. Mendes
 * @author Mark Paluch
 * @since 1.5
 * @see CqlIdentifier
 */
public class CreateUserTypeSpecification extends UserTypeSpecification<CreateUserTypeSpecification>
		implements CqlSpecification {

	private boolean ifNotExists;

	private CreateUserTypeSpecification(@Nullable CqlIdentifier keyspace, CqlIdentifier name) {
		super(keyspace, name);
	}

	/**
	 * Entry point into the {@link CreateUserTypeSpecification}'s fluent API given {@code typeName} to create a type.
	 * Convenient if imported statically.
	 *
	 * @param typeName must not {@literal null} or empty.
	 * @return a new {@link CreateUserTypeSpecification}.
	 */
	public static CreateUserTypeSpecification createType(String typeName) {
		return new CreateUserTypeSpecification(null, CqlIdentifier.fromCql(typeName));
	}

	/**
	 * Entry point into the {@link CreateUserTypeSpecification}'s fluent API given {@code typeName} to create a type.
	 * Convenient if imported statically.
	 *
	 * @param typeName must not {@literal null}.
	 * @return a new {@link CreateUserTypeSpecification}.
	 */
	public static CreateUserTypeSpecification createType(CqlIdentifier typeName) {
		return new CreateUserTypeSpecification(null, typeName);
	}

	/**
	 * Entry point into the {@link CreateUserTypeSpecification}'s fluent API given {@code typeName} to create a type.
	 * Convenient if imported statically. Uses the default keyspace if {@code keyspace} is null; otherwise, of the
	 * {@code keyspace} is not {@link null}, then the UDT name is prefixed with {@code keyspace}.
	 *
	 * @param keyspace can be {@literal null}.
	 * @param typeName must not {@literal null}.
	 * @return a new {@link CreateUserTypeSpecification}.
	 * @since 4.4
	 */
	public static CreateUserTypeSpecification createType(@Nullable CqlIdentifier keyspace, CqlIdentifier typeName) {
		return new CreateUserTypeSpecification(keyspace, typeName);
	}

	/**
	 * Enables the inclusion of an{@code IF NOT EXISTS} clause.
	 *
	 * @return this {@link CreateUserTypeSpecification}.
	 */
	public CreateUserTypeSpecification ifNotExists() {
		return ifNotExists(true);
	}

	/**
	 * Sets the inclusion of an {@code IF NOT EXISTS} clause.
	 *
	 * @param ifNotExists {@literal true} to include an {@code IF NOT EXISTS} clause, {@literal false} to omit the
	 *          {@code IF NOT EXISTS} clause.
	 * @return this {@link CreateUserTypeSpecification}.
	 */
	public CreateUserTypeSpecification ifNotExists(boolean ifNotExists) {

		this.ifNotExists = ifNotExists;

		return this;
	}

	/**
	 * @return {@literal true} if the {@code IF NOT EXISTS} clause is included.
	 */
	public boolean getIfNotExists() {
		return ifNotExists;
	}
}
