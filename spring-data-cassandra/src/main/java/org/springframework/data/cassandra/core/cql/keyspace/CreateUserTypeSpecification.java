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
package org.springframework.data.cassandra.core.cql.keyspace;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * Object to configure a {@code CREATE TYPE} specification.
 *
 * @author Fabio J. Mendes
 * @author Mark Paluch
 * @since 1.5
 * @see CqlIdentifier
 */
public class CreateUserTypeSpecification extends UserTypeSpecification<CreateUserTypeSpecification> {

	private boolean ifNotExists;

	private CreateUserTypeSpecification(CqlIdentifier name) {
		super(name);
	}

	/**
	 * Entry point into the {@link CreateUserTypeSpecification}'s fluent API given {@code name} to create a type.
	 * Convenient if imported statically.
	 *
	 * @param name must not {@literal null} or empty.
	 * @return a new {@link CreateUserTypeSpecification}.
	 */
	public static CreateUserTypeSpecification createType(String name) {
		return new CreateUserTypeSpecification(CqlIdentifier.fromCql(name));
	}

	/**
	 * Entry point into the {@link CreateUserTypeSpecification}'s fluent API given {@code name} to create a type.
	 * Convenient if imported statically.
	 *
	 * @param name must not {@literal null}.
	 * @return a new {@link CreateUserTypeSpecification}.
	 */
	public static CreateUserTypeSpecification createType(CqlIdentifier name) {
		return new CreateUserTypeSpecification(name);
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
