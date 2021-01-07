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
 * Object to configure a {@code DROP TYPE} specification.
 *
 * @author Fabio J. Mendes
 * @author Mark Paluch
 * @since 1.5
 * @see CqlIdentifier
 */
public class DropUserTypeSpecification extends UserTypeNameSpecification {

	private boolean ifExists;

	private DropUserTypeSpecification(CqlIdentifier name) {
		super(name);
	}

	/**
	 * Entry point into the {@link DropUserTypeSpecification}'s fluent API given {@code name} to drop a type. Convenient
	 * if imported statically.
	 *
	 * @param name must not be {@code null} or empty.
	 * @return a new {@link DropUserTypeSpecification}.
	 */
	public static DropUserTypeSpecification dropType(String name) {
		return new DropUserTypeSpecification(CqlIdentifier.fromCql(name));
	}

	/**
	 * Entry point into the {@link DropUserTypeSpecification}'s fluent API given {@code name} to drop a type. Convenient
	 * if imported statically.
	 *
	 * @param name must not be {@code null} or empty.
	 * @return a new {@link DropUserTypeSpecification}.
	 */
	public static DropUserTypeSpecification dropType(CqlIdentifier name) {
		return new DropUserTypeSpecification(name);
	}

	/**
	 * Enables the inclusion of an{@code IF EXISTS} clause.
	 *
	 * @return this {@link DropUserTypeSpecification}.
	 */
	public DropUserTypeSpecification ifExists() {
		return ifExists(true);
	}

	/**
	 * Sets the inclusion of an {@code IF EXISTS} clause.
	 *
	 * @param ifExists {@literal true} to include an {@code IF EXISTS} clause, {@literal false} to omit the
	 *          {@code IF NOT EXISTS} clause.
	 * @return this {@link DropUserTypeSpecification}.
	 */
	public DropUserTypeSpecification ifExists(boolean ifExists) {

		this.ifExists = ifExists;

		return this;
	}

	/**
	 * @return {@literal true} if the {@code IF EXISTS} clause is included.
	 */
	public boolean getIfExists() {
		return ifExists;
	}
}
