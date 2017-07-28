/*
 * Copyright 2016-2017 the original author or authors.
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
package org.springframework.data.cassandra.core.cql.keyspace;

import org.springframework.data.cassandra.core.cql.CqlIdentifier;

/**
 * Builder class that supports the construction of {@code DROP TYPE} specifications.
 *
 * @author Fabio J. Mendes
 * @author Mark Paluch
 * @since 1.5
 * @see CqlIdentifier
 */
public class DropUserTypeSpecification extends UserTypeNameSpecification<DropUserTypeSpecification> {

	private boolean ifExists;

	private DropUserTypeSpecification(CqlIdentifier name) {
		super(name);
	}

	/**
	 * Entry point into the {@link DropUserTypeSpecification}'s fluent API to drop a type. Convenient if imported
	 * statically.
	 *
	 * @param typeName The name of the type to drop.
	 */
	public static DropUserTypeSpecification dropType(String typeName) {
		return new DropUserTypeSpecification(CqlIdentifier.cqlId(typeName));
	}

	/**
	 * Entry point into the {@link DropUserTypeSpecification}'s fluent API to drop a type. Convenient if imported
	 * statically.
	 *
	 * @param typeName The name of the type to drop.
	 */
	public static DropUserTypeSpecification dropType(CqlIdentifier typeName) {
		return new DropUserTypeSpecification(typeName);
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
