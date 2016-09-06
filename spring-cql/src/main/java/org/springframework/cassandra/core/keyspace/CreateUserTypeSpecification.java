/*
 * Copyright 2013-2014 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.cassandra.core.keyspace;

import org.springframework.cassandra.core.cql.CqlIdentifier;

/**
 * Builder class to construct a <code>CREATE TYPE</code> specification.
 * 
 * @author Fabio J. Mendes
 */
public class CreateUserTypeSpecification extends UserTypeSpecification<CreateUserTypeSpecification> {

	/**
	 * Entry point into the {@link CreateUserTypeSpecification}'s fluent API to create a type. Convenient if imported
	 * statically.
	 */
	public static CreateUserTypeSpecification createType() {
		return new CreateUserTypeSpecification();
	}

	/**
	 * Entry point into the {@link CreateUserTypeSpecification}'s fluent API to create a type. Convenient if imported
	 * statically.
	 */
	public static CreateUserTypeSpecification createType(CqlIdentifier name) {
		return new CreateUserTypeSpecification().name(name);
	}

	/**
	 * Entry point into the {@link CreateUserTypeSpecification}'s fluent API to create a type. Convenient if imported
	 * statically.
	 */
	public static CreateUserTypeSpecification createType(String name) {
		return new CreateUserTypeSpecification().name(name);
	}

	private boolean ifNotExists = false;

	@Override
	public CreateUserTypeSpecification name(CqlIdentifier name) {
		return (CreateUserTypeSpecification) super.name(name);
	}

	/**
	 * Causes the inclusion of an <code>IF NOT EXISTS</code> clause.
	 * 
	 * @return this
	 */
	public CreateUserTypeSpecification ifNotExists() {
		return ifNotExists(true);
	}

	/**
	 * Toggles the inclusion of an <code>IF NOT EXISTS</code> clause.
	 * 
	 * @return this
	 */
	public CreateUserTypeSpecification ifNotExists(boolean ifNotExists) {
		this.ifNotExists = ifNotExists;
		return this;
	}

	public boolean getIfNotExists() {
		return ifNotExists;
	}

	@Override
	public CreateUserTypeSpecification name(String name) {
		return (CreateUserTypeSpecification) super.name(name);
	}
}
