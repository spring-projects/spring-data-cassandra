/*
 * Copyright 2013-2018 the original author or authors.
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
package org.springframework.data.cassandra.core.cql.keyspace;

import lombok.EqualsAndHashCode;

import org.springframework.data.cassandra.core.cql.KeyspaceIdentifier;

/**
 * Object to configure a {@code DROP KEYSPACE} specification.
 *
 * @author Mark Paluch
 */
@EqualsAndHashCode(callSuper = true)
public class DropKeyspaceSpecification extends KeyspaceActionSpecification {

	private boolean ifExists;

	private DropKeyspaceSpecification(KeyspaceIdentifier name) {
		super(name);
	}

	/**
	 * Create a new {@link DropKeyspaceSpecification} for the given {@code name}.
	 *
	 * @param name must not be {@literal null} or empty.
	 * @return a new {@link DropKeyspaceSpecification}.
	 */
	public static DropKeyspaceSpecification dropKeyspace(String name) {
		return new DropKeyspaceSpecification(KeyspaceIdentifier.of(name));
	}

	/**
	 * Create a new {@link DropKeyspaceSpecification} for the given {@code name}.
	 *
	 * @param name must not be {@literal null}.
	 * @return a new {@link DropKeyspaceSpecification}.
	 */
	public static DropKeyspaceSpecification dropKeyspace(KeyspaceIdentifier name) {
		return new DropKeyspaceSpecification(name);
	}

	/**
	 * Causes the inclusion of an {@code IF EXISTS} clause.
	 *
	 * @return this
	 */
	public DropKeyspaceSpecification ifExists() {
		return ifExists(true);
	}

	/**
	 * Toggles the inclusion of an {@code IF EXISTS} clause.
	 *
	 * @return this
	 */
	public DropKeyspaceSpecification ifExists(boolean ifExists) {

		this.ifExists = ifExists;

		return this;
	}

	public boolean getIfExists() {
		return ifExists;
	}
}
