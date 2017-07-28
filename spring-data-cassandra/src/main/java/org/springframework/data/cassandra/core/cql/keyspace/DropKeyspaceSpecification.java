/*
 * Copyright 2013-2017 the original author or authors.
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

@EqualsAndHashCode(callSuper = true)
public class DropKeyspaceSpecification extends KeyspaceActionSpecification<DropKeyspaceSpecification> {

	private boolean ifExists;

	private DropKeyspaceSpecification(KeyspaceIdentifier name) {
		super(name);
	}

	/**
	 * Entry point into the {@link DropKeyspaceSpecification}'s fluent API to drop a keyspace. Convenient if imported
	 * statically.
	 */
	public static DropKeyspaceSpecification dropKeyspace(KeyspaceIdentifier name) {
		return new DropKeyspaceSpecification(name);
	}

	/**
	 * Entry point into the {@link DropKeyspaceSpecification}'s fluent API to drop a keyspace. Convenient if imported
	 * statically.
	 */
	public static DropKeyspaceSpecification dropKeyspace(String name) {
		return new DropKeyspaceSpecification(KeyspaceIdentifier.ksId(name));
	}

	public DropKeyspaceSpecification ifExists() {
		return ifExists(true);
	}

	public DropKeyspaceSpecification ifExists(boolean ifExists) {
		this.ifExists = ifExists;
		return this;
	}

	public boolean getIfExists() {
		return ifExists;
	}
}
