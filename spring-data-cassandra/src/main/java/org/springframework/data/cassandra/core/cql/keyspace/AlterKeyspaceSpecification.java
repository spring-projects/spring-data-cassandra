/*
 * Copyright 2013-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
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
 * Object to configure a {@code ALTER KEYSPACE} specification.
 *
 * @author Mark Paluch
 */
public class AlterKeyspaceSpecification extends KeyspaceOptionsSpecification<AlterKeyspaceSpecification> {

	private AlterKeyspaceSpecification(CqlIdentifier name) {
		super(name);
	}

	/**
	 * Entry point into the {@link AlterKeyspaceSpecification}'s fluent API given {@code name} to alter a keyspace.
	 * Convenient if imported statically.
	 *
	 * @param name must not be {@literal null} or empty.
	 * @return a new {@link AlterKeyspaceSpecification}.
	 */
	public static AlterKeyspaceSpecification alterKeyspace(String name) {
		return alterKeyspace(CqlIdentifier.fromCql(name));
	}

	/**
	 * Entry point into the {@link AlterKeyspaceSpecification}'s fluent API given {@code name} to alter a keyspace.
	 * Convenient if imported statically.
	 *
	 * @param name must not be {@literal null} or empty.
	 * @return a new {@link AlterKeyspaceSpecification}.
	 */
	public static AlterKeyspaceSpecification alterKeyspace(CqlIdentifier name) {
		return new AlterKeyspaceSpecification(name);
	}
}
