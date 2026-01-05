/*
 * Copyright 2013-present the original author or authors.
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


import org.jspecify.annotations.Nullable;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * Object to configure a {@code DROP TABLE} specification.
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
public class DropTableSpecification extends TableNameSpecification implements CqlSpecification {

	private boolean ifExists = false;

	private DropTableSpecification(@Nullable CqlIdentifier keyspace, CqlIdentifier name) {
		super(keyspace, name);
	}

	/**
	 * Entry point into the {@link DropTableSpecification}'s fluent API {@code tableName} to drop a table. Convenient if
	 * imported statically.
	 *
	 * @param tableName must not be {@literal null} or empty.
	 * @return a new {@link DropTableSpecification}.
	 */
	public static DropTableSpecification dropTable(String tableName) {
		return dropTable(CqlIdentifier.fromCql(tableName));
	}

	/**
	 * Entry point into the {@link DropTableSpecification}'s fluent API given {@code tableName} to drop a table.
	 * Convenient if imported statically.
	 *
	 * @param tableName must not be {@literal null}.
	 * @return a new {@link DropTableSpecification}.
	 */
	public static DropTableSpecification dropTable(CqlIdentifier tableName) {
		return new DropTableSpecification(null, tableName);
	}

	/**
	 * Entry point into the {@link DropTableSpecification}'s fluent API given {@code tableName} to drop a table.
	 * Convenient if imported statically. Uses the default keyspace if {@code keyspace} is null; otherwise, of the
	 * {@code keyspace} is not {@literal null}, then the table name is prefixed with {@code keyspace}.
	 *
	 * @param keyspace can be {@literal null}.
	 * @param tableName must not be {@literal null}.
	 * @return a new {@link DropTableSpecification}.
	 * @since 4.4
	 */
	public static DropTableSpecification dropTable(@Nullable CqlIdentifier keyspace, CqlIdentifier tableName) {
		return new DropTableSpecification(keyspace, tableName);
	}

	/**
	 * Causes the inclusion of an {@code IF EXISTS} clause.
	 *
	 * @return this
	 * @since 2.1
	 */
	public DropTableSpecification ifExists() {
		return ifExists(true);
	}

	/**
	 * Toggles the inclusion of an {@code IF EXISTS} clause.
	 *
	 * @return this
	 * @since 2.1
	 */
	public DropTableSpecification ifExists(boolean ifExists) {

		this.ifExists = ifExists;

		return this;
	}

	public boolean getIfExists() {
		return this.ifExists;
	}
}
