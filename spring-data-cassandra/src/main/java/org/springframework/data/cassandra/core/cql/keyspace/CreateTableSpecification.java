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
 * Object to configure a {@code CREATE TABLE} specification.
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
public class CreateTableSpecification extends TableSpecification<CreateTableSpecification> implements CqlSpecification {

	private boolean ifNotExists = false;

	private CreateTableSpecification(@Nullable CqlIdentifier keyspace, CqlIdentifier name) {
		super(keyspace, name);
	}

	/**
	 * Entry point into the {@link CreateTableSpecification}'s fluent API given {@code tableName} to create a table.
	 * Convenient if imported statically.
	 *
	 * @param tableName must not be {@literal null} or empty.
	 * @return a new {@link CreateTableSpecification}.
	 */
	public static CreateTableSpecification createTable(String tableName) {
		return new CreateTableSpecification(null, CqlIdentifier.fromCql(tableName));
	}

	/**
	 * Entry point into the {@link CreateTableSpecification}'s fluent API given {@code tableName} to create a table.
	 * Convenient if imported statically.
	 *
	 * @param tableName must not be {@literal null}.
	 * @return a new {@link CreateTableSpecification}.
	 */
	public static CreateTableSpecification createTable(CqlIdentifier tableName) {
		return new CreateTableSpecification(null, tableName);
	}

	/**
	 * Entry point into the {@link CreateTableSpecification}'s fluent API given {@code tableName} to create a table.
	 * Convenient if imported statically. Uses the default keyspace if {@code keyspace} is null; otherwise, of the
	 * {@code keyspace} is not {@literal null}, then the table name is prefixed with {@code keyspace}.
	 *
	 * @param keyspace can be {@literal null}.
	 * @param tableName must not be {@literal null}.
	 * @return a new {@link CreateTableSpecification}.
	 * @since 4.4
	 */
	public static CreateTableSpecification createTable(@Nullable CqlIdentifier keyspace, CqlIdentifier tableName) {
		return new CreateTableSpecification(keyspace, tableName);
	}

	/**
	 * Causes the inclusion of an {@code IF NOT EXISTS} clause.
	 *
	 * @return this
	 */
	public CreateTableSpecification ifNotExists() {
		return ifNotExists(true);
	}

	/**
	 * Toggles the inclusion of an {@code IF NOT EXISTS} clause.
	 *
	 * @return this
	 */
	public CreateTableSpecification ifNotExists(boolean ifNotExists) {

		this.ifNotExists = ifNotExists;

		return this;
	}

	public boolean getIfNotExists() {
		return this.ifNotExists;
	}

	@Override
	public CreateTableSpecification with(TableOption option) {
		return (CreateTableSpecification) super.with(option);
	}

	@Override
	public CreateTableSpecification with(TableOption option, Object value) {
		return (CreateTableSpecification) super.with(option, value);
	}

	@Override
	public CreateTableSpecification with(String name, @Nullable Object value) {
		return (CreateTableSpecification) super.with(name, value);
	}

	@Override
	public CreateTableSpecification with(String name, @Nullable Object value, boolean escape, boolean quote) {
		return (CreateTableSpecification) super.with(name, value, escape, quote);
	}
}
