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

import org.springframework.lang.Nullable;

/**
 * Object to configure a {@code CREATE TABLE} specification.
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
public class CreateTableSpecification extends TableSpecification<CreateTableSpecification> {

	private boolean ifNotExists = false;

	private CreateTableSpecification(CqlIdentifier name) {
		super(name);
	}

	/**
	 * Entry point into the {@link CreateTableSpecification}'s fluent API given {@code tableName} to create a table.
	 * Convenient if imported statically.
	 *
	 * @param tableName must not be {@literal null} or empty.
	 * @return a new {@link CreateTableSpecification}.
	 */
	public static CreateTableSpecification createTable(String tableName) {
		return new CreateTableSpecification(CqlIdentifier.fromCql(tableName));
	}

	/**
	 * Entry point into the {@link CreateTableSpecification}'s fluent API given {@code tableName} to create a table.
	 * Convenient if imported statically.
	 *
	 * @param tableName must not be {@literal null}.
	 * @return a new {@link CreateTableSpecification}.
	 */
	public static CreateTableSpecification createTable(CqlIdentifier tableName) {
		return new CreateTableSpecification(tableName);
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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.keyspace.TableOptionsSpecification#with(org.springframework.data.cassandra.core.cql.keyspace.TableOption)
	 */
	@Override
	public CreateTableSpecification with(TableOption option) {
		return (CreateTableSpecification) super.with(option);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.keyspace.TableOptionsSpecification#with(org.springframework.data.cassandra.core.cql.keyspace.TableOption, java.lang.Object)
	 */
	@Override
	public CreateTableSpecification with(TableOption option, Object value) {
		return (CreateTableSpecification) super.with(option, value);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.keyspace.TableOptionsSpecification#with(java.lang.String, java.lang.Object, boolean, boolean)
	 */
	@Override
	public CreateTableSpecification with(String name, @Nullable Object value, boolean escape, boolean quote) {
		return (CreateTableSpecification) super.with(name, value, escape, quote);
	}
}
