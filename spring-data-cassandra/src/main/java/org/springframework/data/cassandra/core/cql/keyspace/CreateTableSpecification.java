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

import org.springframework.data.cassandra.core.cql.CqlIdentifier;

/**
 * Builder class to construct a {@code CREATE TABLE} specification.
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
	 * Entry point into the {@link CreateTableSpecification}'s fluent API to create a table. Convenient if imported
	 * statically.
	 */
	public static CreateTableSpecification createTable(CqlIdentifier name) {
		return new CreateTableSpecification(name);
	}

	/**
	 * Entry point into the {@link CreateTableSpecification}'s fluent API to create a table. Convenient if imported
	 * statically.
	 */
	public static CreateTableSpecification createTable(String name) {
		return new CreateTableSpecification(CqlIdentifier.cqlId(name));
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
	public CreateTableSpecification with(String name, Object value, boolean escape, boolean quote) {
		return (CreateTableSpecification) super.with(name, value, escape, quote);
	}
}
