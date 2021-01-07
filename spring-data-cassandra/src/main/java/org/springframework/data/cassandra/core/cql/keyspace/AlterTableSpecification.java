/*
 * Copyright 2013-2021 the original author or authors.
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;

/**
 * Object to configure a {@code ALTER TABLE} specification.
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
 * @see AddColumnSpecification
 * @see AlterColumnSpecification
 * @see DropColumnSpecification
 * @see RenameColumnSpecification
 * @see CreateTableSpecification
 * @see DropTableSpecification
 * @see TableOptionsSpecification
 * @see org.springframework.data.cassandra.core.cql.generator.AlterTableCqlGenerator
 */
public class AlterTableSpecification extends TableOptionsSpecification<AlterTableSpecification> {

	/**
	 * The list of column changes.
	 */
	private List<ColumnChangeSpecification> changes = new ArrayList<>();

	private AlterTableSpecification(CqlIdentifier name) {
		super(name);
	}

	/**
	 * Entry point into the {@link AlterTableSpecification}'s fluent API given {@code tableName} to alter a table.
	 * Convenient if imported statically.
	 *
	 * @param tableName must not be {@literal null} or empty.
	 * @return a new {@link AlterTableSpecification}.
	 */
	public static AlterTableSpecification alterTable(String tableName) {
		return alterTable(CqlIdentifier.fromCql(tableName));
	}

	/**
	 * Entry point into the {@link AlterTableSpecification}'s fluent API given {@code tableName} to alter a table.
	 * Convenient if imported statically.
	 *
	 * @param tableName must not be {@literal null}.
	 * @return a new {@link AlterTableSpecification}.
	 */
	public static AlterTableSpecification alterTable(CqlIdentifier tableName) {
		return new AlterTableSpecification(tableName);
	}

	/**
	 * Adds an {@code ADD} to the list of column changes.
	 *
	 * @param column must not be {@literal null} or empty.
	 * @param type must not be {@literal null}.
	 * @return {@literal this} {@link AlterTableSpecification}.
	 */
	public AlterTableSpecification add(String column, DataType type) {
		return add(CqlIdentifier.fromCql(column), type);
	}

	/**
	 * Adds an {@code ADD} to the list of column changes.
	 *
	 * @param column must not be {@literal null}.
	 * @param type must not be {@literal null}.
	 * @return {@literal this} {@link AlterTableSpecification}.
	 * @since 2.0
	 */
	public AlterTableSpecification add(CqlIdentifier column, DataType type) {
		return add(AddColumnSpecification.addColumn(column, type));
	}

	/*
	 * Adds a {@code DROP} to the list of column changes.
	 *
	 * @param column must not be {@literal null} or empty.
	 * @return {@literal this} {@link AlterTableSpecification}.
	 */
	public AlterTableSpecification drop(String column) {
		return drop(CqlIdentifier.fromCql(column));
	}

	/*
	 * Adds a {@code DROP} to the list of column changes.
	 *
	 * @param column must not be {@literal null} or empty.
	 * @return {@literal this} {@link AlterTableSpecification}.
	 * @since 2.0
	 */
	public AlterTableSpecification drop(CqlIdentifier column) {
		return add(DropColumnSpecification.dropColumn(column));
	}

	/**
	 * Adds a {@code RENAME} to the list of column changes.
	 *
	 * @param from must not be {@literal null} or empty.
	 * @param to must not be {@literal null} or empty.
	 * @return {@literal this} {@link AlterTableSpecification}.
	 */
	public AlterTableSpecification rename(String from, String to) {
		return rename(CqlIdentifier.fromCql(from), CqlIdentifier.fromCql(to));
	}

	/**
	 * Adds a {@code RENAME} to the list of column changes.
	 *
	 * @param from must not be {@literal null}.
	 * @param to must not be {@literal null}.
	 * @return {@literal this} {@link AlterTableSpecification}.
	 * @since 2.0
	 */
	public AlterTableSpecification rename(CqlIdentifier from, CqlIdentifier to) {
		return add(new RenameColumnSpecification(from, to));
	}

	/**
	 * Adds an {@literal ALTER} to the list of column changes.
	 *
	 * @param column must not be {@literal null} or empty
	 * @param type must not be {@literal null}
	 * @return {@literal this} {@link AlterTableSpecification}.
	 */
	public AlterTableSpecification alter(String column, DataType type) {
		return alter(CqlIdentifier.fromCql(column), type);
	}

	/**
	 * Adds an {@literal ALTER} to the list of column changes.
	 *
	 * @param column must not be {@literal null}.
	 * @param type must not be {@literal null}.
	 * @return {@literal this} {@link AlterTableSpecification}.
	 * @since 2.0
	 */
	public AlterTableSpecification alter(CqlIdentifier column, DataType type) {
		return add(AlterColumnSpecification.alterColumn(column, type));
	}

	private AlterTableSpecification add(ColumnChangeSpecification specification) {

		changes.add(specification);

		return this;
	}

	/**
	 * Returns an unmodifiable list of column changes.
	 */
	public List<ColumnChangeSpecification> getChanges() {
		return Collections.unmodifiableList(changes);
	}
}
