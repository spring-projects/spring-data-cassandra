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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.springframework.data.cassandra.core.cql.CqlIdentifier;

import com.datastax.driver.core.DataType;

/**
 * Builder class to construct an {@code ALTER TABLE} specification.
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
	 * Entry point into the {@link AlterTableSpecification}'s fluent API to alter a table. Convenient if imported
	 * statically.
	 */
	public static AlterTableSpecification alterTable(CqlIdentifier tableName) {
		return new AlterTableSpecification(tableName);
	}

	/**
	 * Entry point into the {@link AlterTableSpecification}'s fluent API to alter a table. Convenient if imported
	 * statically.
	 */
	public static AlterTableSpecification alterTable(String tableName) {
		return new AlterTableSpecification(CqlIdentifier.cqlId(tableName));
	}

	/*
	 * Adds a {@code DROP} to the list of column changes.
	 *
	 * @param column must not be empty or {@literal null}
	 * @return {@literal this} {@link AlterTableSpecification}
	 */
	public AlterTableSpecification drop(String column) {
		changes.add(DropColumnSpecification.dropColumn(column));
		return this;
	}

	/**
	 * Adds an {@code ADD} to the list of column changes.
	 *
	 * @param column must not be empty or {@literal null}
	 * @param type must not be {@literal null}
	 * @return {@literal this} {@link AlterTableSpecification}
	 */
	public AlterTableSpecification add(String column, DataType type) {
		changes.add(AddColumnSpecification.addColumn(column, type));
		return this;
	}

	/**
	 * Adds a {@code RENAME} to the list of column changes.
	 *
	 * @param from must not be empty or {@literal null}
	 * @param to must not be empty or {@literal null}
	 * @return {@literal this} {@link AlterTableSpecification}
	 */
	public AlterTableSpecification rename(String from, String to) {
		changes.add(new RenameColumnSpecification(from, to));
		return this;
	}

	/**
	 * Adds an {@literal ALTER} to the list of column changes.
	 *
	 * @param column must not be empty or {@literal null}
	 * @param type must not be {@literal null}
	 * @return {@literal this} {@link AlterTableSpecification}
	 */
	public AlterTableSpecification alter(String column, DataType type) {
		changes.add(AlterColumnSpecification.alterColumn(column, type));
		return this;
	}

	/**
	 * Returns an unmodifiable list of column changes.
	 */
	public List<ColumnChangeSpecification> getChanges() {
		return Collections.unmodifiableList(changes);
	}

}
