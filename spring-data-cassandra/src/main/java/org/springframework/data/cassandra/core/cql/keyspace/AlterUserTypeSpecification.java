/*
 * Copyright 2016-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
 * Builder class to construct an {@code ALTER TYPE} specification.
 *
 * @author Fabio J. Mendes
 * @author Mark Paluch
 * @since 1.5
 * @see CqlIdentifier
 */
public class AlterUserTypeSpecification extends UserTypeNameSpecification<AlterUserTypeSpecification> {

	private final List<ColumnChangeSpecification> changes = new ArrayList<>();

	/**
	 * Entry point into the {@link AlterUserTypeSpecification}'s fluent API to alter a type. Convenient if imported
	 * statically.
	 */
	public static AlterUserTypeSpecification alterType(String typeName) {
		return alterType(CqlIdentifier.cqlId(typeName));
	}

	/**
	 * Entry point into the {@link AlterUserTypeSpecification}'s fluent API to alter a type. Convenient if imported
	 * statically.
	 */
	private static AlterUserTypeSpecification alterType(CqlIdentifier typeName) {
		return new AlterUserTypeSpecification(typeName);
	}

	private AlterUserTypeSpecification(CqlIdentifier name) {
		super(name);
	}

	/**
	 * Adds an {@literal ADD} to the list of field changes.
	 *
	 * @param field must not be empty or {@literal null}.
	 * @param type must not be {@literal null}.
	 * @return {@code this} {@link AlterUserTypeSpecification}.
	 */
	public AlterUserTypeSpecification add(String field, DataType type) {
		return add(CqlIdentifier.cqlId(field), type);
	}

	/**
	 * Adds an {@literal ADD} to the list of field changes.
	 *
	 * @param field must not be {@literal null}.
	 * @param type must not be {@literal null}.
	 * @return {@code this} {@link AlterUserTypeSpecification}.
	 */
	public AlterUserTypeSpecification add(CqlIdentifier field, DataType type) {

		changes.add(AddColumnSpecification.addColumn(field, type));

		return this;
	}

	/**
	 * Adds an {@literal ALTER} to the list of field changes.
	 *
	 * @param field must not be empty or {@literal null}.
	 * @param type must not be {@literal null}.
	 * @return {@code this} {@link AlterUserTypeSpecification}.
	 */
	public AlterUserTypeSpecification alter(String field, DataType type) {
		return alter(CqlIdentifier.cqlId(field), type);
	}

	/**
	 * Adds an {@literal ALTER} to the list of field changes.
	 *
	 * @param field must not be {@literal null}.
	 * @param type must not be {@literal null}.
	 * @return {@code this} {@link AlterUserTypeSpecification}.
	 */
	public AlterUserTypeSpecification alter(CqlIdentifier field, DataType type) {

		changes.add(AlterColumnSpecification.alterColumn(field, type));

		return this;
	}

	/**
	 * Adds an {@literal RENAME} to the list of field changes.
	 *
	 * @param from must not be empty or {@literal null}.
	 * @param to must not be empty or {@literal null}.
	 * @return {@code this} {@link AlterUserTypeSpecification}.
	 */
	public AlterUserTypeSpecification rename(String from, String to) {
		return rename(CqlIdentifier.cqlId(from), CqlIdentifier.cqlId(to));
	}

	/**
	 * Adds an {@literal RENAME} to the list of field changes.
	 *
	 * @param from must not be {@literal null}.
	 * @param to must not be empty or {@literal null}.
	 * @return {@code this} {@link AlterUserTypeSpecification}.
	 */
	public AlterUserTypeSpecification rename(CqlIdentifier from, CqlIdentifier to) {

		changes.add(new RenameColumnSpecification(from, to));

		return this;
	}

	/**
	 * @return an unmodifiable list of field changes.
	 */
	public List<ColumnChangeSpecification> getChanges() {
		return Collections.unmodifiableList(changes);
	}
}
