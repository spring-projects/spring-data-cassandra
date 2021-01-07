/*
 * Copyright 2016-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
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
 * Object to configure a {@code ALTER TYPE} specification.
 *
 * @author Fabio J. Mendes
 * @author Mark Paluch
 * @since 1.5
 * @see CqlIdentifier
 */
public class AlterUserTypeSpecification extends UserTypeNameSpecification {

	private final List<ColumnChangeSpecification> changes = new ArrayList<>();

	private AlterUserTypeSpecification(CqlIdentifier name) {
		super(name);
	}

	/**
	 * Entry point into the {@link AlterColumnSpecification}'s fluent API given {@code typeName} to alter a user type.
	 * Convenient if imported statically.
	 *
	 * @param typeName must not be {@literal null} or empty.
	 * @return a new {@link AlterUserTypeSpecification}.
	 */
	public static AlterUserTypeSpecification alterType(String typeName) {
		return alterType(CqlIdentifier.fromCql(typeName));
	}

	/**
	 * Entry point into the {@link AlterUserTypeSpecification}'s fluent API given {@code typeName} to alter a type.
	 * Convenient if imported statically.
	 *
	 * @param typeName must not be {@literal null} or empty.
	 * @return a new {@link AlterUserTypeSpecification}.
	 */
	private static AlterUserTypeSpecification alterType(CqlIdentifier typeName) {
		return new AlterUserTypeSpecification(typeName);
	}

	/**
	 * Adds an {@literal ADD} to the list of field changes.
	 *
	 * @param field must not be {@literal null} or empty.
	 * @param type must not be {@literal null}.
	 * @return {@code this} {@link AlterUserTypeSpecification}.
	 */
	public AlterUserTypeSpecification add(String field, DataType type) {
		return add(CqlIdentifier.fromCql(field), type);
	}

	/**
	 * Adds an {@literal ADD} to the list of field changes.
	 *
	 * @param field must not be {@literal null}.
	 * @param type must not be {@literal null}.
	 * @return {@code this} {@link AlterUserTypeSpecification}.
	 */
	public AlterUserTypeSpecification add(CqlIdentifier field, DataType type) {
		return add(AddColumnSpecification.addColumn(field, type));
	}

	/**
	 * Adds an {@literal ALTER} to the list of field changes.
	 *
	 * @param field must not be {@literal null} or empty.
	 * @param type must not be {@literal null}.
	 * @return {@code this} {@link AlterUserTypeSpecification}.
	 */
	public AlterUserTypeSpecification alter(String field, DataType type) {
		return alter(CqlIdentifier.fromCql(field), type);
	}

	/**
	 * Adds an {@literal ALTER} to the list of field changes.
	 *
	 * @param field must not be {@literal null}.
	 * @param type must not be {@literal null}.
	 * @return {@code this} {@link AlterUserTypeSpecification}.
	 */
	public AlterUserTypeSpecification alter(CqlIdentifier field, DataType type) {
		return add(AlterColumnSpecification.alterColumn(field, type));
	}

	/**
	 * Adds an {@literal RENAME} to the list of field changes.
	 *
	 * @param from must not be {@literal null} or empty.
	 * @param to must not be {@literal null} or empty.
	 * @return {@code this} {@link AlterUserTypeSpecification}.
	 */
	public AlterUserTypeSpecification rename(String from, String to) {
		return rename(CqlIdentifier.fromCql(from), CqlIdentifier.fromCql(to));
	}

	/**
	 * Adds an {@literal RENAME} to the list of field changes.
	 *
	 * @param from must not be {@literal null}.
	 * @param to must not be {@literal null} or empty.
	 * @return {@code this} {@link AlterUserTypeSpecification}.
	 */
	public AlterUserTypeSpecification rename(CqlIdentifier from, CqlIdentifier to) {
		return add(new RenameColumnSpecification(from, to));
	}

	private AlterUserTypeSpecification add(ColumnChangeSpecification specification) {

		changes.add(specification);

		return this;
	}

	/**
	 * @return an unmodifiable list of field changes.
	 */
	public List<ColumnChangeSpecification> getChanges() {
		return Collections.unmodifiableList(changes);
	}
}
