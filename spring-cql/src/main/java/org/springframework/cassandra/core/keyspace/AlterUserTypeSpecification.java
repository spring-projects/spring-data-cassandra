/*
 * Copyright 2013-2014 the original author or authors.
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
package org.springframework.cassandra.core.keyspace;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.springframework.cassandra.core.cql.CqlIdentifier;

import com.datastax.driver.core.DataType;

/**
 * Builder class to construct an <code>ALTER TYPE</code> specification.
 * 
 * @author Fabio J. Mendes
 */
public class AlterUserTypeSpecification extends UserTypeSpecification<AlterUserTypeSpecification> {

	/**
	 * Entry point into the {@link AlterUserTypeSpecification}'s fluent API to alter a type. Convenient if imported
	 * statically.
	 */
	public static AlterUserTypeSpecification alterType() {
		return new AlterUserTypeSpecification();
	}

	/**
	 * Entry point into the {@link AlterUserTypeSpecification}'s fluent API to alter a type. Convenient if imported
	 * statically.
	 */
	public static AlterUserTypeSpecification alterType(CqlIdentifier tableName) {
		return (AlterUserTypeSpecification) new AlterUserTypeSpecification().name(tableName);
	}

	/**
	 * Entry point into the {@link AlterUserTypeSpecification}'s fluent API to alter a type. Convenient if imported
	 * statically.
	 */
	public static AlterUserTypeSpecification alterType(String tableName) {
		return (AlterUserTypeSpecification) new AlterUserTypeSpecification().name(tableName);
	}
	
	@Override
	public AlterUserTypeSpecification name(String name) {
		return (AlterUserTypeSpecification) super.name(name);
	}

	/**
	 * The list of column changes.
	 */
	private List<ColumnChangeSpecification> changes = new ArrayList<ColumnChangeSpecification>();

	/*
	 * Adds a <code>DROP</code> to the list of column changes.
	 * 
	 * DW Removed as this only works in C* 2.0
	 */
	// public AlterUserTypeSpecification drop(String column) {
	// changes.add(new DropColumnSpecification(column));
	// return this;
	// }

	/**
	 * Adds an <code>ADD</code> to the list of column changes.
	 */
	public AlterUserTypeSpecification add(String column, DataType type) {
		changes.add(new AddColumnSpecification(column, type));
		return this;
	}

	/**
	 * Adds an <code>ALTER</code> to the list of column changes.
	 */
	public AlterUserTypeSpecification alter(String column, DataType type) {
		changes.add(new AlterColumnSpecification(column, type));
		return this;
	}

	/**
	 * Returns an unmodifiable list of column changes.
	 */
	public List<ColumnChangeSpecification> getChanges() {
		return Collections.unmodifiableList(changes);
	}
}
