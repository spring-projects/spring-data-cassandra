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

import static org.springframework.cassandra.core.cql.CqlIdentifier.cqlId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.springframework.cassandra.core.cql.CqlIdentifier;

import com.datastax.driver.core.DataType;


/**
 * Builder class to support the construction of user type specifications that have columns. This class can also be used as a
 * standalone {@link UserTypeDescriptor}, independent of {@link CreateUserTypeSpecification}.
 * 
 * @author Fabio J. Mendes
 */
public class UserTypeSpecification<T> extends UserTypeNameSpecification<UserTypeSpecification<T>> implements UserTypeDescriptor {

	/**
	 * List of all columns.
	 */
	private List<ColumnSpecification> columns = new ArrayList<ColumnSpecification>();

	/**
	 * Adds the given column to the type.
	 * 
	 * @param name The column name; must be a valid unquoted or quoted identifier without the surrounding double quotes.
	 * @param type The data type of the column.
	 */
	public T column(String name, DataType type) {
		return column(cqlId(name), type, false);
	}

	/**
	 * Adds the given column to the type.
	 * 
	 * @param name The column name; must be a valid unquoted or quoted identifier without the surrounding double quotes.
	 * @param type The data type of the column.
	 */
	public T column(CqlIdentifier name, DataType type) {
		return column(name, type, false);
	}

	@SuppressWarnings("unchecked")
	protected T column(CqlIdentifier name, DataType type, boolean frozen) {
		ColumnSpecification column = new ColumnSpecification().name(name).type(type);
		columns.add(column);
		return (T) this;
	}

	/**
	 * Returns an unmodifiable list of all columns.
	 */
	@Override
	public List<ColumnSpecification> getColumns() {
		return Collections.unmodifiableList(columns);
	}

}
