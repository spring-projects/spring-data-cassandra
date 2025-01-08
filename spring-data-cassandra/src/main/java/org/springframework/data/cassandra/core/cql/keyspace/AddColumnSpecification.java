/*
 * Copyright 2013-2025 the original author or authors.
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
import com.datastax.oss.driver.api.core.type.DataType;


/**
 * Value object representing a specification to add a column.
 *
 * @author Matthew Adams
 * @author Mark Paluch
 * @see ColumnTypeChangeSpecification
 */
public class AddColumnSpecification extends ColumnTypeChangeSpecification {

	private AddColumnSpecification(CqlIdentifier name, DataType type) {
		super(name, type);
	}

	/**
	 * Create a new {@link AddColumnSpecification} for the given {@code name} and {@link DataType}.
	 *
	 * @param name must not be {@literal null} or empty.
	 * @param type must not be {@literal null}.
	 * @return a new {@link AddColumnSpecification}.
	 */
	public static AddColumnSpecification addColumn(String name, DataType type) {
		return addColumn(CqlIdentifier.fromCql(name), type);
	}

	/**
	 * Create a new {@link AddColumnSpecification} for the given {@code name} and {@link DataType}.
	 *
	 * @param name must not be {@literal null}.
	 * @param type must not be {@literal null}.
	 * @return a new {@link AddColumnSpecification}.
	 */
	public static AddColumnSpecification addColumn(CqlIdentifier name, DataType type) {
		return new AddColumnSpecification(name, type);
	}
}
