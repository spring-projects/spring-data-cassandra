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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;

/**
 * Value object representing a specification to alter a column.
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
 * @see CqlIdentifier
 */
public class AlterColumnSpecification extends ColumnTypeChangeSpecification {

	private AlterColumnSpecification(CqlIdentifier name, DataType type) {
		super(name, type);
	}

	/**
	 * Entry point into the {@link AlterColumnSpecification}'s fluent API given {@code name} and {@link DataType} to alter
	 * a column. Convenient if imported statically.
	 *
	 * @param name must not be {@literal null} or empty.
	 * @param type must not be {@literal null}.
	 * @return a new {@link AlterColumnSpecification}.
	 */
	public static AlterColumnSpecification alterColumn(String name, DataType type) {
		return alterColumn(CqlIdentifier.fromCql(name), type);
	}

	/**
	 * Entry point into the {@link AlterColumnSpecification}'s fluent API given {@code name} and {@link DataType} to alter
	 * a column. Convenient if imported statically.
	 *
	 * @param name must not be {@literal null}.
	 * @param type must not be {@literal null}.
	 * @return a new {@link AlterColumnSpecification}.
	 */
	public static AlterColumnSpecification alterColumn(CqlIdentifier name, DataType type) {
		return new AlterColumnSpecification(name, type);
	}
}
