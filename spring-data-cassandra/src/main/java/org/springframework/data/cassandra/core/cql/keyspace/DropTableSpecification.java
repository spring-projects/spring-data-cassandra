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
 * Builder class that supports the construction of {@code DROP TABLE} specifications.
 *
 * @author Matthew T. Adams
 */
public class DropTableSpecification extends TableNameSpecification {

	private DropTableSpecification(CqlIdentifier name) {
		super(name);
	}

	/**
	 * Entry point into the {@link DropTableSpecification}'s fluent API to drop a table. Convenient if imported
	 * statically. This static method is shorter than the no-arg form, which would be {@code dropTable().name(tableName)}.
	 *
	 * @param tableName The name of the table to drop.
	 */
	public static DropTableSpecification dropTable(CqlIdentifier tableName) {
		return new DropTableSpecification(tableName);
	}

	/**
	 * Entry point into the {@link DropTableSpecification}'s fluent API to drop a table. Convenient if imported
	 * statically. This static method is shorter than the no-arg form, which would be {@code dropTable().name(tableName)}.
	 *
	 * @param tableName The name of the table to drop.
	 */
	public static DropTableSpecification dropTable(String tableName) {
		return new DropTableSpecification(CqlIdentifier.cqlId(tableName));
	}
}
