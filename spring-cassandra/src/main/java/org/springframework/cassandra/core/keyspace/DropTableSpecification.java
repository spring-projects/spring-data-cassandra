/*
 * Copyright 2011-2013 the original author or authors.
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
package org.springframework.cassandra.core.keyspace;

/**
 * Builder class that supports the construction of <code>DROP TABLE</code> specifications.
 * 
 * @author Matthew T. Adams
 */
public class DropTableSpecification extends TableNameSpecification<DropTableSpecification> {

	// private boolean ifExists;

	// Added in Cassandra 2.0.

	// public DropTableSpecification ifExists() {
	// return ifExists(true);
	// }
	//
	// public DropTableSpecification ifExists(boolean ifExists) {
	// this.ifExists = ifExists;
	// return this;
	// }
	//
	// public boolean getIfExists() {
	// return ifExists;
	// }

	/**
	 * Entry point into the {@link DropTableSpecification}'s fluent API to drop a table. Convenient if imported
	 * statically.
	 */
	public static DropTableSpecification dropTable() {
		return new DropTableSpecification();
	}

	/**
	 * Entry point into the {@link DropTableSpecification}'s fluent API to drop a table. Convenient if imported
	 * statically. This static method is shorter than the no-arg form, which would be
	 * <code>dropTable().name(tableName)</code>.
	 * 
	 * @param tableName The name of the table to drop.
	 */
	public static DropTableSpecification dropTable(String tableName) {
		return new DropTableSpecification().name(tableName);
	}
}
