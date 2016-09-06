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

import org.springframework.cassandra.core.cql.CqlIdentifier;

/**
 * Builder class that supports the construction of <code>DROP TYPE</code> specifications.
 * 
 * @author Fabio J. Mendes
 */
public class DropUserTypeSpecification extends UserTypeNameSpecification<DropUserTypeSpecification> {

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
	 * Entry point into the {@link DropUserTypeSpecification}'s fluent API to drop a type. Convenient if imported
	 * statically.
	 */
	public static DropUserTypeSpecification dropType() {
		return new DropUserTypeSpecification();
	}

	/**
	 * Entry point into the {@link DropUserTypeSpecification}'s fluent API to drop a type. Convenient if imported
	 * statically. This static method is shorter than the no-arg form, which would be
	 * <code>dropType().name(typeName)</code>.
	 * 
	 * @param typeName The name of the type to drop.
	 */
	public static DropUserTypeSpecification dropType(CqlIdentifier typeName) {
		return new DropUserTypeSpecification().name(typeName);
	}

	/**
	 * Entry point into the {@link DropUserTypeSpecification}'s fluent API to drop a type. Convenient if imported
	 * statically. This static method is shorter than the no-arg form, which would be
	 * <code>dropType().name(typeName)</code>.
	 * 
	 * @param typeName The name of the type to drop.
	 */
	public static DropUserTypeSpecification dropType(String typeName) {
		return new DropUserTypeSpecification().name(typeName);
	}
}
