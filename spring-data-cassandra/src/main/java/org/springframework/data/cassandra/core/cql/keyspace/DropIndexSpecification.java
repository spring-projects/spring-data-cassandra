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
 * Builder class that supports the construction of {@code DROP INDEX} specifications.
 *
 * @author Matthew T. Adams
 * @author David Webb
 */
public class DropIndexSpecification extends IndexNameSpecification<DropIndexSpecification> {

	private DropIndexSpecification(CqlIdentifier name) {
		super(name);
	}

	/**
	 * Entry point into the {@link DropIndexSpecification}'s fluent API to drop a table. Convenient if imported
	 * statically.
	 */
	public static DropIndexSpecification dropIndex(String name) {
		return new DropIndexSpecification(CqlIdentifier.cqlId(name));
	}

	/**
	 * Entry point into the {@link DropIndexSpecification}'s fluent API to drop a table. Convenient if imported
	 * statically.
	 */
	public static DropIndexSpecification dropIndex(CqlIdentifier name) {
		return new DropIndexSpecification(name);
	}
}
