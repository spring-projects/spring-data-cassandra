/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.cassandra.core;

import org.springframework.data.cassandra.core.cql.CqlIdentifier;

/**
 * {@link ExecutableInsertOperation} allows creation and execution of Cassandra {@code INSERT} insert operations in a
 * fluent API style.
 * <p>
 * The table to operate on is by default derived from the initial {@literal domainType} and can be defined there via
 * {@link org.springframework.data.cassandra.core.mapping.Table}. Using {@code inTable} allows to override the
 * collection name for the execution.
 *
 * <pre>
 *     <code>
 *         insert(Jedi.class)
 *             .inTable("star_wars")
 *             .one(luke);
 *     </code>
 * </pre>
 *
 * @author Mark Paluch
 * @since 2.1
 */
public interface ExecutableInsertOperation {

	/**
	 * Start creating an {@code INSERT} operation for given {@literal domainType}.
	 *
	 * @param domainType must not be {@literal null}.
	 * @return new instance of {@link ExecutableInsert}.
	 * @throws IllegalArgumentException if domainType is {@literal null}.
	 */
	<T> ExecutableInsert<T> insert(Class<T> domainType);

	/**
	 * Trigger insert execution by calling one of the terminating methods.
	 */
	interface TerminatingInsert<T> {

		/**
		 * Insert exactly one object.
		 *
		 * @param object must not be {@literal null}.
		 * @throws IllegalArgumentException if object is {@literal null}.
		 */
		WriteResult one(T object);
	}

	/**
	 * Collection override (optional).
	 */
	interface InsertWithTable<T> extends InsertWithOptions<T> {

		/**
		 * Explicitly set the name of the table.
		 * <p>
		 * Skip this step to use the default table derived from the domain type.
		 *
		 * @param table must not be {@literal null} or empty.
		 * @return new instance of {@link TerminatingInsert}.
		 * @throws IllegalArgumentException if {@code table} is {@literal null} or empty.
		 */
		InsertWithOptions<T> inTable(String table);

		/**
		 * Explicitly set the name of the table.
		 * <p>
		 * Skip this step to use the default table derived from the domain type.
		 *
		 * @param table must not be {@literal null}.
		 * @return new instance of {@link TerminatingInsert}.
		 * @throws IllegalArgumentException if {@link CqlIdentifier} is {@literal null}.
		 */
		InsertWithOptions<T> inTable(CqlIdentifier table);
	}

	/**
	 * Apply {@link InsertOptions} (optional).
	 */
	interface InsertWithOptions<T> extends TerminatingInsert<T> {

		/**
		 * Set insert options.
		 *
		 * @param insertOptions insertOptions not be {@literal null}.
		 * @return new instance of {@link TerminatingInsert}.
		 * @throws IllegalArgumentException if {@link InsertOptions} is {@literal null}.
		 */
		TerminatingInsert<T> withOptions(InsertOptions insertOptions);
	}

	/**
	 * {@link ExecutableInsert} provides methods for constructing {@code INSERT} operations in a fluent way.
	 */
	interface ExecutableInsert<T> extends TerminatingInsert<T>, InsertWithTable<T>, InsertWithOptions<T> {}
}
