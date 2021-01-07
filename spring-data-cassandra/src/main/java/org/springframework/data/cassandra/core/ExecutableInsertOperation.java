/*
 * Copyright 2018-2021 the original author or authors.
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
package org.springframework.data.cassandra.core;

import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.CqlIdentifier;

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
 * @author John Blum
 * @since 2.1
 */
public interface ExecutableInsertOperation {

	/**
	 * Begin creating an {@code INSERT} operation for given {@link Class domainType}.
	 *
	 * @param domainType {@link Class type} of domain object to insert; must not be {@literal null}.
	 * @return new instance of {@link ExecutableInsert}.
	 * @throws IllegalArgumentException if {@link Class domainType} is {@literal null}.
	 * @see ExecutableInsert
	 */
	<T> ExecutableInsert<T> insert(Class<T> domainType);

	/**
	 * Table override (optional).
	 */
	interface InsertWithTable<T> extends InsertWithOptions<T> {

		/**
		 * Explicitly set the {@link String name} of the table.
		 * <p>
		 * Skip this step to use the default table derived from the {@link Class domain type}.
		 *
		 * @param table {@link String name} of the table; must not be {@literal null} or empty.
		 * @return new instance of {@link InsertWithOptions}.
		 * @throws IllegalArgumentException if {@link String table} is {@literal null} or empty.
		 * @see InsertWithOptions
		 */
		default InsertWithOptions<T> inTable(String table) {

			Assert.hasText(table, "Table name must not be null or empty");

			return inTable(CqlIdentifier.fromCql(table));
		}

		/**
		 * Explicitly set the {@link CqlIdentifier name} of the table.
		 * <p>
		 * Skip this step to use the default table derived from the {@link Class domain type}.
		 *
		 * @param table {@link CqlIdentifier name} of the table; must not be {@literal null}.
		 * @return new instance of {@link InsertWithOptions}.
		 * @throws IllegalArgumentException if {@link CqlIdentifier table} is {@literal null}.
		 * @see com.datastax.oss.driver.api.core.CqlIdentifier
		 * @see InsertWithOptions
		 */
		InsertWithOptions<T> inTable(CqlIdentifier table);

	}

	/**
	 * Apply {@link InsertOptions} (optional).
	 */
	interface InsertWithOptions<T> extends TerminatingInsert<T> {

		/**
		 * Set {@link InsertOptions}.
		 *
		 * @param insertOptions {@link InsertOptions} to set; must not be {@literal null}.
		 * @return new instance of {@link TerminatingInsert}.
		 * @throws IllegalArgumentException if {@link InsertOptions} is {@literal null}.
		 * @see org.springframework.data.cassandra.core.InsertOptions
		 * @see TerminatingInsert
		 */
		TerminatingInsert<T> withOptions(InsertOptions insertOptions);

	}

	/**
	 * Trigger {@code INSERT} execution by calling one of the terminating methods.
	 */
	interface TerminatingInsert<T> {

		/**
		 * Insert exactly one {@link Object}.
		 *
		 * @param object {@link Object} to insert; must not be {@literal null}.
		 * @return the {@link EntityWriteResult} for this operation.
		 * @throws IllegalArgumentException if {@link Object} is {@literal null}.
		 * @see org.springframework.data.cassandra.core.EntityWriteResult
		 */
		EntityWriteResult<T> one(T object);

	}

	/**
	 * The {@link ExecutableInsert} interface provides methods for constructing {@code INSERT} operations in a fluent way.
	 */
	interface ExecutableInsert<T> extends InsertWithTable<T> {}

}
