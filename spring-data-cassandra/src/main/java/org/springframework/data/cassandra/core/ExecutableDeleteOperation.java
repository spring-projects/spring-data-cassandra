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

import org.springframework.data.cassandra.core.query.Query;
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * {@link ExecutableDeleteOperation} allows creation and execution of Cassandra {@code DELETE} operations in a fluent
 * API style.
 * <p>
 * The starting {@literal domainType} is used for mapping the {@link Query} provided via {@code matching} into the
 * Cassandra specific representation. The table to operate on is by default derived from the initial
 * {@literal domainType} and can be defined there via {@link org.springframework.data.cassandra.core.mapping.Table}.
 * Using {@code inTable} allows to override the table name for the execution.
 *
 * <pre>
 *     <code>
 *         delete(Jedi.class)
 *             .inTable("star_wars")
 *             .matching(query(where("firstname").is("luke")))
 *             .all();
 *     </code>
 * </pre>
 *
 * @author Mark Paluch
 * @since 2.1
 */
public interface ExecutableDeleteOperation {

	/**
	 * Begin creating a {@code DELETE} operation for the given {@link Class domainType}.
	 *
	 * @param domainType {@link Class type} of domain object to delete; must not be {@literal null}.
	 * @return new instance of {@link ExecutableDelete}.
	 * @throws IllegalArgumentException if {@link Class domainType} is {@literal null}.
	 * @see ExecutableDelete
	 */
	ExecutableDelete delete(Class<?> domainType);

	/**
	 * Table override (optional).
	 */
	interface DeleteWithTable {

		/**
		 * Explicitly set the {@link String name} of the table on which to execute the delete.
		 * <p>
		 * Skip this step to use the default table derived from the {@link Class domain type}.
		 *
		 * @param table {@link String name} of the table; must not be {@literal null} or empty.
		 * @return new instance of {@link DeleteWithQuery}.
		 * @throws IllegalArgumentException if {@link String table} is {@literal null} or empty.
		 * @see #inTable(CqlIdentifier)
		 * @see DeleteWithQuery
		 */
		default DeleteWithQuery inTable(String table) {

			Assert.hasText(table, "Table name must not be null or empty");

			return inTable(CqlIdentifier.fromCql(table));
		}

		/**
		 * Explicitly set the {@link CqlIdentifier name} of the table on which to execute the delete.
		 * <p>
		 * Skip this step to use the default table derived from the {@link Class domain type}.
		 *
		 * @param table {@link CqlIdentifier name} of the table; must not be {@literal null}.
		 * @return new instance of {@link DeleteWithQuery}.
		 * @throws IllegalArgumentException if {@link CqlIdentifier table} is {@literal null}.
		 * @see com.datastax.oss.driver.api.core.CqlIdentifier
		 * @see DeleteWithQuery
		 */
		DeleteWithQuery inTable(CqlIdentifier table);

	}

	/**
	 * Filtering (optional).
	 */
	interface DeleteWithQuery {

		/**
		 * Define the {@link Query} filtering elements to delete.
		 *
		 * @param query {@link Query} used to filter the elements to delete; must not be {@literal null}.
		 * @return new instance of {@link TerminatingDelete}.
		 * @throws IllegalArgumentException if {@link Query} is {@literal null}.
		 * @see TerminatingDelete
		 */
		TerminatingDelete matching(Query query);

	}

	/**
	 * Trigger {@code DELETE} execution by calling one of the terminating methods.
	 */
	interface TerminatingDelete {

		/**
		 * Remove all matching rows.
		 *
		 * @return the {@link WriteResult}; never {@literal null}.
		 */
		WriteResult all();

	}

	/**
	 * the {@link ExecutableDelete} interface provides methods for constructing {@code DELETE} operations in a fluent way.
	 */
	interface ExecutableDelete extends DeleteWithTable, DeleteWithQuery {}

}
