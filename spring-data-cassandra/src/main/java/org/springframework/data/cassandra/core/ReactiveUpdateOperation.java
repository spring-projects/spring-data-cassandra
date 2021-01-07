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

import reactor.core.publisher.Mono;

import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.cassandra.core.query.Update;
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * The {@link ReactiveUpdateOperation} interface allows creation and execution of Cassandra {@code UPDATE} operations in
 * a fluent API style.
 * <p>
 * The starting {@literal domainType} is used for mapping the {@link Query} provided via {@code matching}, as well as
 * the {@link Update} via {@code apply} into the Cassandra specific representations.
 * <p>
 * By default, the table to operate on is derived from the initial {@literal domainType} and can be defined there via
 * the {@link org.springframework.data.cassandra.core.mapping.Table} annotation. Using {@code inTable} allows a
 * developer to override the table name for the execution.
 *
 * <pre>
 *     <code>
 *         update(Jedi.class)
 *             .inTable("star_wars")
 *             .matching(query(where("firstname").is("luke")))
 *             .apply(update("lastname", "skywalker"))
 *             .all();
 *     </code>
 * </pre>
 *
 * @author Mark Paluch
 * @author John Blum
 * @see org.springframework.data.cassandra.core.query.Query
 * @see org.springframework.data.cassandra.core.query.Update
 * @since 2.1
 */
public interface ReactiveUpdateOperation {

	/**
	 * Begin creating an {@code UPDATE} operation for the given {@link Class domainType}.
	 *
	 * @param domainType {@link Class type} of domain object to update; must not be {@literal null}.
	 * @return new instance of {@link ReactiveUpdate}.
	 * @throws IllegalArgumentException if {@link Class domainType} is {@literal null}.
	 * @see ReactiveUpdate
	 */
	ReactiveUpdate update(Class<?> domainType);

	/**
	 * Table override (optional).
	 */
	interface UpdateWithTable {

		/**
		 * Explicitly set the {@link String name} of the table on which to perform the update.
		 * <p>
		 * Skip this step to use the default table derived from the {@link Class domain type}.
		 *
		 * @param table {@link String name} of the table; must not be {@literal null} or empty.
		 * @return new instance of {@link UpdateWithQuery}.
		 * @throws IllegalArgumentException if {@link String table} is {@literal null} or empty.
		 * @see #inTable(CqlIdentifier)
		 * @see UpdateWithQuery
		 */
		default UpdateWithQuery inTable(String table) {

			Assert.hasText(table, "Table name must not be null or empty");

			return inTable(CqlIdentifier.fromCql(table));
		}

		/**
		 * Explicitly set the {@link CqlIdentifier name} of the table to on which to perform the update.
		 * <p>
		 * Skip this step to use the default table derived from the {@link Class domain type}.
		 *
		 * @param table {@link CqlIdentifier name} of the table; must not be {@literal null}.
		 * @return new instance of {@link UpdateWithQuery}.
		 * @throws IllegalArgumentException if {@link CqlIdentifier table} is {@literal null}.
		 * @see com.datastax.oss.driver.api.core.CqlIdentifier
		 * @see UpdateWithQuery
		 */
		UpdateWithQuery inTable(CqlIdentifier table);

	}

	/**
	 * Define a {@link Query} used as the filter for the {@link Update}.
	 */
	interface UpdateWithQuery {

		/**
		 * Filter rows to update by the given {@link Query}.
		 *
		 * @param query {@link Query} used as a filter in the update; must not be {@literal null}.
		 * @return new instance of {@link TerminatingUpdate}.
		 * @throws IllegalArgumentException if {@link Query} is {@literal null}.
		 * @see org.springframework.data.cassandra.core.query.Query
		 * @see TerminatingUpdate
		 */
		TerminatingUpdate matching(Query query);

	}

	/**
	 * Trigger {@code UPDATE} execution by calling one of the terminating methods.
	 */
	interface TerminatingUpdate {

		/**
		 * Update all matching rows in the table.
		 *
		 * @return the {@link WriteResult} of the update; never {@literal null}.
		 * @see org.springframework.data.cassandra.core.query.Update
		 * @see org.springframework.data.cassandra.core.WriteResult
		 * @see reactor.core.publisher.Mono
		 */
		Mono<WriteResult> apply(Update update);

	}

	/**
	 * The {@link ReactiveUpdate} interface provides methods for constructing {@code UPDATE} operations in a fluent way.
	 */
	interface ReactiveUpdate extends UpdateWithTable, UpdateWithQuery {}

}
