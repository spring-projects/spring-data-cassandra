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

import reactor.core.publisher.Mono;

import org.springframework.data.cassandra.core.cql.CqlIdentifier;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.cassandra.core.query.Update;

/**
 * {@link ReactiveUpdateOperation} allows creation and execution of Cassandra {@code UPDATE} operation in a fluent API
 * style.
 * <p>
 * The starting {@literal domainType} is used for mapping the {@link Query} provided via {@code matching}, as well as
 * the {@link Update} via {@code apply} into the Cassandra specific representations. The table to operate on is by
 * default derived from the initial {@literal domainType} and can be defined there via
 * {@link org.springframework.data.cassandra.core.mapping.Table}. Using {@code inTable} allows to override the table
 * name for the execution.
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
 * @since 2.1
 */
public interface ReactiveUpdateOperation {

	/**
	 * Start creating an {@code UPDATE} operation for the given {@literal domainType}.
	 *
	 * @param domainType must not be {@literal null}.
	 * @return new instance of {@link ReactiveUpdate}.
	 * @throws IllegalArgumentException if domainType is {@literal null}.
	 */
	<T> ReactiveUpdate<T> update(Class<T> domainType);

	/**
	 * Declare the {@link Update} to apply.
	 */
	interface UpdateWithUpdate<T> {

		/**
		 * Set the {@link Update} to be applied.
		 *
		 * @param update must not be {@literal null}.
		 * @return new instance of {@link TerminatingUpdate}.
		 * @throws IllegalArgumentException if update is {@literal null}.
		 */
		TerminatingUpdate<T> apply(Update update);
	}

	/**
	 * Explicitly define the name of the table to perform operation in.
	 */
	interface UpdateWithTable<T> {

		/**
		 * Explicitly set the name of the table to perform the query on.
		 * <p>
		 * Skip this step to use the default table derived from the domain type.
		 *
		 * @param table must not be {@literal null} or empty.
		 * @return new instance of {@link UpdateWithTable}.
		 * @throws IllegalArgumentException if {@code table} is {@literal null} or empty.
		 */
		UpdateWithQuery<T> inTable(String table);

		/**
		 * Explicitly set the name of the table to perform the query on.
		 * <p>
		 * Skip this step to use the default table derived from the domain type.
		 *
		 * @param table must not be {@literal null}.
		 * @return new instance of {@link UpdateWithTable}.
		 * @throws IllegalArgumentException if {@link CqlIdentifier} is {@literal null}.
		 */
		UpdateWithQuery<T> inTable(CqlIdentifier table);
	}

	/**
	 * Define a filter query for the {@link Update}.
	 */
	interface UpdateWithQuery<T> {

		/**
		 * Filter documents by given {@literal query}.
		 *
		 * @param query must not be {@literal null}.
		 * @return new instance of {@link UpdateWithQuery}.
		 * @throws IllegalArgumentException if query is {@literal null}.
		 */
		UpdateWithUpdate<T> matching(Query query);
	}

	/**
	 * Trigger update execution by calling one of the terminating methods.
	 */
	interface TerminatingUpdate<T> {

		/**
		 * Update all matching rows in the table.
		 *
		 * @return never {@literal null}.
		 */
		Mono<WriteResult> all();
	}

	/**
	 * {@link ReactiveUpdate} provides methods for constructing {@code UPDATE} operations in a fluent way.
	 */
	interface ReactiveUpdate<T> extends UpdateWithTable<T>, UpdateWithQuery<T> {}
}
