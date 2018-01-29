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

/**
 * {@link ReactiveDeleteOperation} allows creation and execution of Cassandra {@code DELETE} operations in a fluent API
 * style.
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
public interface ReactiveDeleteOperation {

	/**
	 * Start creating a {@code DELETE} operation for the given {@literal domainType}.
	 *
	 * @param domainType must not be {@literal null}.
	 * @return new instance of {@link ReactiveDelete}.
	 * @throws IllegalArgumentException if domainType is {@literal null}.
	 */
	ReactiveDelete delete(Class<?> domainType);

	/**
	 * Table override (optional).
	 */
	interface DeleteWithTable {

		/**
		 * Explicitly set the name of the table to perform the query on.
		 * <p>
		 * Skip this step to use the default table derived from the domain type.
		 *
		 * @param table must not be {@literal null} or empty.
		 * @return new instance of {@link DeleteWithTable}.
		 * @throws IllegalArgumentException if {@code table} is {@literal null} or empty.
		 */
		DeleteWithQuery inTable(String table);

		/**
		 * Explicitly set the name of the table to perform the query on.
		 * <p>
		 * Skip this step to use the default table derived from the domain type.
		 *
		 * @param table must not be {@literal null}.
		 * @return new instance of {@link DeleteWithTable}.
		 * @throws IllegalArgumentException if {@link CqlIdentifier} is {@literal null}.
		 */
		DeleteWithQuery inTable(CqlIdentifier table);
	}

	interface TerminatingDelete {

		/**
		 * Remove all matching rows.
		 *
		 * @return the {@link WriteResult}. Never {@literal null}.
		 */
		Mono<WriteResult> all();
	}

	interface DeleteWithQuery {

		/**
		 * Define the query filtering elements.
		 *
		 * @param query must not be {@literal null}.
		 * @return new instance of {@link TerminatingDelete}.
		 * @throws IllegalArgumentException if query is {@literal null}.
		 */
		TerminatingDelete matching(Query query);
	}

	/**
	 * {@link ReactiveDelete} provides methods for constructing {@code DELETE} operations in a fluent way.
	 */
	interface ReactiveDelete extends DeleteWithTable, DeleteWithQuery {}
}
