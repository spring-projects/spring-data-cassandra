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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.data.cassandra.core.query.Query;
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * The {@link ReactiveSelectOperation} interface allows creation and execution of Cassandra {@code SELECT} operations in
 * a fluent API style.
 * <p>
 * The starting {@literal domainType} is used for mapping the {@link Query} provided via {@code matching} int the
 * Cassandra specific representation. By default, the originating {@literal domainType} is also used for mapping back
 * the result from the {@link com.datastax.driver.core.Row}. However, it is possible to define an different
 * {@literal returnType} via {@code as} to mapping the result.
 * <p>
 * By default, the table to operate on is derived from the initial {@literal domainType} and can be defined there via
 * the {@link org.springframework.data.cassandra.core.mapping.Table} annotation. Using {@code inTable} allows a
 * developer to override the table name for the execution.
 *
 * <pre>
 *     <code>
 *         query(Human.class)
 *             .inTable("star_wars")
 *             .as(Jedi.class)
 *             .matching(query(where("firstname").is("luke")))
 *             .all();
 *     </code>
 * </pre>
 *
 * @author Mark Paluch
 * @author John Blum
 * @see org.springframework.data.cassandra.core.query.Query
 * @since 2.1
 */
public interface ReactiveSelectOperation {

	/**
	 * Begin creating a {@code SELECT} operation for the given {@link Class domainType}.
	 *
	 * @param <T> {@link Class type} of the application domain object.
	 * @param domainType {@link Class type} of the domain object to query; must not be {@literal null}.
	 * @return new instance of {@link ReactiveSelect}.
	 * @throws IllegalArgumentException if {@link Class domainType} is {@literal null}.
	 * @see ReactiveSelect
	 */
	<T> ReactiveSelect<T> query(Class<T> domainType);

	/**
	 * Table override (optional).
	 */
	interface SelectWithTable<T> extends SelectWithQuery<T> {

		/**
		 * Explicitly set the {@link String name} of the table on which to perform the query.
		 * <p>
		 * Skip this step to use the default table derived from the {@link Class domain type}.
		 *
		 * @param table {@link String name} of the table; must not be {@literal null} or empty.
		 * @return new instance of {@link SelectWithProjection}.
		 * @throws IllegalArgumentException if {@link String table} is {@literal null} or empty.
		 * @see #inTable(CqlIdentifier)
		 * @see SelectWithProjection
		 */
		default SelectWithProjection<T> inTable(String table) {

			Assert.hasText(table, "Table name must not be null or empty");

			return inTable(CqlIdentifier.fromCql(table));
		}

		/**
		 * Explicitly set the {@link CqlIdentifier name} of the table on which to perform the query.
		 * <p>
		 * Skip this step to use the default table derived from the {@link Class domain type}.
		 *
		 * @param table {@link CqlIdentifier name} of the table; must not be {@literal null}.
		 * @return new instance of {@link SelectWithProjection}.
		 * @throws IllegalArgumentException if {@link CqlIdentifier table} is {@literal null}.
		 * @see com.datastax.oss.driver.api.core.CqlIdentifier
		 * @see SelectWithProjection
		 */
		SelectWithProjection<T> inTable(CqlIdentifier table);

	}

	/**
	 * Result type override (optional).
	 */
	interface SelectWithProjection<T> extends SelectWithQuery<T> {

		/**
		 * Define the {@link Class result target type} that the fields should be mapped to.
		 * <p>
		 * Skip this step if you are only interested in the original {@link Class domain type}.
		 *
		 * @param <R> {@link Class type} of the result.
		 * @param resultType desired {@link Class type} of the result; must not be {@literal null}.
		 * @return new instance of {@link SelectWithQuery}.
		 * @throws IllegalArgumentException if {@link Class resultType} is {@literal null}.
		 * @see SelectWithQuery
		 */
		<R> SelectWithQuery<R> as(Class<R> resultType);

	}

	/**
	 * Define a {@link Query} used as the filter for the {@code SELECT}.
	 */
	interface SelectWithQuery<T> extends TerminatingSelect<T> {

		/**
		 * Set the {@link Query} used as a filter in the {@code SELECT} statement.
		 *
		 * @param query {@link Query} used as a filter; must not be {@literal null}.
		 * @return new instance of {@link TerminatingSelect}.
		 * @throws IllegalArgumentException if {@link Query} is {@literal null}.
		 * @see org.springframework.data.cassandra.core.query.Query
		 * @see TerminatingSelect
		 */
		TerminatingSelect<T> matching(Query query);

	}

	/**
	 * Trigger {@code SELECT} execution by calling one of the terminating methods.
	 */
	interface TerminatingSelect<T> {

		/**
		 * Get the number of matching elements.
		 *
		 * @return a {@link Mono} emitting the total number of matching elements; never {@literal null}.
		 * @see reactor.core.publisher.Mono
		 */
		Mono<Long> count();

		/**
		 * Check for the presence of matching elements.
		 *
		 * @return a {@link Mono} emitting {@literal true} if at least one matching element exists; never {@literal null}.
		 * @see reactor.core.publisher.Mono
		 */
		Mono<Boolean> exists();

		/**
		 * Get the first result or no result.
		 *
		 * @return the first result or {@link Mono#empty()} if no match found; never {@literal null}.
		 * @see reactor.core.publisher.Mono
		 */
		Mono<T> first();

		/**
		 * Get exactly zero or one result.
		 *
		 * @return exactly one result or {@link Mono#empty()} if no match found; never {@literal null}.
		 * @throws org.springframework.dao.IncorrectResultSizeDataAccessException if more than one match found.
		 * @see reactor.core.publisher.Mono
		 */
		Mono<T> one();

		/**
		 * Get all matching elements.
		 *
		 * @return all matching elements; never {@literal null}.
		 * @see reactor.core.publisher.Flux
		 */
		Flux<T> all();

	}

	/**
	 * The {@link ReactiveSelect} interface provides methods for constructing {@code SELECT} operations in a fluent way.
	 */
	interface ReactiveSelect<T> extends SelectWithTable<T>, SelectWithProjection<T> {}

}
