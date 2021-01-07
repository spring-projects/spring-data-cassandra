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

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.springframework.data.cassandra.core.query.Query;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * The {@link ExecutableSelectOperation} interface allows creation and execution of Cassandra {@code SELECT} operations
 * in a fluent API style.
 * <p>
 * The starting {@literal domainType} is used for mapping the {@link Query} provided via {@code matching} into the
 * Cassandra-specific representation. By default, the originating {@literal domainType} is also used for mapping back
 * the result from the {@link com.datastax.driver.core.Row}. However, it is possible to define an different
 * {@literal returnType} via {@code as} for mapping the result.
 * <p>
 * By default, the table to operate on is derived from the initial {@literal domainType} and can be defined there with
 * the {@link org.springframework.data.cassandra.core.mapping.Table} annotation as well. Using {@code inTable} allows a
 * user to override the table name for the execution.
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
public interface ExecutableSelectOperation {

	/**
	 * Begin creating a Cassandra {@code SELECT} query operation for the given {@link Class domainType}.
	 *
	 * @param <T> {@link Class type} of the application domain object.
	 * @param domainType {@link Class type} to domain object to query; must not be {@literal null}.
	 * @return new instance of {@link ExecutableSelect}.
	 * @throws IllegalArgumentException if {@link Class domainType} is {@literal null}.
	 * @see ExecutableSelect
	 */
	<T> ExecutableSelect<T> query(Class<T> domainType);

	/**
	 * Table override (optional).
	 */
	interface SelectWithTable<T> extends SelectWithQuery<T> {

		/**
		 * Explicitly set the {@link String name} of the table on which to execute the query.
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
		 * Explicitly set the {@link CqlIdentifier name} of the table on which to execute the query.
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
		 * Define the {@link Class result target type} that the Cassandra Row fields should be mapped to.
		 * <p>
		 * Skip this step if you are anyway only interested in the original {@link Class domain type}.
		 *
		 * @param <R> {@link Class type} of the result.
		 * @param resultType desired {@link Class target type} of the result; must not be {@literal null}.
		 * @return new instance of {@link SelectWithQuery}.
		 * @throws IllegalArgumentException if resultType is {@literal null}.
		 * @see SelectWithQuery
		 */
		<R> SelectWithQuery<R> as(Class<R> resultType);

	}

	/**
	 * Filtering (optional).
	 */
	interface SelectWithQuery<T> extends TerminatingSelect<T> {

		/**
		 * Set the {@link Query} to use as a filter.
		 *
		 * @param query {@link Query} used as a filter; must not be {@literal null}.
		 * @return new instance of {@link TerminatingSelect}.
		 * @throws IllegalArgumentException if {@link Query} is {@literal null}.
		 * @see TerminatingSelect
		 */
		TerminatingSelect<T> matching(Query query);

	}

	/**
	 * Trigger {@code SELECT} query execution by calling one of the terminating methods.
	 */
	interface TerminatingSelect<T> {

		/**
		 * Get the number of matching elements.
		 *
		 * @return total number of matching elements.
		 */
		long count();

		/**
		 * Check for the presence of matching elements.
		 *
		 * @return {@literal true} if at least one matching element exists.
		 * @see #count()
		 */
		default boolean exists() {
			return count() > 0;
		}

		/**
		 * Get the first result, or no result.
		 *
		 * @return the first result or {@link Optional#empty()} if no match found.
		 * @see #firstValue()
		 */
		default Optional<T> first() {
			return Optional.ofNullable(firstValue());
		}

		/**
		 * Get the first result, or no result.
		 *
		 * @return the first result or {@literal null} if no match found.
		 */
		@Nullable
		T firstValue();

		/**
		 * Get exactly zero or one result.
		 *
		 * @return a single result or {@link Optional#empty()} if no match found.
		 * @throws org.springframework.dao.IncorrectResultSizeDataAccessException if more than one match found.
		 * @see #oneValue()
		 */
		default Optional<T> one() {
			return Optional.ofNullable(oneValue());
		}

		/**
		 * Get exactly zero or one result.
		 *
		 * @return the single result or {@literal null} if no match found.
		 * @throws org.springframework.dao.IncorrectResultSizeDataAccessException if more than one match found.
		 */
		@Nullable
		T oneValue();

		/**
		 * Get all matching elements.
		 *
		 * @return a {@link List} of all the matching elements; never {@literal null}.
		 * @see java.util.List
		 */
		List<T> all();

		/**
		 * Stream all matching elements.
		 *
		 * @return a {@link Stream} wrapping the Cassandra {@link com.datastax.driver.core.ResultSet}, which needs to be
		 *         closed; never {@literal null}.
		 * @see java.util.stream.Stream
		 * @see #all()
		 */
		default Stream<T> stream() {
			return all().stream();
		}
	}

	/**
	 * The {@link ExecutableSelect} interface provides methods for constructing {@code SELECT} query operations in a
	 * fluent way.
	 */
	interface ExecutableSelect<T> extends SelectWithTable<T>, SelectWithProjection<T> {}

}
