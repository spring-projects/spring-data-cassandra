/*
 * Copyright 2025 the original author or authors.
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

import com.datastax.oss.driver.api.core.cql.Row;

/**
 * Converter for Cassandra query results.
 * <p>
 * This is a functional interface that allows for mapping a {@link Row} to a result type.
 * {@link #mapRow(Row, ConversionResultSupplier) row mapping} can obtain upstream a {@link ConversionResultSupplier
 * upstream converter} to enrich the final result object. This is useful when e.g. wrapping result objects where the
 * wrapper needs to obtain information from the actual {@link Row}.
 *
 * @param <T> object type accepted by this converter.
 * @param <R> the returned result type.
 * @author Mark Paluch
 * @since 5.0
 */
@FunctionalInterface
public interface QueryResultConverter<T, R> {

	/**
	 * Returns a function that returns the materialized entity.
	 *
	 * @param <T> the type of the input and output entity to the function.
	 * @return a function that returns the materialized entity.
	 */
	@SuppressWarnings("unchecked")
	static <T> QueryResultConverter<T, T> entity() {
		return (QueryResultConverter<T, T>) EntityResultConverter.INSTANCE;
	}

	/**
	 * Map a {@link Row} that is read from the Cassandra database to a query result.
	 *
	 * @param row the raw row from the Cassandra result.
	 * @param reader reader object that supplies an upstream result from an earlier converter.
	 * @return the mapped result.
	 */
	R mapRow(Row row, ConversionResultSupplier<T> reader);

	/**
	 * Returns a composed function that first applies this function to its input, and then applies the {@code after}
	 * function to the result. If evaluation of either function throws an exception, it is relayed to the caller of the
	 * composed function.
	 *
	 * @param <V> the type of output of the {@code after} function, and of the composed function.
	 * @param after the function to apply after this function is applied.
	 * @return a composed function that first applies this function and then applies the {@code after} function.
	 */
	default <V> QueryResultConverter<T, V> andThen(QueryResultConverter<? super R, ? extends V> after) {
		return (row, reader) -> after.mapRow(row, () -> mapRow(row, reader));
	}

	/**
	 * A supplier that converts a {@link Row} into {@code T}. Allows for lazy reading of query results.
	 *
	 * @param <T> type of the returned result.
	 */
	interface ConversionResultSupplier<T> {

		/**
		 * Obtain the upstream conversion result.
		 *
		 * @return the upstream conversion result.
		 */
		T get();

	}

}
