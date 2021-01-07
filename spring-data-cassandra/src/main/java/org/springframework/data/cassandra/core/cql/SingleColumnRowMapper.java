/*
 * Copyright 2016-2021 the original author or authors.
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
package org.springframework.data.cassandra.core.cql;

import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;

import org.springframework.dao.TypeMismatchDataAccessException;
import org.springframework.lang.Nullable;
import org.springframework.util.ClassUtils;
import org.springframework.util.NumberUtils;

/**
 * {@link RowMapper} implementation that converts a single column into a single result value per row. Expects to operate
 * on a {@link com.datastax.driver.core.Row} that just contains a single column.
 * <p>
 * The type of the result value for each row can be specified. The value for the single column will be extracted from a
 * {@link Row} and converted into the specified target type.
 *
 * @author Mark Paluch
 * @since 2.0
 * @see AsyncCqlTemplate#queryForList(String, Class)
 * @see AsyncCqlTemplate#queryForObject(String, Class)
 * @see CqlTemplate#queryForList(String, Class)
 * @see CqlTemplate#queryForObject(String, Class)
 * @see ReactiveCqlTemplate#queryForFlux(String, Class)
 * @see ReactiveCqlTemplate#queryForObject(String, Class)
 */
public class SingleColumnRowMapper<T> implements RowMapper<T> {

	private @Nullable Class<?> requiredType;

	/**
	 * Create a new {@link SingleColumnRowMapper} for bean-style configuration.
	 *
	 * @see #setRequiredType
	 */
	public SingleColumnRowMapper() {}

	/**
	 * Create a new {@code SingleColumnRowMapper}.
	 * <p>
	 * Consider using the {@link #newInstance} factory method instead, which allows for specifying the required type once
	 * only.
	 *
	 * @param requiredType the type that each result object is expected to match
	 */
	public SingleColumnRowMapper(Class<T> requiredType) {
		setRequiredType(requiredType);
	}

	/**
	 * Set the type that each result object is expected to match.
	 * <p>
	 * If not specified, the column value will be exposed as returned by the {@link Row}.
	 */
	public void setRequiredType(Class<T> requiredType) {
		this.requiredType = ClassUtils.resolvePrimitiveIfNecessary(requiredType);
	}

	/**
	 * Extract a value for the single column in the current row.
	 * <p>
	 * Validates that there is only one column selected, then delegates to {@code getColumnValue()} and also
	 * {@code convertValueToRequiredType}, if necessary.
	 *
	 * @see ColumnDefinitions#size()
	 * @see #getColumnValue(Row, int, Class)
	 * @see #convertValueToRequiredType(Object, Class)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public T mapRow(Row row, int rowNum) throws DriverException {

		// Validate column count.
		ColumnDefinitions definitions = row.getColumnDefinitions();
		int nrOfColumns = definitions.size();

		if (nrOfColumns != 1) {
			throw new IncorrectResultSetColumnCountException(1, nrOfColumns);
		}

		// Extract column value from CQL ResultSet.
		Object result = getColumnValue(row, 0, this.requiredType);

		if (result != null && this.requiredType != null && !this.requiredType.isInstance(result)) {
			// Extracted value does not match already: try to convert it.
			try {
				return (T) convertValueToRequiredType(result, this.requiredType);
			} catch (IllegalArgumentException ex) {

				ColumnDefinition columnDefinition = definitions.get(0);

				throw new TypeMismatchDataAccessException(
						String.format("Type mismatch affecting row number %d and column type '%s': %s", rowNum,
								columnDefinition.getType(), ex.getMessage()));
			}
		}

		return (T) result;
	}

	/**
	 * Retrieve a CQL object value for the specified column.
	 * <p>
	 * The default implementation calls {@link RowUtils#getRowValue(Row, int, Class)}. If no required type has been
	 * specified, this method delegates to {@code getColumnValue(rs, index)}, which basically calls
	 * {@link Row#getObject(int)} but applies some additional default conversion to appropriate value types.
	 *
	 * @param row is the {@link Row} holding the data, must not be {@literal null}.
	 * @param index is the column index
	 * @param requiredType the type that each result object is expected to match (or {@literal null} if none specified).
	 * @return the Object value.
	 * @throws DriverException in case of extraction failure
	 * @see RowUtils#getRowValue(Row, int, Class)
	 * @see #getColumnValue(Row, int)
	 */
	@Nullable
	protected Object getColumnValue(Row row, int index, @Nullable Class<?> requiredType) throws DriverException {

		if (requiredType != null) {
			return RowUtils.getRowValue(row, index, requiredType);
		} else {
			// No required type specified -> perform default extraction.
			return getColumnValue(row, index);
		}
	}

	/**
	 * Retrieve a object value for the specified column, using the most appropriate value type. Called if no required type
	 * has been specified.
	 * <p>
	 * The default implementation delegates to {@link RowUtils#getRowValue(Row, int, Class)}, which uses the
	 * {@link Row#getObject(int)} method.
	 *
	 * @param row is the {@link Row} holding the data, must not be {@literal null}.
	 * @param index is the column index
	 * @return the Object value.
	 * @throws DriverException in case of extraction failure.
	 * @see RowUtils#getRowValue(Row, int, Class)
	 */
	@Nullable
	protected Object getColumnValue(Row row, int index) {
		return RowUtils.getRowValue(row, index, null);
	}

	/**
	 * Convert the given column value to the specified required type. Only called if the extracted column value does not
	 * match already.
	 * <p>
	 * If the required type is String, the value will simply get stringified via {@code toString()}. In case of a Number,
	 * the value will be converted into a Number, either through number conversion or through String parsing (depending on
	 * the value type).
	 *
	 * @param value the column value as extracted from {@code getColumnValue()} (never {@literal null})
	 * @param requiredType the type that each result object is expected to match (never {@literal null})
	 * @return the converted value
	 * @see #getColumnValue(Row, int, Class)
	 */
	@SuppressWarnings("unchecked")
	protected Object convertValueToRequiredType(Object value, Class<?> requiredType) {

		if (String.class == requiredType) {
			return value.toString();
		} else if (Number.class.isAssignableFrom(requiredType)) {
			if (value instanceof Number) {
				// Convert original Number to target Number class.
				return NumberUtils.convertNumberToTargetClass(((Number) value), (Class<Number>) requiredType);
			} else {
				// Convert stringified value to target Number class.
				return NumberUtils.parseNumber(value.toString(), (Class<Number>) requiredType);
			}
		} else {
			throw new IllegalArgumentException(
					String.format("Value [%s] is of type [%s] and cannot be converted to required type [%s]", value,
							value.getClass().getName(), requiredType.getName()));
		}
	}

	/**
	 * Static factory method to create a new {@code SingleColumnRowMapper} (with the required type specified only once).
	 *
	 * @param requiredType the type that each result object is expected to match
	 */
	public static <T> SingleColumnRowMapper<T> newInstance(Class<T> requiredType) {
		return new SingleColumnRowMapper<>(requiredType);
	}
}
