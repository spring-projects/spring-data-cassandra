/*
 * Copyright 2016-2025 the original author or authors.
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

import java.util.Map;

import org.springframework.lang.Nullable;
import org.springframework.util.LinkedCaseInsensitiveMap;

import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;

/**
 * {@link RowMapper} implementation that creates a {@code java.util.Map} for each row, representing all columns as
 * key-value pairs: one entry for each column, with the column name as key.
 * <p>
 * The Map implementation to use and the key to use for each column in the column Map can be customized through
 * overriding {@link #createColumnMap} and {@link #getColumnKey}, respectively.
 * <p>
 * <b>Note:</b> By default, ColumnMapRowMapper will try to build a linked Map with case-insensitive keys, to preserve
 * column order as well as allow any casing to be used for column names. This requires Commons Collections on the
 * classpath (which will be autodetected). Else, the fallback is a standard linked HashMap, which will still preserve
 * column order but requires the application to specify the column names in the same casing as exposed by the driver.
 *
 * @author Mark Paluch
 * @since 2.0
 * @see ReactiveCqlTemplate#queryForFlux(String)
 * @see ReactiveCqlTemplate#queryForMap(String)
 */
public class ColumnMapRowMapper implements RowMapper<Map<String, Object>> {

	@Override
	public Map<String, Object> mapRow(Row rs, int rowNum) {

		ColumnDefinitions columnDefinitions = rs.getColumnDefinitions();
		int columnCount = columnDefinitions.size();
		Map<String, Object> mapOfColValues = createColumnMap(columnCount);

		for (int i = 0; i < columnCount; i++) {
			ColumnDefinition columnDefinition = columnDefinitions.get(i);
			String key = getColumnKey(columnDefinition.getName().toString());
			Object obj = getColumnValue(rs, i);
			mapOfColValues.put(key, obj);
		}

		return mapOfColValues;
	}

	/**
	 * Create a {@link Map} instance to be used as column map.
	 * <p>
	 * By default, a linked case-insensitive Map will be created.
	 *
	 * @param columnCount the column count, to be used as initial capacity for the {@link Map}, must not be
	 *          {@literal null}.
	 * @return the new Map instance.
	 * @see org.springframework.util.LinkedCaseInsensitiveMap
	 */
	protected Map<String, Object> createColumnMap(int columnCount) {
		return new LinkedCaseInsensitiveMap<>(columnCount);
	}

	/**
	 * Determine the key to use for the given column in the column Map.
	 *
	 * @param columnName the column name as returned by the {@link Row}, must not be {@literal null}.
	 * @return the column key to use.
	 * @see ColumnDefinitions#get(int)
	 */
	protected String getColumnKey(String columnName) {
		return columnName;
	}

	/**
	 * Retrieve a CQL object value for the specified column.
	 * <p>
	 * The default implementation uses the {@code getObject} method.
	 *
	 * @param row is the {@link Row} holding the data, must not be {@literal null}.
	 * @param index is the column index.
	 * @return the Object returned
	 */
	@Nullable
	protected Object getColumnValue(Row row, int index) {
		return row.getObject(index);
	}
}
