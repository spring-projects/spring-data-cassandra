/*
 * Copyright 2016-2018 the original author or authors.
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
package org.springframework.data.cassandra.core.cql;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.UUID;

import org.springframework.lang.Nullable;

import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.UDTValue;

/**
 * Generic utility methods for working with Cassandra. Mainly for internal use within the framework, but also useful for
 * custom CQL access code.
 *
 * @author Mark Paluch
 * @since 2.0
 */
public abstract class RowUtils {

	private RowUtils() {}

	/**
	 * Retrieve a CQL column value from a {@link Row}, using the specified value type.
	 * <p>
	 * Uses the specifically typed {@link Row} accessor methods, falling back to {@link Row#getObject(int)} for unknown
	 * types.
	 * <p>
	 * Note that the returned value may not be assignable to the specified required type, in case of an unknown type.
	 * Calling code needs to deal with this case appropriately, e.g. throwing a corresponding exception.
	 *
	 * @param row is the {@link Row} holding the data
	 * @param index is the column index
	 * @param requiredType the required value type (may be {@literal null})
	 * @return the value object
	 */
	@Nullable
	public static Object getRowValue(Row row, int index, @Nullable Class<?> requiredType) {

		if (requiredType == null) {
			return row.getObject(index);
		}

		Object value;

		// Explicitly extract typed value, as far as possible.
		if (String.class == requiredType) {
			return row.getString(index);
		} else if (boolean.class == requiredType || Boolean.class == requiredType) {
			value = row.getBool(index);
		} else if (byte.class == requiredType || Byte.class == requiredType) {
			value = row.getByte(index);
		} else if (short.class == requiredType || Short.class == requiredType) {
			value = row.getShort(index);
		} else if (int.class == requiredType || Integer.class == requiredType) {
			value = row.getInt(index);
		} else if (long.class == requiredType || Long.class == requiredType) {
			value = row.getLong(index);
		} else if (float.class == requiredType || Float.class == requiredType) {
			value = row.getFloat(index);
		} else if (double.class == requiredType || Double.class == requiredType || Number.class == requiredType) {
			value = row.getDouble(index);
		} else if (BigDecimal.class == requiredType) {
			return row.getDecimal(index);
		} else if (LocalDate.class == requiredType) {
			return row.getDate(index);
		} else if (java.util.Date.class == requiredType) {
			return row.getTimestamp(index);
		} else if (ByteBuffer.class == requiredType) {
			return row.getBytes(index);
		} else if (TupleValue.class == requiredType) {
			return row.getTupleValue(index);
		} else if (UDTValue.class == requiredType) {
			return row.getUDTValue(index);
		} else if (UUID.class == requiredType) {
			return row.getUUID(index);
		} else {
			// Some unknown type desired -> rely on getObject.
			return row.getObject(index);
		}

		return (row.isNull(index) ? null : value);
	}
}
