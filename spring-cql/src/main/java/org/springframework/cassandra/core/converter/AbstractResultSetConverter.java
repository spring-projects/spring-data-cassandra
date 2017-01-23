/*
 * Copyright 2017 the original author or authors.
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
package org.springframework.cassandra.core.converter;

import java.util.List;
import java.util.Map;

import org.springframework.core.convert.converter.Converter;

import com.datastax.driver.core.ResultSet;

/**
 * Convenient converter that can be used to convert a single-row-single-column, single-row-multi-column, or multi-row
 * {@link ResultSet} into the a value of a given type. The majority of the expected usage is to convert a
 * single-row-single-column result set into the target type.
 * <p/>
 * The algorithm is:
 * <ul>
 * <li>if there is one row with one column, convert that value to this converter's type if possible or throw,</li>
 * <li>else if there is one row with multiple columns, convert the columns into this converter's type if possible or
 * throw,</li>
 * <li>else convert the rows into this converter's type (since there are multiple rows) or throw.</li>
 * </ul>
 * <p/>
 * If the converter throws due to the inability to convert a given {@link ResultSet}, it will throw an
 * {@link IllegalArgumentException}.
 *
 * @author Matthew T. Adams
 * @param <T>
 */
public abstract class AbstractResultSetConverter<T> implements Converter<ResultSet, T> {

	/**
	 * Converts the given value to this converter's type or throws {@link IllegalArgumentException}.
	 */
	protected abstract T doConvertSingleValue(Object object);

	protected abstract Class<?> getType();

	protected ResultSetToListConverter converter = new ResultSetToListConverter();

	protected T getNullResultSetValue() {
		return null;
	}

	protected T getExhaustedResultSetValue() {
		return null;
	}

	@Override
	public T convert(ResultSet source) {

		if (source == null) {
			return getNullResultSetValue();
		}

		if (source.isExhausted()) {
			return getExhaustedResultSetValue();
		}

		List<Map<String, Object>> list = converter.convert(source);

		if (list.size() == 1) {

			Map<String, Object> map = list.get(0);
			return map.size() == 1 ? doConvertSingleValue(map.get(map.keySet().iterator().next())) : doConvertSingleRow(map);
		}

		return doConvertResultSet(list);
	}

	/**
	 * Converts the given result set (as a {@link List}&lt;{@link Map}&lt;String,Object&gt;&gt;) to this converter's type
	 * or throws {@link IllegalArgumentException}. This default implementation simply throws.
	 */
	protected T doConvertResultSet(List<Map<String, Object>> resultSet) {
		doThrow("result set");
		return null;
	}

	/**
	 * Converts the given row (as a {@link Map}&lt;String,Object&gt;) to this converter's type or throws
	 * {@link IllegalArgumentException}. This default implementation simply throws.
	 */
	protected T doConvertSingleRow(Map<String, Object> row) {
		doThrow("row");
		return null;
	}

	protected void doThrow(String string) {
		throw new IllegalArgumentException(String.format("can't convert %s to desired type [%s]", string, getType()
				.getName()));
	}
}
