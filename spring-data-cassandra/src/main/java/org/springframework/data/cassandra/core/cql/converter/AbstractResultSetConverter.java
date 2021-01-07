/*
 * Copyright 2017-2021 the original author or authors.
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
package org.springframework.data.cassandra.core.cql.converter;

import java.util.List;
import java.util.Map;

import com.datastax.oss.driver.api.core.cql.ResultSet;

import org.springframework.core.convert.converter.Converter;
import org.springframework.lang.Nullable;

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
 * @author Mark Paluch
 * @param <T>
 */
public abstract class AbstractResultSetConverter<T> implements Converter<ResultSet, T> {

	private static final ResultSetToListConverter converter = new ResultSetToListConverter();

	/**
	 * Converts the given value to this converter's type or throws {@link IllegalArgumentException}.
	 */
	protected abstract T doConvertSingleValue(Object object);

	/**
	 * @return the target type.
	 */
	protected abstract Class<?> getType();

	/**
	 * @return surrogate value if the {@link ResultSet} is {@literal null}.
	 */
	@Nullable
	protected T getNullResultSetValue() {
		return null;
	}

	/**
	 * @return surrogate value if the {@link ResultSet} is {@link ResultSet#isExhausted() exhausted}.
	 */
	@Nullable
	protected T getExhaustedResultSetValue() {
		return null;
	}

	@Override
	public T convert(ResultSet source) {

		if (source.getAvailableWithoutFetching() == 0 && source.isFullyFetched()) {
			return getExhaustedResultSetValue();
		}

		List<Map<String, Object>> list = converter.convert(source);

		if (list == null) {
			return getNullResultSetValue();
		}

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

		throw new IllegalArgumentException(
				String.format("Cannot convert %s to desired type [%s]", "result set", getType().getName()));
	}

	/**
	 * Converts the given row (as a {@link Map}&lt;String,Object&gt;) to this converter's type or throws
	 * {@link IllegalArgumentException}. This default implementation simply throws.
	 */
	protected T doConvertSingleRow(Map<String, Object> row) {

		throw new IllegalArgumentException(
				String.format("Cannot convert %s to desired type [%s]", "row", getType().getName()));
	}
}
