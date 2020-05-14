/*
 * Copyright 2017-2020 the original author or authors.
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

/**
 * {@link Converter} from {@link ResultSet} to {@link Map}.
 *
 * @author Mark Paluch
 */
public class ResultSetToListConverter implements Converter<ResultSet, List<Map<String, Object>>> {

	private final Converter<Row, Map<String, Object>> rowConverter;

	/**
	 * Create a new {@link ResultSetToListConverter} using a default {@link RowToMapConverter}.
	 */
	public ResultSetToListConverter() {
		this(RowToMapConverter.INSTANCE);
	}

	/**
	 * Create a new {@link ResultSetToListConverter} given a row to map {@link Converter}.
	 *
	 * @param rowConverter must not be {@literal null}.
	 */
	public ResultSetToListConverter(Converter<Row, Map<String, Object>> rowConverter) {

		Assert.notNull(rowConverter, "Converter must not be null");

		this.rowConverter = rowConverter;
	}

	/* (non-Javadoc)
	 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
	 */
	@Override
	public List<Map<String, Object>> convert(ResultSet resultSet) {

		List<Map<String, Object>> list = new ArrayList<>();
		for (Row row : resultSet) {
			list.add(rowConverter.convert(row));
		}

		return list;
	}
}
