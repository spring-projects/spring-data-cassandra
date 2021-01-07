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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.datastax.oss.driver.api.core.cql.ResultSet;

import org.springframework.core.convert.converter.Converter;
import org.springframework.util.StringUtils;

/**
 * {@link Converter} from {@link ResultSet} to a {@link List} of {@link String}.
 *
 * @author Mark Paluch
 */
public class ResultSetToListOfStringConverter extends AbstractResultSetConverter<List<String>> {

	public static final ResultSetToListOfStringConverter INSTANCE = new ResultSetToListOfStringConverter();

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.converter.AbstractResultSetConverter#doConvertSingleValue(java.lang.Object)
	 */
	@Override
	protected List<String> doConvertSingleValue(Object object) {
		return Collections.singletonList(object.toString());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.converter.AbstractResultSetConverter#doConvertSingleRow(java.util.Map)
	 */
	@Override
	protected List<String> doConvertSingleRow(Map<String, Object> row) {
		return row.values().stream().map(value -> value == null ? null : value.toString()).collect(Collectors.toList());
	}

	@Override
	protected List<String> doConvertResultSet(List<Map<String, Object>> resultSet) {
		return resultSet.stream().map(row -> doConvertSingleRow(row).toArray()) //
				.map(StringUtils::arrayToCommaDelimitedString) //
				.collect(Collectors.toList());
	}

	@Override
	protected Class<?> getType() {
		return List.class;
	}
}
