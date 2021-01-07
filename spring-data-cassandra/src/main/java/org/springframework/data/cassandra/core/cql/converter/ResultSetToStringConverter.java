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

/**
 * {@link Converter} from {@link ResultSet} to a single {@link String} value.
 *
 * @author Mark Paluch
 */
public class ResultSetToStringConverter extends AbstractResultSetConverter<String> {

	public static final ResultSetToStringConverter INSTANCE = new ResultSetToStringConverter();

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.converter.AbstractResultSetConverter#doConvertSingleValue(java.lang.Object)
	 */
	@Override
	protected String doConvertSingleValue(Object object) {
		return object.toString();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.converter.AbstractResultSetConverter#doConvertSingleRow(java.util.Map)
	 */
	@Override
	protected String doConvertSingleRow(Map<String, Object> row) {

		StringBuilder s = new StringBuilder();

		boolean firstEntry = true;

		for (Map.Entry<String, Object> entry : row.entrySet()) {

			if (firstEntry) {
				firstEntry = false;
			} else {
				s.append(", ");
			}

			s.append("\"").append(entry.getKey().replaceAll("\"", "\\\"")).append("\"");
			s.append(" : ");
			s.append("\"").append(entry.getValue().toString().replaceAll("\"", "\\\"")).append("\"");
		}

		return s.toString();
	}

	@Override
	protected String doConvertResultSet(List<Map<String, Object>> resultSet) {

		boolean firstElement = true;
		StringBuilder s = new StringBuilder("{ ");

		for (Map<String, Object> map : resultSet) {

			if (firstElement) {
				firstElement = false;
			} else {
				s.append(", ");
			}

			s.append(doConvertSingleRow(map));
		}
		s.append(" }");

		return s.toString();
	}

	@Override
	protected Class<?> getType() {
		return String.class;
	}
}
