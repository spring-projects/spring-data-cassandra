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

public class ResultSetToStringConverter extends AbstractResultSetConverter<String> {

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.converter.AbstractResultSetConverter#doConvertSingleValue(java.lang.Object)
	 */
	@Override
	protected String doConvertSingleValue(Object object) {
		return object == null ? null : object.toString();
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.converter.AbstractResultSetConverter#doConvertSingleRow(java.util.Map)
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
