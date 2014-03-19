package org.springframework.cassandra.core.converter;

import java.util.List;
import java.util.Map;

public class ResultSetToStringConverter extends AbstractResultSetConverter<String> {

	@Override
	protected String doConvertSingleValue(Object object) {
		return object == null ? null : object.toString();
	}

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
