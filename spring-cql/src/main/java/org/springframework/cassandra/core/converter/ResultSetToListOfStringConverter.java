package org.springframework.cassandra.core.converter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.util.StringUtils;

public class ResultSetToListOfStringConverter extends AbstractResultSetConverter<List<String>> {

	@Override
	protected List<String> doConvertSingleValue(Object object) {

		List<String> list = new ArrayList<String>();

		list.add(object == null ? null : object.toString());
		return list;
	}

	@Override
	protected List<String> doConvertSingleRow(Map<String, Object> row) {

		List<String> list = new ArrayList<String>(row.size());

		for (Object value : row.values()) {
			list.add(value == null ? null : value.toString());
		}

		return list;
	}

	@Override
	protected List<String> doConvertResultSet(List<Map<String, Object>> resultSet) {

		List<String> list = new ArrayList<String>(resultSet.size());

		for (Map<String, Object> row : resultSet) {
			list.add(StringUtils.arrayToCommaDelimitedString(doConvertSingleRow(row).toArray()));
		}

		return list;
	}

	@Override
	protected Class<?> getType() {
		return List.class;
	}
}
