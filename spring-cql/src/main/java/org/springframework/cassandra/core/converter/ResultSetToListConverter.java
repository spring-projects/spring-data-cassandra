package org.springframework.cassandra.core.converter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

public class ResultSetToListConverter implements Converter<ResultSet, List<Map<String, Object>>> {

	protected Converter<Row, Map<String, Object>> rowConverter = new RowToMapConverter();

	public ResultSetToListConverter() {}

	public ResultSetToListConverter(Converter<Row, Map<String, Object>> rowConverter) {
		setRowConverter(rowConverter);
	}

	public Converter<Row, Map<String, Object>> getRowConverter() {
		return rowConverter;
	}

	public void setRowConverter(Converter<Row, Map<String, Object>> rowConverter) {

		Assert.notNull(rowConverter);
		this.rowConverter = rowConverter;
	}

	@Override
	public List<Map<String, Object>> convert(ResultSet resultSet) {

		if (resultSet == null) {
			return null;
		}

		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		Iterator<Row> i = resultSet.iterator();
		while (i.hasNext()) {
			list.add(rowConverter.convert(i.next()));
		}

		return list;
	}
}
