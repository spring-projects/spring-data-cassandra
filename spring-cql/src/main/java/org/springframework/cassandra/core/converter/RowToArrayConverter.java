package org.springframework.cassandra.core.converter;

import java.util.List;

import org.springframework.core.convert.converter.Converter;

import com.datastax.driver.core.Row;

public class RowToArrayConverter implements Converter<Row, Object[]> {

	protected RowToListConverter delegate = new RowToListConverter();

	@Override
	public Object[] convert(Row row) {

		List<Object> list = delegate.convert(row);
		return list == null ? null : list.toArray();
	}
}
