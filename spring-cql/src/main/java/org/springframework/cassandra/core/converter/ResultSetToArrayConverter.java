package org.springframework.cassandra.core.converter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

public class ResultSetToArrayConverter implements Converter<ResultSet, Object[]> {

	protected Converter<Row, Object[]> rowConverter;

	public ResultSetToArrayConverter(Converter<Row, Object[]> rowConverter) {
		setRowConverter(rowConverter);
	}

	public Converter<Row, Object[]> getRowConverter() {
		return rowConverter;
	}

	public void setRowConverter(Converter<Row, Object[]> rowConverter) {

		Assert.notNull(rowConverter);
		this.rowConverter = rowConverter;
	}

	@Override
	public Object[] convert(ResultSet resultSet) {

		if (resultSet == null) {
			return null;
		}

		List<Object[]> list = new ArrayList<Object[]>();
		Iterator<Row> i = resultSet.iterator();
		while (i.hasNext()) {
			list.add(rowConverter.convert(i.next()));
		}

		return list.toArray();
	}
}
