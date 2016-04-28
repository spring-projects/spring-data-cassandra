package org.springframework.cassandra.core.converter;

import java.util.ArrayList;
import java.util.List;

import org.springframework.core.convert.converter.Converter;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.ColumnDefinitions.Definition;

public class RowToListConverter implements Converter<Row, List<Object>> {

	@Override
	public List<Object> convert(Row row) {

		if (row == null) {
			return null;
		}

		ColumnDefinitions cols = row.getColumnDefinitions();
		List<Object> list = new ArrayList<Object>(cols.size());

		for (Definition def : cols.asList()) {
			String name = def.getName();
			list.add(row.isNull(name) ? null : def.getType().deserialize(
					row.getBytesUnsafe(name), ProtocolVersion.NEWEST_SUPPORTED));
		}

		return list;
	}
}
