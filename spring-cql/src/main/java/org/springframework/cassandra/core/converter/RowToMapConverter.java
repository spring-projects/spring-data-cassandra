package org.springframework.cassandra.core.converter;

import java.util.HashMap;
import java.util.Map;

import com.datastax.driver.core.*;
import org.springframework.core.convert.converter.Converter;

import com.datastax.driver.core.ColumnDefinitions.Definition;

public class RowToMapConverter implements Converter<Row, Map<String, Object>> {

	@Override
	public Map<String, Object> convert(Row row) {

		if (row == null) {
			return null;
		}

		ColumnDefinitions cols = row.getColumnDefinitions();
		Map<String, Object> map = new HashMap<String, Object>(cols.size());

		for (Definition def : cols.asList()) {

			String name = def.getName();
			//TODO cassandra3
			map.put(
					name,
					row.isNull(name) ? null : CodecRegistry.DEFAULT_INSTANCE.codecFor(def.getType()).deserialize(row.getBytesUnsafe(name), ProtocolVersion.NEWEST_SUPPORTED));
//			map.put(
//					name,
//					row.isNull(name) ? null : def.getType().deserialize(row.getBytesUnsafe(name),
//							ProtocolVersion.NEWEST_SUPPORTED));
		}

		return map;
	}
}
