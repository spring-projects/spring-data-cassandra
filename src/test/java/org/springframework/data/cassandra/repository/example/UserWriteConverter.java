package org.springframework.data.cassandra.repository.example;

import java.util.HashMap;
import java.util.Map;

import org.json.simple.JSONValue;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.cassandra.core.CassandraDataObject;

public class UserWriteConverter implements Converter<User, CassandraDataObject> {

	public CassandraDataObject convert(User source) {
		String json = JSONValue.toJSONString(source);

		CassandraDataObject dataObject = new CassandraDataObject();
		Map<String,String> columns = new HashMap<String,String>();
		columns.put("DATA", json);
		
		dataObject.putRow(source.getUserId(), columns);
		return dataObject;
	}
}
