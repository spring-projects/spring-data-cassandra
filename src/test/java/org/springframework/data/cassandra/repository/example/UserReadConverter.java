package org.springframework.data.cassandra.repository.example;

import java.util.Map;

import org.json.simple.JSONValue;
import org.springframework.core.convert.ConversionException;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.cassandra.core.CassandraDataObject;

public class UserReadConverter implements Converter<CassandraDataObject, User> {
	public User convert(CassandraDataObject source) throws ConversionException {
		User user = new User();
		// For now, assume single row per entity
		Map<String,String> columns = source.getOnlyRow();
		
		// Let's pretend all fields are stored in a JSON object in a column called "DATA"
		Map<String, String> dataValues = (Map<String, String>) JSONValue.parse(columns.get("DATA"));
		user.setFirstName(dataValues.get("first_name"));
		user.setLastName(dataValues.get("last_name"));
		user.setCity(dataValues.get("city"));
		return user;
	}
}
