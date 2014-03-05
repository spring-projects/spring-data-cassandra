package org.springframework.cassandra.core.converter;

import java.util.Date;

public class ResultSetToDateConverter extends AbstractResultSetToBasicFixedTypeConverter<Date> {

	@Override
	protected Date doConvertSingleValue(Object object) {
		return CONVERTER.convert(object, Date.class);
	}

	@Override
	protected Class<?> getType() {
		return Date.class;
	}
}
