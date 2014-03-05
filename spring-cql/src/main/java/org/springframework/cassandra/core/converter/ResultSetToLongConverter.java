package org.springframework.cassandra.core.converter;

public class ResultSetToLongConverter extends AbstractResultSetToBasicFixedTypeConverter<Long> {

	@Override
	protected Long doConvertSingleValue(Object object) {
		return CONVERTER.convert(object, Long.class);
	}

	@Override
	protected Class<?> getType() {
		return Long.class;
	}
}
