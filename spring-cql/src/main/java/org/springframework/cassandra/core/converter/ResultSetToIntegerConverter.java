package org.springframework.cassandra.core.converter;

public class ResultSetToIntegerConverter extends AbstractResultSetToBasicFixedTypeConverter<Integer> {

	@Override
	protected Integer doConvertSingleValue(Object object) {
		return CONVERTER.convert(object, Integer.class);
	}

	@Override
	protected Class<?> getType() {
		return Integer.class;
	}
}
