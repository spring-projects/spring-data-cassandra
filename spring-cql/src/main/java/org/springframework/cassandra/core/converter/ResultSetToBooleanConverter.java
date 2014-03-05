package org.springframework.cassandra.core.converter;

public class ResultSetToBooleanConverter extends AbstractResultSetToBasicFixedTypeConverter<Boolean> {

	@Override
	protected Boolean doConvertSingleValue(Object object) {
		return CONVERTER.convert(object, Boolean.class);
	}

	@Override
	protected Class<?> getType() {
		return Boolean.class;
	}
}
