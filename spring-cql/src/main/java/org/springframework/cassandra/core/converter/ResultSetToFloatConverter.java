package org.springframework.cassandra.core.converter;

public class ResultSetToFloatConverter extends AbstractResultSetToBasicFixedTypeConverter<Float> {

	@Override
	protected Float doConvertSingleValue(Object object) {
		return CONVERTER.convert(object, Float.class);
	}

	@Override
	protected Class<?> getType() {
		return Float.class;
	}
}
