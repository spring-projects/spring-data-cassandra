package org.springframework.cassandra.core.converter;

public class ResultSetToDoubleConverter extends AbstractResultSetToBasicFixedTypeConverter<Double> {

	@Override
	protected Double doConvertSingleValue(Object object) {
		return CONVERTER.convert(object, Double.class);
	}

	@Override
	protected Class<?> getType() {
		return Double.class;
	}
}
