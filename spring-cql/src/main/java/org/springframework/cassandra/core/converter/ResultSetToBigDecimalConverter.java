package org.springframework.cassandra.core.converter;

import java.math.BigDecimal;

public class ResultSetToBigDecimalConverter extends AbstractResultSetToBasicFixedTypeConverter<BigDecimal> {

	@Override
	protected BigDecimal doConvertSingleValue(Object object) {
		return CONVERTER.convert(object, BigDecimal.class);
	}

	@Override
	protected Class<?> getType() {
		return BigDecimal.class;
	}
}
