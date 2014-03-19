package org.springframework.cassandra.core.converter;

import java.math.BigInteger;

public class ResultSetToBigIntegerConverter extends AbstractResultSetToBasicFixedTypeConverter<BigInteger> {

	@Override
	protected BigInteger doConvertSingleValue(Object object) {
		return CONVERTER.convert(object, BigInteger.class);
	}

	@Override
	protected Class<?> getType() {
		return BigInteger.class;
	}
}
