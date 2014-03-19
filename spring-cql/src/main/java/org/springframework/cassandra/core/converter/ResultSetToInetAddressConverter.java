package org.springframework.cassandra.core.converter;

import java.net.InetAddress;

public class ResultSetToInetAddressConverter extends AbstractResultSetToBasicFixedTypeConverter<InetAddress> {

	@Override
	protected InetAddress doConvertSingleValue(Object object) {
		return CONVERTER.convert(object, InetAddress.class);
	}

	@Override
	protected Class<?> getType() {
		return InetAddress.class;
	}
}
