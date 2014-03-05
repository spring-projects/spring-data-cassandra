package org.springframework.cassandra.core.converter;

import java.util.UUID;

public class ResultSetToUuidConverter extends AbstractResultSetToBasicFixedTypeConverter<UUID> {

	@Override
	protected UUID doConvertSingleValue(Object object) {
		return CONVERTER.convert(object, UUID.class);
	}

	@Override
	protected Class<?> getType() {
		return UUID.class;
	}
}
