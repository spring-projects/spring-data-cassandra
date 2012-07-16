package org.springframework.data.cassandra.core.convert;

import org.springframework.core.convert.ConversionException;

public class CassandraConversionException extends ConversionException {

	public CassandraConversionException(String message) {
		super(message);
	}

}
