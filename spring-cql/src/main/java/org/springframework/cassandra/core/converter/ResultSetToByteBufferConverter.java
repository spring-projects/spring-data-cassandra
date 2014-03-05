package org.springframework.cassandra.core.converter;

import java.nio.ByteBuffer;

public class ResultSetToByteBufferConverter extends AbstractResultSetConverter<ByteBuffer> {

	@Override
	protected ByteBuffer doConvertSingleValue(Object object) {

		if (!(object instanceof ByteBuffer)) {
			doThrow("value");
		}

		return (ByteBuffer) object;
	}

	@Override
	protected Class<?> getType() {
		return ByteBuffer.class;
	}
}
