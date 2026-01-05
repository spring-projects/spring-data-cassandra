/*
 * Copyright 2017-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core.cql.converter;

import java.nio.ByteBuffer;

import org.springframework.core.convert.converter.Converter;

import com.datastax.oss.driver.api.core.cql.ResultSet;

/**
 * {@link Converter} from {@link ResultSet} to a single {@link ByteBuffer} value.
 *
 * @author Mark Paluch
 */
public class ResultSetToByteBufferConverter extends AbstractResultSetConverter<ByteBuffer> {

	public static final ResultSetToByteBufferConverter INSTANCE = new ResultSetToByteBufferConverter();

	@Override
	protected ByteBuffer doConvertSingleValue(Object object) {

		if (!(object instanceof ByteBuffer)) {
			throw new IllegalArgumentException(
					String.format("Cannot convert %s to desired type [%s]", "value", getType().getName()));
		}

		return (ByteBuffer) object;
	}

	@Override
	protected Class<?> getType() {
		return ByteBuffer.class;
	}
}
