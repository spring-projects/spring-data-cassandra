/*
 * Copyright 2013-2014 the original author or authors
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.convert;

import java.nio.ByteBuffer;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ProtocolVersion;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.PropertyAccessor;
import org.springframework.expression.TypedValue;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;

/**
 * {@link PropertyAccessor} to read values from a {@link Row}.
 * 
 * @author Alex Shvid
 */
enum RowReaderPropertyAccessor implements PropertyAccessor {

	INSTANCE;

	@Override
	public Class<?>[] getSpecificTargetClasses() {
		return new Class<?>[] { Row.class };
	}

	@Override
	public boolean canRead(EvaluationContext context, Object target, String name) {
		return ((Row) target).getColumnDefinitions().contains(name);
	}

	@Override
	public TypedValue read(EvaluationContext context, Object target, String name) {
		Row row = (Row) target;
		if (row.isNull(name)) {
			return TypedValue.NULL;
		}
		DataType columnType = row.getColumnDefinitions().getType(name);
		ByteBuffer bytes = row.getBytes(name);
//		Object object = columnType.deserialize(bytes);
		Object object =CodecRegistry.DEFAULT_INSTANCE.codecFor(columnType).deserialize(bytes, ProtocolVersion.NEWEST_SUPPORTED);
		return new TypedValue(object);
	}

	@Override
	public boolean canWrite(EvaluationContext context, Object target, String name) {
		return false;
	}

	@Override
	public void write(EvaluationContext context, Object target, String name, Object newValue) {
		throw new UnsupportedOperationException();
	}
}
