/*
 * Copyright 2013-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core.convert;

import org.springframework.expression.EvaluationContext;
import org.springframework.expression.PropertyAccessor;
import org.springframework.expression.TypedValue;
import org.springframework.lang.Nullable;

import com.datastax.oss.driver.api.core.cql.Row;

/**
 * {@link PropertyAccessor} to read values from a {@link Row}.
 *
 * @author Alex Shvid
 * @author Antoine Toulme
 * @author Mark Paluch
 */
enum RowReaderPropertyAccessor implements PropertyAccessor {

	INSTANCE;

	@Override
	public Class<?>[] getSpecificTargetClasses() {
		return new Class<?>[] { Row.class };
	}

	@Override
	public boolean canRead(EvaluationContext context, @Nullable Object target, String name) {
		return target != null && ((Row) target).getColumnDefinitions().contains(name);
	}

	@Override
	public TypedValue read(EvaluationContext context, @Nullable Object target, String name) {

		if (target == null) {
			return TypedValue.NULL;
		}

		Row row = (Row) target;

		if (row.isNull(name)) {
			return TypedValue.NULL;
		}

		return new TypedValue(row.getObject(name));
	}

	@Override
	public boolean canWrite(EvaluationContext context, @Nullable Object target, String name) {
		return false;
	}

	@Override
	public void write(EvaluationContext context, @Nullable Object target, String name, @Nullable Object newValue) {
		throw new UnsupportedOperationException();
	}
}
