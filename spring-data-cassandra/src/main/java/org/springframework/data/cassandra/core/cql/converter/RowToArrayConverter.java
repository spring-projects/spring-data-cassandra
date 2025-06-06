/*
 * Copyright 2017-2025 the original author or authors.
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

import java.util.List;

import org.springframework.core.convert.converter.Converter;

import com.datastax.oss.driver.api.core.cql.Row;

/**
 * Converter to convert {@link Row} to {@link Object} array.
 *
 * @author Mark Paluch
 */
public enum RowToArrayConverter implements Converter<Row, Object[]> {

	INSTANCE;

	@Override
	public Object[] convert(Row row) {

		List<Object> list = RowToListConverter.INSTANCE.convert(row);
		return list.toArray();
	}
}
