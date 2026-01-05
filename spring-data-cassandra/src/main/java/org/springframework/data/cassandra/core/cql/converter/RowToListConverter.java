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

import java.util.ArrayList;
import java.util.List;

import org.jspecify.annotations.Nullable;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;

import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;

/**
 * Converter to convert {@link Row}s to a {@link List} of {@link Object} representation.
 *
 * @author Matthew T. Adams
 * @author Stefan Birkner
 * @author Mark Paluch
 * @author Antoine Toulme
 */
@ReadingConverter
public enum RowToListConverter implements Converter<Row, List<Object>> {

	INSTANCE;

	@Override
	public List<@Nullable Object> convert(Row row) {

		ColumnDefinitions cols = row.getColumnDefinitions();
		List<@Nullable Object> objects = new ArrayList<>();

		cols.forEach(columnDefinition -> {
			objects.add(row.isNull(columnDefinition.getName()) ? null : row.getObject(columnDefinition.getName()));

		});

		return objects;
	}
}
