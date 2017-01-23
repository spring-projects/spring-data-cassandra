/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.cassandra.core.converter;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.Row;

/**
 * Converter to convert {@link Row}s to a {@link List} of {@link Object} representation.
 *
 * @author Matthew T. Adams
 * @author Stefan Birkner
 * @author Mark Paluch
 * @author Antoine Toulme
 */
@ReadingConverter
public class RowToListConverter implements Converter<Row, List<Object>> {

	public final static RowToListConverter INSTANCE = new RowToListConverter();

	/* (non-Javadoc)
	 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
	 */
	@Override
	public List<Object> convert(Row row) {

		if (row == null) {
			return null;
		}

		ColumnDefinitions cols = row.getColumnDefinitions();
		return cols.asList().stream() //
				.map(Definition::getName).map(name -> row.isNull(name) ? null : row.getObject(name)) //
				.collect(Collectors.toList());
	}
}
