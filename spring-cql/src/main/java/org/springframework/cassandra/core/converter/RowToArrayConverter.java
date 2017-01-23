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

import java.util.List;

import org.springframework.core.convert.converter.Converter;

import com.datastax.driver.core.Row;

public class RowToArrayConverter implements Converter<Row, Object[]> {

	protected RowToListConverter delegate = new RowToListConverter();

	/* (non-Javadoc)
	 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
	 */
	@Override
	public Object[] convert(Row row) {

		List<Object> list = delegate.convert(row);
		return list == null ? null : list.toArray();
	}
}
