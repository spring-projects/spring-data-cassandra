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

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;

/**
 * Helpful class to read a column's value from a row, with possible type conversion.
 * 
 * @author Matthew T. Adams
 */
public class ColumnReader extends AbstractColumnReader {

	protected Row row;
	protected ColumnDefinitions columns;

	public ColumnReader(Row row) {
		super(row);
		this.row = row;
		this.columns = row.getColumnDefinitions();
	}

	@Override
	protected DataType getDataType(int i) {
		return columns.getType(i);
	}

	protected int getColumnIndex(String name) {
		int indexOf = columns.getIndexOf(name);
		if (indexOf == -1) {
			throw new IllegalArgumentException("Column does not exist in Cassandra table: " + name);
		}
		return indexOf;
	}

	public Row getRow() {
		return row;
	}
}
