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
