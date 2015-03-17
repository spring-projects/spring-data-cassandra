package org.springframework.data.cassandra.convert;

import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;

/**
 * Helpful class to read a column's value from a row, with possible type conversion.
 * 
 * @author Matthew T. Adams
 */
public class UDTValueAccessor extends AbstractColumnAccessor {

	protected UDTValue udtValue;
	protected UserType udtType;
	private List<String> fieldNames;

	public UDTValueAccessor(UDTValue value) {
		super(value, value);
		this.udtValue = value;
		this.udtType = value.getType();
		this.fieldNames = new ArrayList<String>(udtType.getFieldNames());
	}

	@Override
	protected DataType getDataType(int i) {
		return udtType.getFieldType(fieldNames.get(i));
	}

	protected int getColumnIndex(String name) {
		int indexOf = fieldNames.indexOf(name);
		if (indexOf == -1) {
			throw new IllegalArgumentException("Column does not exist in Cassandra table: " + name);
		}
		return indexOf;
	}

	public UDTValue getUDTValue() {
		return udtValue;
	}
}
