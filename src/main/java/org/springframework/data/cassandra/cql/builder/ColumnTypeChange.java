package org.springframework.data.cassandra.cql.builder;

import org.springframework.util.Assert;

import com.datastax.driver.core.DataType;

public abstract class ColumnTypeChange extends ColumnChange {

	private DataType type;

	public ColumnTypeChange(String name, DataType type) {
		super(name);
		setType(type);
	}

	private void setType(DataType type) {
		Assert.notNull(type);
		this.type = type;
	}

	public DataType getType() {
		return type;
	}
}
