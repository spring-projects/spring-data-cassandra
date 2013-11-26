package org.springframework.cassandra.core.keyspace;

import org.springframework.util.Assert;

import com.datastax.driver.core.DataType;

public abstract class ColumnTypeChangeSpecification extends ColumnChangeSpecification {

	private DataType type;

	public ColumnTypeChangeSpecification(String name, DataType type) {
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
