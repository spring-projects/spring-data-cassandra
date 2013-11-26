package org.springframework.cassandra.core.keyspace;

import com.datastax.driver.core.DataType;

public class AddColumnSpecification extends ColumnTypeChangeSpecification {

	public AddColumnSpecification(String name, DataType type) {
		super(name, type);
	}
}
