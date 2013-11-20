package org.springframework.cassandra.core.keyspace;

import com.datastax.driver.core.DataType;

public class AlterColumnSpecification extends ColumnTypeChangeSpecification {

	public AlterColumnSpecification(String name, DataType type) {
		super(name, type);
	}
}
