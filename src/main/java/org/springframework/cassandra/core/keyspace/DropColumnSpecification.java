package org.springframework.cassandra.core.keyspace;

public class DropColumnSpecification extends ColumnChangeSpecification {

	public DropColumnSpecification(String name) {
		super(name);
	}
}
