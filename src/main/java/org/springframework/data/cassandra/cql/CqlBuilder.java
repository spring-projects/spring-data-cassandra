package org.springframework.data.cassandra.cql;

public class CqlBuilder {

	public static CreateTable createTable() {
		return new CreateTable();
	}
}
