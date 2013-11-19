package org.springframework.cassandra.cql.builder;

import static org.springframework.cassandra.core.cql.builder.CqlBuilder.dropTable;

import org.junit.Test;

public class DropTableBuilderTest {

	@Test
	public void testDropTableBuilder() throws Exception {
		String cql = dropTable().name("mytable").ifExists().toCql();

		System.out.println(cql);
	}
}
