package org.springframework.data.cassandra.cql.builder;

import static org.springframework.data.cassandra.cql.builder.CqlBuilder.alterTable;

import org.junit.Test;

import com.datastax.driver.core.DataType;

public class AlterTableBuilderTest {

	@Test
	public void testAlterTableBuilder() throws Exception {
		String name = "mytable";
		DataType addedType = DataType.timeuuid();
		String addedName = "added_column";
		DataType alteredType = DataType.text();
		String alteredName = "altered_column";
		String droppedName = "dropped";

		String cql = alterTable().name(name).add(addedName, addedType).alter(alteredName, alteredType).drop(droppedName)
				.toCql();
		System.out.println(cql);
	}
}
