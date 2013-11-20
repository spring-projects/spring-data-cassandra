package org.springframework.cassandra.test.unit.core.cql.generator;

import static org.springframework.cassandra.core.keyspace.TableOperations.alterTable;

import org.junit.Test;
import org.springframework.cassandra.core.cql.generator.AlterTableCqlGenerator;
import org.springframework.cassandra.core.keyspace.AlterTableSpecification;

import com.datastax.driver.core.DataType;

public class AlterTableCqlGeneratorTest {

	@Test
	public void testAlterTableBuilder() throws Exception {
		String name = "mytable";
		DataType addedType = DataType.timeuuid();
		String addedName = "added_column";
		DataType alteredType = DataType.text();
		String alteredName = "altered_column";
		String droppedName = "dropped";

		AlterTableSpecification alter = alterTable().name(name).add(addedName, addedType).alter(alteredName, alteredType)
				.drop(droppedName);
		AlterTableCqlGenerator generator = new AlterTableCqlGenerator(alter);
		System.out.println(generator.toCql());
	}
}
