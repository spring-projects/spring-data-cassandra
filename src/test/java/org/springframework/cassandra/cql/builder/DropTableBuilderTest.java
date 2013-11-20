package org.springframework.cassandra.cql.builder;

import static org.springframework.cassandra.core.keyspace.TableOperations.dropTable;

import org.junit.Test;
import org.springframework.cassandra.core.cql.generator.DropTableCqlGenerator;
import org.springframework.cassandra.core.keyspace.DropTableSpecification;

public class DropTableBuilderTest {

	@Test
	public void testDropTableBuilder() throws Exception {
		DropTableSpecification drop = dropTable().ifExists().name("mytable");

		DropTableCqlGenerator generator = new DropTableCqlGenerator(drop);
		System.out.println(generator.toCql());
	}
}
