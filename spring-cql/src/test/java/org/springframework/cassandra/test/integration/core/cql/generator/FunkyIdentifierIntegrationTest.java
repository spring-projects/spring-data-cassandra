package org.springframework.cassandra.test.integration.core.cql.generator;

import org.junit.Test;
import org.springframework.cassandra.core.cql.generator.CreateTableCqlGenerator;
import org.springframework.cassandra.core.keyspace.CreateTableSpecification;
import org.springframework.cassandra.test.integration.AbstractKeyspaceCreatingIntegrationTest;
import org.springframework.cassandra.test.unit.core.cql.generator.CreateTableCqlGeneratorTests.FunkyTableNameTest;

import com.datastax.driver.core.DataType;

public class FunkyIdentifierIntegrationTest extends AbstractKeyspaceCreatingIntegrationTest {

	public FunkyIdentifierIntegrationTest() {
		super(randomKeyspaceName());
	}

	@Test
	public void testFunkyTableName() {
		for (String name : FunkyTableNameTest.FUNKY_LEGAL_NAMES) {
			SESSION.execute(new CreateTableCqlGenerator(CreateTableSpecification.createTable().name(name)
					.partitionKeyColumn("key", DataType.text())).toCql());
		}
	}

	@Test
	public void testFunkyColumnName() {
		String table = "funky";
		int i = 0;
		for (String name : FunkyTableNameTest.FUNKY_LEGAL_NAMES) {
			SESSION.execute(new CreateTableCqlGenerator(CreateTableSpecification.createTable().name(table + i++)
					.partitionKeyColumn(name, DataType.text())).toCql());
		}
	}

	@Test
	public void testFunkyTableAndColumnName() {
		for (String name : FunkyTableNameTest.FUNKY_LEGAL_NAMES) {
			SESSION.execute(new CreateTableCqlGenerator(CreateTableSpecification.createTable().name(name)
					.partitionKeyColumn(name, DataType.text())).toCql());
		}
	}
}
