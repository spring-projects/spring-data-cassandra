package org.springframework.data.cassandra.test.integration.forcequote.simple;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.springframework.data.cassandra.mapping.BasicCassandraPersistentEntity;
import org.springframework.data.cassandra.mapping.Table;
import org.springframework.data.util.ClassTypeInformation;

public class ForceQuotedEntitiesSimpleIntegrationTests {

	@Test
	public void testImplicitTableNameForceQuoted() {
		BasicCassandraPersistentEntity<ImplicitTableNameForceQuoted> entity = new BasicCassandraPersistentEntity<ImplicitTableNameForceQuoted>(
				ClassTypeInformation.from(ImplicitTableNameForceQuoted.class));

		assertEquals("\"" + ImplicitTableNameForceQuoted.class.getSimpleName() + "\"", entity.getTableName().toCql());
		assertEquals(ImplicitTableNameForceQuoted.class.getSimpleName(), entity.getTableName().getUnquoted());
	}

	@Table(forceQuote = true)
	public static class ImplicitTableNameForceQuoted {}

	public static final String EXPLICIT_TABLE_NAME = "Xx";

	@Test
	public void testExplicitTableNameForceQuoted() {
		BasicCassandraPersistentEntity<ExplicitTableNameForceQuoted> entity = new BasicCassandraPersistentEntity<ExplicitTableNameForceQuoted>(
				ClassTypeInformation.from(ExplicitTableNameForceQuoted.class));

		assertEquals("\"" + EXPLICIT_TABLE_NAME + "\"", entity.getTableName().toCql());
		assertEquals(EXPLICIT_TABLE_NAME, entity.getTableName().getUnquoted());
	}

	@Table(value = EXPLICIT_TABLE_NAME, forceQuote = true)
	public static class ExplicitTableNameForceQuoted {}

	@Test
	public void testDefaultTableNameForceQuoted() {
		BasicCassandraPersistentEntity<DefaultTableNameForceQuoted> entity = new BasicCassandraPersistentEntity<DefaultTableNameForceQuoted>(
				ClassTypeInformation.from(DefaultTableNameForceQuoted.class));

		assertEquals(DefaultTableNameForceQuoted.class.getSimpleName().toLowerCase(), entity.getTableName().toCql());
		assertEquals(DefaultTableNameForceQuoted.class.getSimpleName().toLowerCase(), entity.getTableName().getUnquoted());
	}

	@Table
	public static class DefaultTableNameForceQuoted {}
}
