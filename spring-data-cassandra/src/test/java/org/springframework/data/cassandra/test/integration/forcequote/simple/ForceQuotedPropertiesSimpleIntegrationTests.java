package org.springframework.data.cassandra.test.integration.forcequote.simple;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.springframework.cassandra.core.cql.CqlIdentifier.cqlId;
import static org.springframework.cassandra.core.cql.CqlIdentifier.quotedCqlId;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.springframework.cassandra.core.PrimaryKeyType;
import org.springframework.cassandra.core.cql.CqlIdentifier;
import org.springframework.data.cassandra.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.mapping.CassandraPersistentProperty;
import org.springframework.data.cassandra.mapping.Column;
import org.springframework.data.cassandra.mapping.BasicCassandraMappingContext;
import org.springframework.data.cassandra.mapping.PrimaryKey;
import org.springframework.data.cassandra.mapping.PrimaryKeyClass;
import org.springframework.data.cassandra.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.mapping.Table;

public class ForceQuotedPropertiesSimpleIntegrationTests {

	CassandraMappingContext context = new BasicCassandraMappingContext();

	@Test
	public void testImplicit() {
		CassandraPersistentEntity<?> entity = context.getPersistentEntity(Implicit.class);

		CassandraPersistentProperty primaryKey = entity.getPersistentProperty("primaryKey");
		CassandraPersistentProperty aString = entity.getPersistentProperty("aString");

		assertEquals("\"primaryKey\"", primaryKey.getColumnName().toCql());
		assertEquals("\"aString\"", aString.getColumnName().toCql());
	}

	@Table
	public static class Implicit {

		@PrimaryKey(forceQuote = true)
		String primaryKey;

		@Column(forceQuote = true)
		String aString;
	}

	@Test
	public void testDefault() {
		CassandraPersistentEntity<?> entity = context.getPersistentEntity(Default.class);

		CassandraPersistentProperty primaryKey = entity.getPersistentProperty("primaryKey");
		CassandraPersistentProperty aString = entity.getPersistentProperty("aString");

		assertEquals("primarykey", primaryKey.getColumnName().toCql());
		assertEquals("astring", aString.getColumnName().toCql());
	}

	@Table
	public static class Default {

		@PrimaryKey
		String primaryKey;

		@Column
		String aString;
	}

	public static final String EXPLICIT_PRIMARY_KEY_NAME = "ThePrimaryKey";
	public static final String EXPLICIT_COLUMN_NAME = "AnotherColumn";

	@Test
	public void testExplicit() {
		CassandraPersistentEntity<?> entity = context.getPersistentEntity(Explicit.class);

		CassandraPersistentProperty primaryKey = entity.getPersistentProperty("primaryKey");
		CassandraPersistentProperty aString = entity.getPersistentProperty("aString");

		assertEquals("\"" + EXPLICIT_PRIMARY_KEY_NAME + "\"", primaryKey.getColumnName().toCql());
		assertEquals("\"" + EXPLICIT_COLUMN_NAME + "\"", aString.getColumnName().toCql());
	}

	@Table
	public static class Explicit {

		@PrimaryKey(value = EXPLICIT_PRIMARY_KEY_NAME, forceQuote = true)
		String primaryKey;

		@Column(value = EXPLICIT_COLUMN_NAME, forceQuote = true)
		String aString;
	}

	@Test
	public void testImplicitComposite() {
		CassandraPersistentEntity<?> key = context.getPersistentEntity(ImplicitKey.class);

		CassandraPersistentProperty stringZero = key.getPersistentProperty("stringZero");
		CassandraPersistentProperty stringOne = key.getPersistentProperty("stringOne");

		assertEquals("\"stringZero\"", stringZero.getColumnName().toCql());
		assertEquals("\"stringOne\"", stringOne.getColumnName().toCql());

		List<CqlIdentifier> names = Arrays
				.asList(new CqlIdentifier[] { quotedCqlId("stringZero"), quotedCqlId("stringOne") });
		CassandraPersistentEntity<?> entity = context.getPersistentEntity(ImplicitComposite.class);

		assertEquals(names, entity.getPersistentProperty("primaryKey").getColumnNames());
	}

	@PrimaryKeyClass
	public static class ImplicitKey implements Serializable {

		private static final long serialVersionUID = -1956747638065267667L;

		@PrimaryKeyColumn(ordinal = 0, forceQuote = true, type = PrimaryKeyType.PARTITIONED)
		String stringZero;

		@PrimaryKeyColumn(ordinal = 1, forceQuote = true)
		String stringOne;
	}

	@Table
	public static class ImplicitComposite {

		@PrimaryKey(forceQuote = true)
		ImplicitKey primaryKey;

		@Column(forceQuote = true)
		String aString;
	}

	@Test
	public void testDefaultComposite() {
		CassandraPersistentEntity<?> key = context.getPersistentEntity(DefaultKey.class);

		CassandraPersistentProperty stringZero = key.getPersistentProperty("stringZero");
		CassandraPersistentProperty stringOne = key.getPersistentProperty("stringOne");

		assertTrue(stringZero.getColumnName().equals("stringZero"));
		assertTrue(stringOne.getColumnName().equals("stringOne"));
		assertEquals("stringzero", stringZero.getColumnName().toCql());
		assertEquals("stringone", stringOne.getColumnName().toCql());

		List<CqlIdentifier> names = Arrays.asList(new CqlIdentifier[] { cqlId("stringZero"), cqlId("stringOne") });
		CassandraPersistentEntity<?> entity = context.getPersistentEntity(DefaultComposite.class);

		assertEquals(names, entity.getPersistentProperty("primaryKey").getColumnNames());
	}

	@PrimaryKeyClass
	public static class DefaultKey implements Serializable {

		private static final long serialVersionUID = -1956747638065267667L;

		@PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED)
		String stringZero;

		@PrimaryKeyColumn(ordinal = 1)
		String stringOne;
	}

	@Table
	public static class DefaultComposite {

		@PrimaryKey
		DefaultKey primaryKey;

		@Column
		String aString;
	}

	public static final String EXPLICIT_KEY_0 = "TheFirstKeyField";
	public static final String EXPLICIT_KEY_1 = "TheSecondKeyField";

	@Test
	public void testExplicitComposite() {
		CassandraPersistentEntity<?> key = context.getPersistentEntity(ExplicitKey.class);

		CassandraPersistentProperty stringZero = key.getPersistentProperty("stringZero");
		CassandraPersistentProperty stringOne = key.getPersistentProperty("stringOne");

		assertEquals("\"" + EXPLICIT_KEY_0 + "\"", stringZero.getColumnName().toCql());
		assertEquals("\"" + EXPLICIT_KEY_1 + "\"", stringOne.getColumnName().toCql());

		List<CqlIdentifier> names = Arrays.asList(new CqlIdentifier[] { quotedCqlId(EXPLICIT_KEY_0),
				quotedCqlId(EXPLICIT_KEY_1) });
		CassandraPersistentEntity<?> entity = context.getPersistentEntity(ExplicitComposite.class);

		assertEquals(names, entity.getPersistentProperty("primaryKey").getColumnNames());
	}

	@PrimaryKeyClass
	public static class ExplicitKey implements Serializable {

		private static final long serialVersionUID = -1956747638065267667L;

		@PrimaryKeyColumn(ordinal = 0, name = EXPLICIT_KEY_0, forceQuote = true, type = PrimaryKeyType.PARTITIONED)
		String stringZero;

		@PrimaryKeyColumn(ordinal = 1, name = EXPLICIT_KEY_1, forceQuote = true)
		String stringOne;
	}

	@Table
	public static class ExplicitComposite {

		@PrimaryKey(forceQuote = true)
		ExplicitKey primaryKey;

		@Column(forceQuote = true)
		String aString;
	}
}
