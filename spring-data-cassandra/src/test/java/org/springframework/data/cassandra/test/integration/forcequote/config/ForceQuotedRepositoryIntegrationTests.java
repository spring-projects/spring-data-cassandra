package org.springframework.data.cassandra.test.integration.forcequote.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import org.springframework.data.cassandra.core.CassandraTemplate;

public class ForceQuotedRepositoryIntegrationTests {

	ImplicitRepository i;
	ImplicitPropertiesRepository ip;
	ExplicitRepository e;
	ExplicitPropertiesRepository ep;
	CassandraTemplate t;

	public void before() {
		t.deleteAll(Implicit.class);
	}

	public String query(String columnName, String tableName, String keyColumnName, String key) {
		return t.queryForObject(
				String.format("select %s from %s where %s = '%s'", columnName, tableName, keyColumnName, key), String.class);
	}

	public void testImplicit() {
		Implicit entity = new Implicit();
		String key = entity.getPrimaryKey();

		Implicit s = i.save(entity);
		assertSame(s, entity);

		Implicit f = i.findOne(key);
		assertNotSame(f, entity);

		String stringValue = query("stringvalue", "\"Implicit\"", "primarykey", f.getPrimaryKey());
		assertEquals(f.getStringValue(), stringValue);

		i.delete(key);

		assertNull(i.findOne(key));
	}

	public void testExplicit() {
		Explicit entity = new Explicit();
		String key = entity.getPrimaryKey();

		Explicit s = e.save(entity);
		assertSame(s, entity);

		Explicit f = e.findOne(key);
		assertNotSame(f, entity);

		String stringValue = query("stringvalue", "\"Xx\"", "primarykey", f.getPrimaryKey());
		assertEquals(f.getStringValue(), stringValue);

		e.delete(key);

		assertNull(e.findOne(key));
	}

	public void testImplicitProperties() {
		ImplicitProperties entity = new ImplicitProperties();
		String key = entity.getPrimaryKey();

		ImplicitProperties s = ip.save(entity);
		assertSame(s, entity);

		ImplicitProperties f = ip.findOne(key);
		assertNotSame(f, entity);

		String stringValue = query("\"stringValue\"", "implicitproperties", "\"primaryKey\"", f.getPrimaryKey());
		assertEquals(f.getStringValue(), stringValue);

		ip.delete(key);

		assertNull(ip.findOne(key));
	}

	public void testExplicitProperties(String stringValueColumnName, String primaryKeyColumnName) {
		ExplicitProperties entity = new ExplicitProperties();
		String key = entity.getPrimaryKey();

		ExplicitProperties s = ep.save(entity);
		assertSame(s, entity);

		ExplicitProperties f = ep.findOne(key);
		assertNotSame(f, entity);

		String stringValue = query(String.format("\"%s\"", ExplicitProperties.EXPLICIT_STRING_VALUE), "explicitproperties",
				String.format("\"%s\"", ExplicitProperties.EXPLICIT_PRIMARY_KEY), f.getPrimaryKey());
		assertEquals(f.getStringValue(), stringValue);

		ip.delete(key);

		assertNull(ip.findOne(key));
	}
}
