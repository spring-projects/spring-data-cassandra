package org.springframework.data.cassandra.test.integration.forcequote.compositeprimarykey;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import java.util.UUID;

import org.springframework.data.cassandra.core.CassandraTemplate;

public class ForceQuotedCompositePrimaryKeyRepositoryIntegrationTests {

	ImplicitRepository i;
	// ImplicitPropertiesRepository ip;
	ExplicitRepository e;
	// ExplicitPropertiesRepository ep;
	CassandraTemplate t;

	public void before() {
		t.deleteAll(Implicit.class);
	}

	public String query(String columnName, String tableName, String keyZeroColumnName, String keyZero,
			String keyOneColumnName, String keyOne) {

		return t.queryForObject(String.format("select %s from %s where %s = '%s' and %s = '%s'", columnName, tableName,
				keyZeroColumnName, keyZero, keyOneColumnName, keyOne), String.class);
	}

	public void testImplicit() {

		ImplicitKey key = new ImplicitKey(UUID.randomUUID().toString(), UUID.randomUUID().toString());
		Implicit entity = new Implicit(key);

		Implicit s = i.save(entity);
		assertSame(s, entity);

		Implicit f = i.findOne(key);
		assertNotSame(f, entity);

		String stringValue = query("stringvalue", "\"Implicit\"", "\"keyZero\"", f.getPrimaryKey().getKeyZero(),
				"\"keyOne\"", f.getPrimaryKey().getKeyOne());
		assertEquals(f.getStringValue(), stringValue);

		i.delete(key);

		assertNull(i.findOne(key));
	}

	public void testExplicit() {
		ExplicitKey key = new ExplicitKey(UUID.randomUUID().toString(), UUID.randomUUID().toString());
		Explicit entity = new Explicit(key);

		Explicit s = e.save(entity);
		assertSame(s, entity);

		Explicit f = e.findOne(key);
		assertNotSame(f, entity);

		String stringValue = query("stringvalue", "\"Explicit\"", String.format("\"%s\"", ExplicitKey.EXPLICIT_KEY_ZERO), f
				.getPrimaryKey().getKeyZero(), String.format("\"%s\"", ExplicitKey.EXPLICIT_KEY_ONE), f.getPrimaryKey()
				.getKeyOne());
		assertEquals(f.getStringValue(), stringValue);

		e.delete(key);

		assertNull(e.findOne(key));
	}

	// public void testImplicitProperties() {
	// ImplicitProperties entity = new ImplicitProperties();
	// String key = entity.getPrimaryKey();
	//
	// ImplicitProperties s = ip.save(entity);
	// assertSame(s, entity);
	//
	// ImplicitProperties f = ip.findOne(key);
	// assertNotSame(f, entity);
	//
	// String stringValue = query("\"stringValue\"", "implicitproperties", "\"primaryKey\"", f.getPrimaryKey());
	// assertEquals(f.getStringValue(), stringValue);
	//
	// ip.delete(key);
	//
	// assertNull(ip.findOne(key));
	// }

	// public void testExplicitProperties(String stringValueColumnName, String primaryKeyColumnName) {
	// ExplicitProperties entity = new ExplicitProperties();
	// String key = entity.getPrimaryKey();
	//
	// ExplicitProperties s = ep.save(entity);
	// assertSame(s, entity);
	//
	// ExplicitProperties f = ep.findOne(key);
	// assertNotSame(f, entity);
	//
	// String stringValue = query(String.format("\"%s\"", ExplicitProperties.EXPLICIT_STRING_VALUE), "explicitproperties",
	// String.format("\"%s\"", ExplicitProperties.EXPLICIT_PRIMARY_KEY), f.getPrimaryKey());
	// assertEquals(f.getStringValue(), stringValue);
	//
	// ip.delete(key);
	//
	// assertNull(ip.findOne(key));
	// }
}
