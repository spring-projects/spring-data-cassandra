package org.springframework.data.cassandra.test.integration.forcequote.compositeprimarykey;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import java.util.UUID;

import org.springframework.data.cassandra.core.CassandraTemplate;

public class ForceQuotedCompositePrimaryKeyRepositoryIntegrationTests {

	ImplicitRepository i;
	ExplicitRepository e;
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

		// insert
		Implicit s = i.save(entity);
		assertSame(s, entity);

		// select
		Implicit f = i.findOne(key);
		assertNotSame(f, entity);
		String stringValue = query("stringvalue", "\"Implicit\"", "\"keyZero\"", f.getPrimaryKey().getKeyZero(),
				"\"keyOne\"", f.getPrimaryKey().getKeyOne());
		assertEquals(f.getStringValue(), stringValue);

		// update
		f.setStringValue(f.getStringValue() + "X");
		Implicit u = i.save(f);
		assertSame(u, f);
		f = i.findOne(u.getPrimaryKey());
		assertNotSame(f, u);
		assertEquals(u.getStringValue(), f.getStringValue());

		// delete
		i.delete(key);
		assertNull(i.findOne(key));
	}

	public void testExplicit(String tableName, String stringValueColumnName, String keyZeroColumnName,
			String keyOneColumnName) {
		ExplicitKey key = new ExplicitKey(UUID.randomUUID().toString(), UUID.randomUUID().toString());
		Explicit entity = new Explicit(key);

		// insert
		Explicit s = e.save(entity);
		assertSame(s, entity);

		// select
		Explicit f = e.findOne(key);
		assertNotSame(f, entity);
		String stringValue = query(stringValueColumnName, tableName, keyZeroColumnName, f.getPrimaryKey().getKeyZero(),
				keyOneColumnName, f.getPrimaryKey().getKeyOne());
		assertEquals(f.getStringValue(), stringValue);

		// update
		f.setStringValue(f.getStringValue() + "X");
		Explicit u = e.save(f);
		assertSame(u, f);
		f = e.findOne(u.getPrimaryKey());
		assertNotSame(f, u);
		assertEquals(u.getStringValue(), f.getStringValue());

		// delete
		e.delete(key);
		assertNull(e.findOne(key));
	}
}
