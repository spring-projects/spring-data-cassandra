package org.springframework.cassandra.test.unit.core.cql;

import static org.junit.Assert.*;
import static org.springframework.cassandra.core.cql.CqlIdentifier.cqlId;
import static org.springframework.cassandra.core.cql.CqlIdentifier.quotedCqlId;

import org.junit.Test;
import org.springframework.cassandra.core.ReservedKeyword;
import org.springframework.cassandra.core.cql.CqlIdentifier;

public class CqlIdentifierTest {

	@Test
	public void testUnquotedIdentifiers() {

		String[] ids = new String[] { "foo", "Foo", "FOO", "a_", "a1" };

		for (String id : ids) {
			CqlIdentifier cqlId = cqlId(id);
			assertFalse(cqlId.isQuoted());
			assertEquals(id.toLowerCase(), cqlId.toCql());
		}
	}

	@Test
	public void testForceQuotedIdentifiers() {

		String[] ids = new String[] { "foo", "Foo", "FOO", "a_", "a1" };

		for (String id : ids) {
			CqlIdentifier cqlId = quotedCqlId(id);
			assertTrue(cqlId.isQuoted());
			assertEquals("\"" + id + "\"", cqlId.toCql());
		}
	}

	@Test
	public void testReservedWordsEndUpQuoted() {

		for (ReservedKeyword id : ReservedKeyword.values()) {
			CqlIdentifier cqlId = cqlId(id.name());
			assertTrue(cqlId.isQuoted());
			assertEquals("\"" + id.name() + "\"", cqlId.toCql());

			cqlId = cqlId(id.name().toLowerCase());
			assertTrue(cqlId.isQuoted());
			assertEquals("\"" + id.name().toLowerCase() + "\"", cqlId.toCql());
		}
	}

	@Test
	public void testIllegals() {
		String[] illegals = new String[] { null, "", "a ", "a a", "a\"", "a'", "a''", "\"\"", "''", "-", "a-", "_", "_a" };
		for (String illegal : illegals) {
			try {
				cqlId(illegal);
				fail(String.format("identifier [%s] should have caused IllegalArgumentException", illegal));
			} catch (IllegalArgumentException x) {
				// :)
			}
		}
	}
}