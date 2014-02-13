package org.springframework.cassandra.test.unit.core.cql;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.springframework.cassandra.core.CqlIdentifier.isQuotedIdentifier;
import static org.springframework.cassandra.core.CqlIdentifier.isUnquotedIdentifier;

import org.junit.Test;

public class CqlIdentifierTest {

	@Test
	public void testIsQuotedIdentifier() throws Exception {
		assertFalse(isQuotedIdentifier("my\"id"));
		assertTrue(isQuotedIdentifier("my\"\"id"));
		assertFalse(isUnquotedIdentifier("my\"id"));
		assertTrue(isUnquotedIdentifier("myid"));
	}

}
