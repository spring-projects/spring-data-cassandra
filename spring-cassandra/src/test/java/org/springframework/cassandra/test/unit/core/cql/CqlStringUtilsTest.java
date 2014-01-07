package org.springframework.cassandra.test.unit.core.cql;

import static org.junit.Assert.*;
import static org.springframework.cassandra.core.cql.CqlStringUtils.isQuotedIdentifier;
import static org.springframework.cassandra.core.cql.CqlStringUtils.isUnquotedIdentifier;

import org.junit.Test;

public class CqlStringUtilsTest {

	@Test
	public void testIsQuotedIdentifier() throws Exception {
		assertFalse(isQuotedIdentifier("my\"id"));
		assertTrue(isQuotedIdentifier("my\"\"id"));
		assertFalse(isUnquotedIdentifier("my\"id"));
		assertTrue(isUnquotedIdentifier("myid"));
	}
}
