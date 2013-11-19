package org.springframework.cassandra.cql;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
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
