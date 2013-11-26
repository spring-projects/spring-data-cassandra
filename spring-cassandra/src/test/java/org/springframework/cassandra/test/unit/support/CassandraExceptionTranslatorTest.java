package org.springframework.cassandra.test.unit.support;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.springframework.cassandra.support.CassandraExceptionTranslator;
import org.springframework.cassandra.support.exception.CassandraInvalidConfigurationInQueryException;
import org.springframework.cassandra.support.exception.CassandraInvalidQueryException;
import org.springframework.cassandra.support.exception.CassandraKeyspaceExistsException;
import org.springframework.cassandra.support.exception.CassandraSchemaElementExistsException;
import org.springframework.cassandra.support.exception.CassandraTableExistsException;
import org.springframework.dao.DataAccessException;

import com.datastax.driver.core.exceptions.AlreadyExistsException;
import com.datastax.driver.core.exceptions.InvalidConfigurationInQueryException;
import com.datastax.driver.core.exceptions.InvalidQueryException;

public class CassandraExceptionTranslatorTest {

	CassandraExceptionTranslator tx = new CassandraExceptionTranslator();

	@Test
	public void testTableExistsException() {
		String keyspace = "";
		String table = "tbl";
		AlreadyExistsException cx = new AlreadyExistsException(keyspace, table);
		DataAccessException dax = tx.translateExceptionIfPossible(cx);
		assertNotNull(dax);
		assertTrue(dax instanceof CassandraTableExistsException);

		CassandraTableExistsException x = (CassandraTableExistsException) dax;
		assertEquals(table, x.getTableName());
		assertEquals(x.getTableName(), x.getElementName());
		assertEquals(CassandraSchemaElementExistsException.ElementType.TABLE, x.getElementType());
		assertEquals(cx, x.getCause());
	}

	@Test
	public void testKeyspaceExistsException() {
		String keyspace = "ks";
		String table = "";
		AlreadyExistsException cx = new AlreadyExistsException(keyspace, table);
		DataAccessException dax = tx.translateExceptionIfPossible(cx);
		assertNotNull(dax);
		assertTrue(dax instanceof CassandraKeyspaceExistsException);

		CassandraKeyspaceExistsException x = (CassandraKeyspaceExistsException) dax;
		assertEquals(keyspace, x.getKeyspaceName());
		assertEquals(x.getKeyspaceName(), x.getElementName());
		assertEquals(CassandraSchemaElementExistsException.ElementType.KEYSPACE, x.getElementType());
		assertEquals(cx, x.getCause());
	}

	@Test
	public void testInvalidConfigurationInQueryException() {
		String msg = "msg";
		InvalidQueryException cx = new InvalidConfigurationInQueryException(msg);
		DataAccessException dax = tx.translateExceptionIfPossible(cx);
		assertNotNull(dax);
		assertTrue(dax instanceof CassandraInvalidConfigurationInQueryException);
		assertEquals(cx, dax.getCause());

		cx = new InvalidQueryException(msg);
		dax = tx.translateExceptionIfPossible(cx);
		assertNotNull(dax);
		assertTrue(dax instanceof CassandraInvalidQueryException);
		assertEquals(cx, dax.getCause());
	}
}
