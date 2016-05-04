/*
 * Copyright 2013-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.cassandra.support;

import static org.junit.Assert.*;

import org.junit.Test;
import org.springframework.cassandra.support.exception.CassandraInvalidConfigurationInQueryException;
import org.springframework.cassandra.support.exception.CassandraInvalidQueryException;
import org.springframework.cassandra.support.exception.CassandraKeyspaceExistsException;
import org.springframework.cassandra.support.exception.CassandraSchemaElementExistsException;
import org.springframework.cassandra.support.exception.CassandraTableExistsException;
import org.springframework.dao.DataAccessException;

import com.datastax.driver.core.exceptions.AlreadyExistsException;
import com.datastax.driver.core.exceptions.InvalidConfigurationInQueryException;
import com.datastax.driver.core.exceptions.InvalidQueryException;

/**
 * Unit tests for {@link CassandraExceptionTranslator}
 *
 * @author Matthew T. Adams
 */
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
		InvalidQueryException cx = new InvalidConfigurationInQueryException(null, msg);
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
