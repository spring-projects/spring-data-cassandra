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

import static org.assertj.core.api.Assertions.assertThat;
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
 * @author Mark Paluch
 */
public class CassandraExceptionTranslatorUnitTests {

	CassandraExceptionTranslator tx = new CassandraExceptionTranslator();

	@Test
	public void testTableExistsException() {
		String keyspace = "";
		String table = "tbl";
		AlreadyExistsException cx = new AlreadyExistsException(keyspace, table);
		DataAccessException dax = tx.translateExceptionIfPossible(cx);
		assertThat(dax).isNotNull();
		assertThat(dax instanceof CassandraTableExistsException).isTrue();

		CassandraTableExistsException x = (CassandraTableExistsException) dax;
		assertThat(x.getTableName()).isEqualTo(table);
		assertThat(x.getElementName()).isEqualTo(x.getTableName());
		assertThat(x.getElementType()).isEqualTo(CassandraSchemaElementExistsException.ElementType.TABLE);
		assertThat(x.getCause()).isEqualTo(cx);
	}

	@Test
	public void testKeyspaceExistsException() {
		String keyspace = "ks";
		String table = "";
		AlreadyExistsException cx = new AlreadyExistsException(keyspace, table);
		DataAccessException dax = tx.translateExceptionIfPossible(cx);
		assertThat(dax).isNotNull();
		assertThat(dax instanceof CassandraKeyspaceExistsException).isTrue();

		CassandraKeyspaceExistsException x = (CassandraKeyspaceExistsException) dax;
		assertThat(x.getKeyspaceName()).isEqualTo(keyspace);
		assertThat(x.getElementName()).isEqualTo(x.getKeyspaceName());
		assertThat(x.getElementType()).isEqualTo(CassandraSchemaElementExistsException.ElementType.KEYSPACE);
		assertThat(x.getCause()).isEqualTo(cx);
	}

	@Test
	public void testInvalidConfigurationInQueryException() {
		String msg = "msg";
		InvalidQueryException cx = new InvalidConfigurationInQueryException(null, msg);
		DataAccessException dax = tx.translateExceptionIfPossible(cx);
		assertThat(dax).isNotNull();
		assertThat(dax instanceof CassandraInvalidConfigurationInQueryException).isTrue();
		assertThat(dax.getCause()).isEqualTo(cx);

		cx = new InvalidQueryException(msg);
		dax = tx.translateExceptionIfPossible(cx);
		assertThat(dax).isNotNull();
		assertThat(dax instanceof CassandraInvalidQueryException).isTrue();
		assertThat(dax.getCause()).isEqualTo(cx);
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void shouldTranslateWithCqlMessage() {

		InvalidQueryException cx = new InvalidConfigurationInQueryException(null, "err");
		DataAccessException dax = tx.translate("Query", "SELECT * FROM person", cx);

		assertThat(dax).hasRootCauseInstanceOf(InvalidQueryException.class).hasMessage(
				"Query; CQL [SELECT * FROM person]; err; nested exception is com.datastax.driver.core.exceptions.InvalidConfigurationInQueryException: err");
	}
}
