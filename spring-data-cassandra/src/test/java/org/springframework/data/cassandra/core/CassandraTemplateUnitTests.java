/*
 * Copyright 2016-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.anyInt;

import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.cassandra.support.exception.CassandraConnectionFailureException;
import org.springframework.data.cassandra.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.domain.Person;
import org.springframework.data.cassandra.test.integration.simpletons.Book;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.querybuilder.Batch;

/**
 * Unit tests for {@link CassandraTemplate}.
 * 
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class CassandraTemplateUnitTests {

	@Mock Session session;
	@Mock ResultSet resultSet;
	@Mock Row row;
	@Mock ColumnDefinitions columnDefinitions;
	@Captor ArgumentCaptor<Statement> statementCaptor;

	private CassandraTemplate template;

	@Before
	public void setUp() {

		template = new CassandraTemplate(session, new MappingCassandraConverter());
		when(session.execute(anyString())).thenReturn(resultSet);
		when(session.execute(any(Statement.class))).thenReturn(resultSet);
		when(resultSet.getColumnDefinitions()).thenReturn(columnDefinitions);
		when(row.getColumnDefinitions()).thenReturn(columnDefinitions);
	}

	@Test // DATACASS-292
	public void selectUsingCqlShouldReturnMappedResults() {

		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());
		when(columnDefinitions.contains(anyString())).thenReturn(true);
		when(columnDefinitions.getType(anyInt())).thenReturn(DataType.ascii());

		when(columnDefinitions.getIndexOf("id")).thenReturn(0);
		when(columnDefinitions.getIndexOf("firstname")).thenReturn(1);
		when(columnDefinitions.getIndexOf("lastname")).thenReturn(2);

		when(row.getObject(0)).thenReturn("myid");
		when(row.getObject(1)).thenReturn("Walter");
		when(row.getObject(2)).thenReturn("White");

		List<Person> list = template.select("SELECT * FROM person", Person.class);

		assertThat(list).hasSize(1).contains(new Person("myid", "Walter", "White"));
		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("SELECT * FROM person");
	}

	@Test // DATACASS-292
	public void selectShouldTranslateException() throws Exception {

		when(resultSet.iterator()).thenThrow(new NoHostAvailableException(Collections.emptyMap()));

		try {
			template.select("SELECT * FROM person", Person.class);

			fail("Missing CassandraConnectionFailureException");
		} catch (CassandraConnectionFailureException e) {
			assertThat(e).hasRootCauseInstanceOf(NoHostAvailableException.class);
		}
	}

	@Test // DATACASS-292
	public void selectOneShouldReturnMappedResults() {

		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());
		when(columnDefinitions.contains(anyString())).thenReturn(true);
		when(columnDefinitions.getType(anyInt())).thenReturn(DataType.ascii());

		when(columnDefinitions.getIndexOf("id")).thenReturn(0);
		when(columnDefinitions.getIndexOf("firstname")).thenReturn(1);
		when(columnDefinitions.getIndexOf("lastname")).thenReturn(2);

		when(row.getObject(0)).thenReturn("myid");
		when(row.getObject(1)).thenReturn("Walter");
		when(row.getObject(2)).thenReturn("White");

		Person person = template.selectOne("SELECT * FROM person WHERE id='myid';", Person.class);

		assertThat(person).isEqualTo(new Person("myid", "Walter", "White"));
		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("SELECT * FROM person WHERE id='myid';");
	}

	@Test // DATACASS-292
	public void selectOneByIdShouldReturnMappedResults() {

		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());
		when(columnDefinitions.contains(anyString())).thenReturn(true);
		when(columnDefinitions.getType(anyInt())).thenReturn(DataType.ascii());

		when(columnDefinitions.getIndexOf("id")).thenReturn(0);
		when(columnDefinitions.getIndexOf("firstname")).thenReturn(1);
		when(columnDefinitions.getIndexOf("lastname")).thenReturn(2);

		when(row.getObject(0)).thenReturn("myid");
		when(row.getObject(1)).thenReturn("Walter");
		when(row.getObject(2)).thenReturn("White");

		Person person = template.selectOneById("myid", Person.class);

		assertThat(person).isEqualTo(new Person("myid", "Walter", "White"));
		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("SELECT * FROM person WHERE id='myid';");
	}

	@Test // DATACASS-292
	public void existsShouldReturnExistingElement() {

		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());
		when(columnDefinitions.contains(anyString())).thenReturn(true);
		when(columnDefinitions.getType(anyInt())).thenReturn(DataType.ascii());

		boolean exists = template.exists("myid", Person.class);

		assertThat(exists).isTrue();
		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("SELECT * FROM person WHERE id='myid';");
	}

	@Test // DATACASS-292
	public void existsShouldReturnNonExistingElement() {

		when(resultSet.iterator()).thenReturn(Collections.emptyIterator());

		boolean exists = template.exists("myid", Person.class);

		assertThat(exists).isFalse();
		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("SELECT * FROM person WHERE id='myid';");
	}

	@Test // DATACASS-292
	public void countShouldExecuteCountQueryElement() {

		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());
		when(row.getLong(0)).thenReturn(42L);
		when(columnDefinitions.size()).thenReturn(1);

		long count = template.count(Person.class);

		assertThat(count).isEqualTo(42L);
		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("SELECT count(*) FROM person;");
	}

	@Test // DATACASS-292
	public void insertShouldInsertEntity() {

		when(resultSet.wasApplied()).thenReturn(true);

		Person person = new Person("heisenberg", "Walter", "White");

		Person inserted = template.insert(person);

		assertThat(inserted).isEqualTo(person);
		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString())
				.isEqualTo("INSERT INTO person (firstname,id,lastname) VALUES ('Walter','heisenberg','White');");
	}

	@Test // DATACASS-292
	public void insertShouldTranslateException() throws Exception {

		reset(session);
		when(session.execute(any(Statement.class))).thenThrow(new NoHostAvailableException(Collections.emptyMap()));

		try {
			template.insert(new Person("heisenberg", "Walter", "White"));

			fail("Missing CassandraConnectionFailureException");
		} catch (CassandraConnectionFailureException e) {
			assertThat(e).hasRootCauseInstanceOf(NoHostAvailableException.class);
		}
	}

	@Test // DATACASS-292
	public void insertShouldNotApplyInsert() {

		when(resultSet.wasApplied()).thenReturn(false);

		Person person = new Person("heisenberg", "Walter", "White");

		Person inserted = template.insert(person);

		assertThat(inserted).isNull();
	}

	@Test // DATACASS-292
	public void updateShouldUpdateEntity() {

		when(resultSet.wasApplied()).thenReturn(true);

		Person person = new Person("heisenberg", "Walter", "White");

		Person updated = template.update(person);

		assertThat(updated).isEqualTo(person);
		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString())
				.isEqualTo("UPDATE person SET firstname='Walter',lastname='White' WHERE id='heisenberg';");
	}

	@Test // DATACASS-292
	public void updateShouldTranslateException() throws Exception {

		reset(session);
		when(session.execute(any(Statement.class))).thenThrow(new NoHostAvailableException(Collections.emptyMap()));

		try {
			template.update(new Person("heisenberg", "Walter", "White"));

			fail("Missing CassandraConnectionFailureException");
		} catch (CassandraConnectionFailureException e) {
			assertThat(e).hasRootCauseInstanceOf(NoHostAvailableException.class);
		}
	}

	@Test // DATACASS-292
	public void updateShouldNotApplyUpdate() {

		when(resultSet.wasApplied()).thenReturn(false);

		Person person = new Person("heisenberg", "Walter", "White");

		Person updated = template.update(person);

		assertThat(updated).isNull();
	}

	@Test // DATACASS-292
	public void deleteByIdShouldRemoveEntity() {

		when(resultSet.wasApplied()).thenReturn(true);

		Person person = new Person("heisenberg", "Walter", "White");

		boolean deleted = template.deleteById(person.getId(), Person.class);

		assertThat(deleted).isTrue();
		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("DELETE FROM person WHERE id='heisenberg';");
	}

	@Test // DATACASS-292
	public void deleteShouldRemoveEntity() {

		when(resultSet.wasApplied()).thenReturn(true);

		Person person = new Person("heisenberg", "Walter", "White");

		Person deleted = template.delete(person);

		assertThat(deleted).isEqualTo(person);
		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("DELETE FROM person WHERE id='heisenberg';");
	}

	@Test // DATACASS-292
	public void deleteShouldTranslateException() throws Exception {

		reset(session);
		when(session.execute(any(Statement.class))).thenThrow(new NoHostAvailableException(Collections.emptyMap()));

		try {
			template.delete(new Person("heisenberg", "Walter", "White"));

			fail("Missing CassandraConnectionFailureException");
		} catch (CassandraConnectionFailureException e) {
			assertThat(e).hasRootCauseInstanceOf(NoHostAvailableException.class);
		}
	}

	@Test // DATACASS-292
	public void deleteShouldNotApplyRemoval() {

		when(resultSet.wasApplied()).thenReturn(false);

		Person person = new Person("heisenberg", "Walter", "White");

		Person deleted = template.delete(person);

		assertThat(deleted).isNull();
	}

	@Test // DATACASS-292
	public void truncateShouldRemoveEntities() {

		template.truncate(Person.class);

		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("TRUNCATE person;");
	}

	@Test // DATACASS-292
	@Ignore
	public void batchOperationsShouldCallSession() {

		template.batchOps().insert(new Book()).execute();

		verify(session).execute(Mockito.any(Batch.class));
	}
}
