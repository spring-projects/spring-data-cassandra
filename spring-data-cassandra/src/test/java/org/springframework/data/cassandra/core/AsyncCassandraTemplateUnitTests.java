/*
 * Copyright 2016 the original author or authors.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.cassandra.support.exception.CassandraConnectionFailureException;
import org.springframework.data.cassandra.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.domain.Person;
import org.springframework.util.concurrent.ListenableFuture;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.google.common.util.concurrent.AbstractFuture;

/**
 * Unit tests for {@link AsyncCassandraTemplate}.
 * 
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class AsyncCassandraTemplateUnitTests {

	@Mock Session session;
	@Mock ResultSet resultSet;
	@Mock Row row;
	@Mock ColumnDefinitions columnDefinitions;
	@Captor ArgumentCaptor<Statement> statementCaptor;

	private AsyncCassandraTemplate template;

	@Before
	public void setUp() {

		template = new AsyncCassandraTemplate(session);
		when(session.executeAsync(anyString())).thenReturn(new TestResultSetFuture(resultSet));
		when(session.executeAsync(any(Statement.class))).thenReturn(new TestResultSetFuture(resultSet));
		when(resultSet.getColumnDefinitions()).thenReturn(columnDefinitions);
		when(row.getColumnDefinitions()).thenReturn(columnDefinitions);
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
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

		ListenableFuture<List<Person>> list = template.select("SELECT * FROM person", Person.class);

		assertThat(getUninterruptibly(list)).hasSize(1).contains(new Person("myid", "Walter", "White"));
		verify(session).executeAsync(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("SELECT * FROM person");
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void selectUsingCqlShouldInvokeCallbackWithMappedResults() {

		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());
		when(resultSet.spliterator()).thenReturn(Arrays.asList(row).spliterator());
		when(columnDefinitions.contains(anyString())).thenReturn(true);
		when(columnDefinitions.getType(anyInt())).thenReturn(DataType.ascii());

		when(columnDefinitions.getIndexOf("id")).thenReturn(0);
		when(columnDefinitions.getIndexOf("firstname")).thenReturn(1);
		when(columnDefinitions.getIndexOf("lastname")).thenReturn(2);

		when(row.getObject(0)).thenReturn("myid");
		when(row.getObject(1)).thenReturn("Walter");
		when(row.getObject(2)).thenReturn("White");

		List<Person> list = new ArrayList<>();

		ListenableFuture<Void> result = template.select("SELECT * FROM person", list::add, Person.class);

		assertThat(getUninterruptibly(result)).isNull();
		assertThat(list).hasSize(1).contains(new Person("myid", "Walter", "White"));
		verify(session).executeAsync(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("SELECT * FROM person");
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void selectShouldTranslateException() throws Exception {

		when(resultSet.iterator()).thenThrow(new NoHostAvailableException(Collections.emptyMap()));

		ListenableFuture<List<Person>> list = template.select("SELECT * FROM person", Person.class);

		try {
			list.get();

			fail("Missing CassandraConnectionFailureException");
		} catch (ExecutionException e) {
			assertThat(e).hasCauseInstanceOf(CassandraConnectionFailureException.class)
					.hasRootCauseInstanceOf(NoHostAvailableException.class);
		}
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
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

		ListenableFuture<Person> future = template.selectOne("SELECT * FROM person WHERE id='myid';", Person.class);

		assertThat(getUninterruptibly(future)).isEqualTo(new Person("myid", "Walter", "White"));
		verify(session).executeAsync(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("SELECT * FROM person WHERE id='myid';");
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
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

		ListenableFuture<Person> future = template.selectOneById("myid", Person.class);

		assertThat(getUninterruptibly(future)).isEqualTo(new Person("myid", "Walter", "White"));
		verify(session).executeAsync(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("SELECT * FROM person WHERE id='myid';");
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void existsShouldReturnExistingElement() {

		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());
		when(columnDefinitions.contains(anyString())).thenReturn(true);
		when(columnDefinitions.getType(anyInt())).thenReturn(DataType.ascii());

		ListenableFuture<Boolean> future = template.exists("myid", Person.class);

		assertThat(getUninterruptibly(future)).isTrue();
		verify(session).executeAsync(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("SELECT * FROM person WHERE id='myid';");
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void existsShouldReturnNonExistingElement() {

		when(resultSet.iterator()).thenReturn(Collections.emptyIterator());

		ListenableFuture<Boolean> future = template.exists("myid", Person.class);

		assertThat(getUninterruptibly(future)).isFalse();
		verify(session).executeAsync(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("SELECT * FROM person WHERE id='myid';");
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void countShouldExecuteCountQueryElement() {

		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());
		when(row.getLong(0)).thenReturn(42L);
		when(columnDefinitions.size()).thenReturn(1);

		ListenableFuture<Long> future = template.count(Person.class);

		assertThat(getUninterruptibly(future)).isEqualTo(42L);
		verify(session).executeAsync(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("SELECT count(*) FROM person;");
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void insertShouldInsertEntity() {

		when(resultSet.wasApplied()).thenReturn(true);

		Person person = new Person("heisenberg", "Walter", "White");

		ListenableFuture<Person> future = template.insert(person);

		assertThat(getUninterruptibly(future)).isEqualTo(person);
		verify(session).executeAsync(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString())
				.isEqualTo("INSERT INTO person (firstname,id,lastname) VALUES ('Walter','heisenberg','White');");
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void insertShouldTranslateException() throws Exception {

		reset(session);
		when(session.executeAsync(any(Statement.class)))
				.thenReturn(TestResultSetFuture.failed(new NoHostAvailableException(Collections.emptyMap())));

		ListenableFuture<Person> future = template.insert(new Person("heisenberg", "Walter", "White"));

		try {
			future.get();

			fail("Missing CassandraConnectionFailureException");
		} catch (ExecutionException e) {
			assertThat(e).hasCauseInstanceOf(CassandraConnectionFailureException.class)
					.hasRootCauseInstanceOf(NoHostAvailableException.class);
		}
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void insertShouldNotApplyInsert() {

		when(resultSet.wasApplied()).thenReturn(false);

		Person person = new Person("heisenberg", "Walter", "White");

		ListenableFuture<Person> future = template.insert(person);

		assertThat(getUninterruptibly(future)).isNull();
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void updateShouldUpdateEntity() {

		when(resultSet.wasApplied()).thenReturn(true);

		Person person = new Person("heisenberg", "Walter", "White");

		ListenableFuture<Person> future = template.update(person);

		assertThat(getUninterruptibly(future)).isEqualTo(person);
		verify(session).executeAsync(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString())
				.isEqualTo("UPDATE person SET firstname='Walter',lastname='White' WHERE id='heisenberg';");
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void updateShouldTranslateException() throws Exception {

		reset(session);
		when(session.executeAsync(any(Statement.class)))
				.thenReturn(TestResultSetFuture.failed(new NoHostAvailableException(Collections.emptyMap())));

		ListenableFuture<Person> future = template.update(new Person("heisenberg", "Walter", "White"));

		try {
			future.get();

			fail("Missing CassandraConnectionFailureException");
		} catch (ExecutionException e) {
			assertThat(e).hasCauseInstanceOf(CassandraConnectionFailureException.class)
					.hasRootCauseInstanceOf(NoHostAvailableException.class);
		}
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void updateShouldNotApplyUpdate() {

		when(resultSet.wasApplied()).thenReturn(false);

		Person person = new Person("heisenberg", "Walter", "White");

		ListenableFuture<Person> future = template.update(person);

		assertThat(getUninterruptibly(future)).isNull();
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void deleteByIdShouldRemoveEntity() {

		when(resultSet.wasApplied()).thenReturn(true);

		Person person = new Person("heisenberg", "Walter", "White");

		ListenableFuture<Boolean> future = template.deleteById(person.getId(), Person.class);

		assertThat(getUninterruptibly(future)).isTrue();
		verify(session).executeAsync(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("DELETE FROM person WHERE id='heisenberg';");
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void deleteShouldRemoveEntity() {

		when(resultSet.wasApplied()).thenReturn(true);

		Person person = new Person("heisenberg", "Walter", "White");

		ListenableFuture<Person> future = template.delete(person);

		assertThat(getUninterruptibly(future)).isEqualTo(person);
		verify(session).executeAsync(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("DELETE FROM person WHERE id='heisenberg';");
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void deleteShouldTranslateException() throws Exception {

		reset(session);
		when(session.executeAsync(any(Statement.class)))
				.thenReturn(TestResultSetFuture.failed(new NoHostAvailableException(Collections.emptyMap())));

		ListenableFuture<Person> future = template.delete(new Person("heisenberg", "Walter", "White"));

		try {
			future.get();

			fail("Missing CassandraConnectionFailureException");
		} catch (ExecutionException e) {
			assertThat(e).hasCauseInstanceOf(CassandraConnectionFailureException.class)
					.hasRootCauseInstanceOf(NoHostAvailableException.class);
		}
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void deleteShouldNotApplyRemoval() {

		when(resultSet.wasApplied()).thenReturn(false);

		Person person = new Person("heisenberg", "Walter", "White");

		ListenableFuture<Person> future = template.delete(person);

		assertThat(getUninterruptibly(future)).isNull();
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void truncateShouldRemoveEntities() {

		template.truncate(Person.class);

		verify(session).executeAsync(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("TRUNCATE person;");
	}

	private static <T> T getUninterruptibly(Future<T> future) {

		try {
			return future.get();
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	private static class TestResultSetFuture extends AbstractFuture<ResultSet> implements ResultSetFuture {

		public TestResultSetFuture() {}

		public TestResultSetFuture(ResultSet resultSet) {
			set(resultSet);
		}

		@Override
		public boolean set(ResultSet value) {
			return super.set(value);
		}

		@Override
		public ResultSet getUninterruptibly() {
			return null;
		}

		@Override
		public ResultSet getUninterruptibly(long l, TimeUnit timeUnit) throws TimeoutException {
			return null;
		}

		@Override
		protected boolean setException(Throwable throwable) {
			return super.setException(throwable);
		}

		/**
		 * Create a completed future that reports a failure given {@link Throwable}.
		 * 
		 * @param throwable must not be {@literal null}.
		 * @return the completed/failed {@link TestResultSetFuture}.
		 */
		public static TestResultSetFuture failed(Throwable throwable) {

			TestResultSetFuture future = new TestResultSetFuture();
			future.setException(throwable);
			return future;
		}
	}
}
