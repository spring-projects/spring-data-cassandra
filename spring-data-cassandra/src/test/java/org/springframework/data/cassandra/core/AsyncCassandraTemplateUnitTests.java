/*
 * Copyright 2016-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.data.cassandra.core.query.Criteria.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import org.springframework.data.cassandra.CassandraConnectionFailureException;
import org.springframework.data.cassandra.core.mapping.event.BeforeConvertCallback;
import org.springframework.data.cassandra.core.mapping.event.BeforeSaveCallback;
import org.springframework.data.cassandra.core.query.Filter;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.cassandra.core.query.Update;
import org.springframework.data.cassandra.domain.User;
import org.springframework.data.cassandra.domain.VersionedUser;
import org.springframework.data.mapping.callback.EntityCallbacks;
import org.springframework.util.concurrent.ListenableFuture;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.NoNodeAvailableException;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;

/**
 * Unit tests for {@link AsyncCassandraTemplate}.
 *
 * @author Mark Paluch
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class AsyncCassandraTemplateUnitTests {

	@Mock CqlSession session;
	@Mock AsyncResultSet resultSet;
	@Mock Row row;
	@Mock ColumnDefinition columnDefinition;
	@Mock ColumnDefinitions columnDefinitions;

	@Captor ArgumentCaptor<SimpleStatement> statementCaptor;

	private AsyncCassandraTemplate template;

	private Object beforeSave;

	private Object beforeConvert;

	@BeforeEach
	void setUp() {

		template = new AsyncCassandraTemplate(session);
		template.setUsePreparedStatements(false);

		when(session.executeAsync(any(Statement.class))).thenReturn(new TestResultSetFuture(resultSet));
		when(row.getColumnDefinitions()).thenReturn(columnDefinitions);

		EntityCallbacks callbacks = EntityCallbacks.create();
		callbacks.addEntityCallback((BeforeSaveCallback<Object>) (entity, tableName, statement) -> {

			assertThat(tableName).isNotNull();
			assertThat(statement).isNotNull();
			beforeSave = entity;
			return entity;
		});

		callbacks.addEntityCallback((BeforeConvertCallback<Object>) (entity, tableName) -> {

			assertThat(tableName).isNotNull();
			beforeConvert = entity;
			return entity;
		});

		template.setEntityCallbacks(callbacks);
	}

	@Test // DATACASS-292
	void selectUsingCqlShouldReturnMappedResults() {

		when(resultSet.currentPage()).thenReturn(Collections.singleton(row));
		when(columnDefinitions.contains(any(CqlIdentifier.class))).thenReturn(true);

		when(columnDefinitions.get(anyInt())).thenReturn(columnDefinition);
		when(columnDefinitions.firstIndexOf("id")).thenReturn(0);
		when(columnDefinitions.firstIndexOf("firstname")).thenReturn(1);
		when(columnDefinitions.firstIndexOf("lastname")).thenReturn(2);

		when(columnDefinition.getType()).thenReturn(DataTypes.TEXT);

		when(row.getObject(0)).thenReturn("myid");
		when(row.getObject(1)).thenReturn("Walter");
		when(row.getObject(2)).thenReturn("White");

		ListenableFuture<List<User>> list = template.select("SELECT * FROM users", User.class);

		assertThat(getUninterruptibly(list)).hasSize(1).contains(new User("myid", "Walter", "White"));
		verify(session).executeAsync(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue())).isEqualTo("SELECT * FROM users");
	}

	@Test // DATACASS-292
	void selectUsingCqlShouldInvokeCallbackWithMappedResults() {

		when(resultSet.currentPage()).thenReturn(Collections.singletonList(row));
		when(columnDefinitions.contains(any(CqlIdentifier.class))).thenReturn(true);
		when(columnDefinitions.get(anyInt())).thenReturn(columnDefinition);
		when(columnDefinitions.firstIndexOf("id")).thenReturn(0);
		when(columnDefinitions.firstIndexOf("firstname")).thenReturn(1);
		when(columnDefinitions.firstIndexOf("lastname")).thenReturn(2);

		when(columnDefinition.getType()).thenReturn(DataTypes.TEXT);

		when(row.getObject(0)).thenReturn("myid");
		when(row.getObject(1)).thenReturn("Walter");
		when(row.getObject(2)).thenReturn("White");

		List<User> list = new ArrayList<>();

		ListenableFuture<Void> result = template.select("SELECT * FROM users", list::add, User.class);

		assertThat(getUninterruptibly(result)).isNull();
		assertThat(list).hasSize(1).contains(new User("myid", "Walter", "White"));
		verify(session).executeAsync(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue())).isEqualTo("SELECT * FROM users");
	}

	@Test // DATACASS-292
	void selectShouldTranslateException() throws Exception {

		when(resultSet.currentPage()).thenThrow(new NoNodeAvailableException());

		ListenableFuture<List<User>> list = template.select("SELECT * FROM users", User.class);

		try {
			list.get();

			fail("Missing CassandraConnectionFailureException");
		} catch (ExecutionException e) {
			assertThat(e).hasCauseInstanceOf(CassandraConnectionFailureException.class)
					.hasRootCauseInstanceOf(NoNodeAvailableException.class);
		}
	}

	@Test // DATACASS-292
	void selectOneShouldReturnMappedResults() {

		when(resultSet.currentPage()).thenReturn(Collections.singleton(row));
		when(columnDefinitions.contains(any(CqlIdentifier.class))).thenReturn(true);

		when(columnDefinitions.get(anyInt())).thenReturn(columnDefinition);
		when(columnDefinitions.firstIndexOf("id")).thenReturn(0);
		when(columnDefinitions.firstIndexOf("firstname")).thenReturn(1);
		when(columnDefinitions.firstIndexOf("lastname")).thenReturn(2);

		when(columnDefinition.getType()).thenReturn(DataTypes.TEXT);

		when(row.getObject(0)).thenReturn("myid");
		when(row.getObject(1)).thenReturn("Walter");
		when(row.getObject(2)).thenReturn("White");

		ListenableFuture<User> future = template.selectOne("SELECT * FROM users WHERE id='myid'", User.class);

		assertThat(getUninterruptibly(future)).isEqualTo(new User("myid", "Walter", "White"));
		verify(session).executeAsync(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue())).isEqualTo("SELECT * FROM users WHERE id='myid'");
	}

	@Test // DATACASS-292
	void selectOneByIdShouldReturnMappedResults() {

		when(resultSet.currentPage()).thenReturn(Collections.singleton(row));
		when(columnDefinitions.contains(any(CqlIdentifier.class))).thenReturn(true);
		when(columnDefinitions.get(anyInt())).thenReturn(columnDefinition);
		when(columnDefinitions.firstIndexOf("id")).thenReturn(0);
		when(columnDefinitions.firstIndexOf("firstname")).thenReturn(1);
		when(columnDefinitions.firstIndexOf("lastname")).thenReturn(2);

		when(columnDefinition.getType()).thenReturn(DataTypes.ASCII);

		when(row.getObject(0)).thenReturn("myid");
		when(row.getObject(1)).thenReturn("Walter");
		when(row.getObject(2)).thenReturn("White");

		ListenableFuture<User> future = template.selectOneById("myid", User.class);

		assertThat(getUninterruptibly(future)).isEqualTo(new User("myid", "Walter", "White"));
		verify(session).executeAsync(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue())).isEqualTo("SELECT * FROM users WHERE id='myid' LIMIT 1");
	}

	@Test // DATACASS-696
	void selectOneShouldNull() {

		when(resultSet.currentPage()).thenReturn(Collections.singleton(row));

		ListenableFuture<String> future = template.selectOne("SELECT id FROM users WHERE id='myid'", String.class);

		assertThat(getUninterruptibly(future)).isNull();
	}

	@Test // DATACASS-292
	void existsShouldReturnExistingElement() {

		when(resultSet.one()).thenReturn(row);

		ListenableFuture<Boolean> future = template.exists("myid", User.class);

		assertThat(getUninterruptibly(future)).isTrue();
		verify(session).executeAsync(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue())).isEqualTo("SELECT * FROM users WHERE id='myid' LIMIT 1");
	}

	@Test // DATACASS-292
	void existsShouldReturnNonExistingElement() {

		ListenableFuture<Boolean> future = template.exists("myid", User.class);

		assertThat(getUninterruptibly(future)).isFalse();
		verify(session).executeAsync(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue())).isEqualTo("SELECT * FROM users WHERE id='myid' LIMIT 1");
	}

	@Test // DATACASS-512
	void existsByQueryShouldReturnExistingElement() {

		when(resultSet.one()).thenReturn(row);

		ListenableFuture<Boolean> future = template.exists(Query.empty(), User.class);

		assertThat(getUninterruptibly(future)).isTrue();
		verify(session).executeAsync(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue())).isEqualTo("SELECT * FROM users LIMIT 1");
	}

	@Test // DATACASS-292
	void countShouldExecuteCountQueryElement() {

		when(resultSet.currentPage()).thenReturn(Collections.singleton(row));
		when(row.getLong(0)).thenReturn(42L);
		when(columnDefinitions.size()).thenReturn(1);

		ListenableFuture<Long> future = template.count(User.class);

		assertThat(getUninterruptibly(future)).isEqualTo(42L);
		verify(session).executeAsync(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue())).isEqualTo("SELECT count(1) FROM users");
	}

	@Test // DATACASS-292
	void countByQueryShouldExecuteCountQueryElement() {

		when(resultSet.currentPage()).thenReturn(Collections.singleton(row));
		when(row.getLong(0)).thenReturn(42L);
		when(columnDefinitions.size()).thenReturn(1);

		ListenableFuture<Long> future = template.count(Query.empty(), User.class);

		assertThat(getUninterruptibly(future)).isEqualTo(42L);
		verify(session).executeAsync(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue())).isEqualTo("SELECT count(1) FROM users");
	}

	@Test // DATACASS-292, DATACASS-618
	void insertShouldInsertEntity() {

		when(resultSet.wasApplied()).thenReturn(true);

		User user = new User("heisenberg", "Walter", "White");

		ListenableFuture<User> future = template.insert(user);

		assertThat(getUninterruptibly(future)).isEqualTo(user);
		verify(session).executeAsync(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue()))
				.isEqualTo("INSERT INTO users (firstname,id,lastname) VALUES ('Walter','heisenberg','White')");
		assertThat(beforeConvert).isSameAs(user);
		assertThat(beforeSave).isSameAs(user);
	}

	@Test // DATACASS-618
	void insertShouldInsertVersionedEntity() {

		when(resultSet.wasApplied()).thenReturn(true);

		VersionedUser user = new VersionedUser("heisenberg", "Walter", "White");

		ListenableFuture<VersionedUser> future = template.insert(user);

		assertThat(getUninterruptibly(future)).isEqualTo(user);
		verify(session).executeAsync(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue())).isEqualTo(
				"INSERT INTO vusers (firstname,id,lastname,version) VALUES ('Walter','heisenberg','White',0) IF NOT EXISTS");
		assertThat(beforeConvert).isSameAs(user);
		assertThat(beforeSave).isSameAs(user);
	}

	@Test // DATACASS-292
	void insertShouldTranslateException() throws Exception {

		reset(session);
		when(session.executeAsync(any(Statement.class)))
				.thenReturn(TestResultSetFuture.failed(new NoNodeAvailableException()));

		ListenableFuture<User> future = template.insert(new User("heisenberg", "Walter", "White"));

		try {
			future.get();

			fail("Missing CassandraConnectionFailureException");
		} catch (ExecutionException e) {
			assertThat(e).hasCauseInstanceOf(CassandraConnectionFailureException.class)
					.hasRootCauseInstanceOf(NoNodeAvailableException.class);
		}
	}

	@Test // DATACASS-292, DATACASS-618
	void updateShouldUpdateEntity() {

		when(resultSet.wasApplied()).thenReturn(true);

		User user = new User("heisenberg", "Walter", "White");

		ListenableFuture<User> future = template.update(user);

		assertThat(getUninterruptibly(future)).isEqualTo(user);
		verify(session).executeAsync(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue()))
				.isEqualTo("UPDATE users SET firstname='Walter', lastname='White' WHERE id='heisenberg'");
		assertThat(beforeConvert).isSameAs(user);
		assertThat(beforeSave).isSameAs(user);
	}

	@Test // DATACASS-618
	void updateShouldUpdateVersionedEntity() {

		when(resultSet.wasApplied()).thenReturn(true);

		VersionedUser user = new VersionedUser("heisenberg", "Walter", "White");
		user.setVersion(0L);

		ListenableFuture<VersionedUser> future = template.update(user);

		assertThat(getUninterruptibly(future)).isEqualTo(user);
		verify(session).executeAsync(statementCaptor.capture());
		SimpleStatement value = statementCaptor.getValue();
		assertThat(render(value)).isEqualTo(
				"UPDATE vusers SET firstname='Walter', lastname='White', version=1 WHERE id='heisenberg' IF version=0");
		assertThat(beforeConvert).isSameAs(user);
		assertThat(beforeSave).isSameAs(user);
	}

	@Test // DATACASS-575
	void updateShouldUpdateEntityWithOptions() {

		UpdateOptions updateOptions = UpdateOptions.builder().withIfExists().build();
		User user = new User("heisenberg", "Walter", "White");

		template.update(user, updateOptions);

		verify(session).executeAsync(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue()))
				.isEqualTo("UPDATE users SET firstname='Walter', lastname='White' WHERE id='heisenberg' IF EXISTS");
	}

	@Test // DATACASS-575
	void updateShouldUpdateEntityWithLwt() {

		UpdateOptions options = UpdateOptions.builder().ifCondition(where("firstname").is("Walter")).build();
		User user = new User("heisenberg", "Walter", "White");

		template.update(user, options);

		verify(session).executeAsync(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue()))
				.isEqualTo("UPDATE users SET firstname='Walter', lastname='White' WHERE id='heisenberg' IF firstname='Walter'");
	}

	@Test // DATACASS-575
	void updateShouldApplyUpdateQuery() {

		Query query = Query.query(where("id").is("heisenberg"));
		Update update = Update.update("firstname", "Walter");

		template.update(query, update, User.class);

		verify(session).executeAsync(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue()))
				.isEqualTo("UPDATE users SET firstname='Walter' WHERE id='heisenberg'");
	}

	@Test // DATACASS-575
	void updateShouldApplyUpdateQueryWitLwt() {

		Filter ifCondition = Filter.from(where("firstname").is("Walter"), where("lastname").is("White"));

		Query query = Query.query(where("id").is("heisenberg"))
				.queryOptions(UpdateOptions.builder().ifCondition(ifCondition).build());

		Update update = Update.update("firstname", "Walter");

		template.update(query, update, User.class);

		verify(session).executeAsync(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue())).isEqualTo(
				"UPDATE users SET firstname='Walter' WHERE id='heisenberg' IF firstname='Walter' AND lastname='White'");
	}

	@Test // DATACASS-292
	void updateShouldTranslateException() throws Exception {

		reset(session);
		when(session.executeAsync(any(Statement.class)))
				.thenReturn(TestResultSetFuture.failed(new NoNodeAvailableException()));

		ListenableFuture<User> future = template.update(new User("heisenberg", "Walter", "White"));

		try {
			future.get();

			fail("Missing CassandraConnectionFailureException");
		} catch (ExecutionException e) {
			assertThat(e).hasCauseInstanceOf(CassandraConnectionFailureException.class)
					.hasRootCauseInstanceOf(NoNodeAvailableException.class);
		}
	}

	@Test // DATACASS-292
	void deleteByIdShouldRemoveEntity() {

		when(resultSet.wasApplied()).thenReturn(true);

		User user = new User("heisenberg", "Walter", "White");

		ListenableFuture<Boolean> future = template.deleteById(user.getId(), User.class);

		assertThat(getUninterruptibly(future)).isTrue();
		verify(session).executeAsync(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue())).isEqualTo("DELETE FROM users WHERE id='heisenberg'");
	}

	@Test // DATACASS-292
	void deleteShouldRemoveEntity() {

		when(resultSet.wasApplied()).thenReturn(true);

		User user = new User("heisenberg", "Walter", "White");

		ListenableFuture<User> future = template.delete(user);

		assertThat(getUninterruptibly(future)).isEqualTo(user);
		verify(session).executeAsync(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue())).isEqualTo("DELETE FROM users WHERE id='heisenberg'");
	}

	@Test // DATACASS-575
	void deleteShouldRemoveEntityWithLwt() {

		User user = new User("heisenberg", "Walter", "White");
		DeleteOptions options = DeleteOptions.builder().ifCondition(where("firstname").is("Walter")).build();

		template.delete(user, options);

		verify(session).executeAsync(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue()))
				.isEqualTo("DELETE FROM users WHERE id='heisenberg' IF firstname='Walter'");
	}

	@Test // DATACASS-575
	void deleteShouldRemoveByQueryWithLwt() {

		DeleteOptions options = DeleteOptions.builder().ifCondition(where("firstname").is("Walter")).build();
		Query query = Query.query(where("id").is("heisenberg")).queryOptions(options);

		template.delete(query, User.class);

		verify(session).executeAsync(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue()))
				.isEqualTo("DELETE FROM users WHERE id='heisenberg' IF firstname='Walter'");
	}

	@Test // DATACASS-292
	void deleteShouldTranslateException() throws Exception {

		reset(session);
		when(session.executeAsync(any(Statement.class)))
				.thenReturn(TestResultSetFuture.failed(new NoNodeAvailableException()));

		ListenableFuture<User> future = template.delete(new User("heisenberg", "Walter", "White"));

		try {
			future.get();

			fail("Missing CassandraConnectionFailureException");
		} catch (ExecutionException e) {
			assertThat(e).hasCauseInstanceOf(CassandraConnectionFailureException.class)
					.hasRootCauseInstanceOf(NoNodeAvailableException.class);
		}
	}

	@Test // DATACASS-292
	void truncateShouldRemoveEntities() {

		template.truncate(User.class);

		verify(session).executeAsync(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue())).isEqualTo("TRUNCATE users");
	}

	private static String render(SimpleStatement statement) {

		String query = statement.getQuery();
		List<Object> positionalValues = statement.getPositionalValues();
		for (Object positionalValue : positionalValues) {

			query = query.replaceFirst("\\?",
					positionalValue != null
							? CodecRegistry.DEFAULT.codecFor((Class) positionalValue.getClass()).format(positionalValue)
							: "NULL");
		}

		return query;
	}

	private static <T> T getUninterruptibly(Future<T> future) {

		try {
			return future.get();
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	private static class TestResultSetFuture extends CompletableFuture<AsyncResultSet> {

		private TestResultSetFuture() {}

		private TestResultSetFuture(AsyncResultSet resultSet) {
			complete(resultSet);
		}

		/**
		 * Create a completed future that reports a failure given {@link Throwable}.
		 *
		 * @param throwable must not be {@literal null}.
		 * @return the completed/failed {@link TestResultSetFuture}.
		 */
		private static TestResultSetFuture failed(Throwable throwable) {

			TestResultSetFuture future = new TestResultSetFuture();
			future.completeExceptionally(throwable);
			return future;
		}
	}
}
