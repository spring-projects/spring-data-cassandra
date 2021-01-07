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

import java.util.Collections;
import java.util.List;

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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.NoNodeAvailableException;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;

/**
 * Unit tests for {@link CassandraTemplate}.
 *
 * @author Mark Paluch
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class CassandraTemplateUnitTests {

	@Mock CqlSession session;
	@Mock ResultSet resultSet;
	@Mock Row row;
	@Mock ColumnDefinition columnDefinition;
	@Mock ColumnDefinitions columnDefinitions;

	@Captor ArgumentCaptor<SimpleStatement> statementCaptor;

	private CassandraTemplate template;

	private Object beforeSave;

	private Object beforeConvert;

	@BeforeEach
	void setUp() {

		template = new CassandraTemplate(session);
		template.setUsePreparedStatements(false);

		when(session.execute(any(Statement.class))).thenReturn(resultSet);
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

		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());
		when(columnDefinitions.contains(any(CqlIdentifier.class))).thenReturn(true);

		when(columnDefinitions.get(anyInt())).thenReturn(columnDefinition);
		when(columnDefinitions.firstIndexOf("id")).thenReturn(0);
		when(columnDefinitions.firstIndexOf("firstname")).thenReturn(1);
		when(columnDefinitions.firstIndexOf("lastname")).thenReturn(2);

		when(columnDefinition.getType()).thenReturn(DataTypes.TEXT);

		when(row.getObject(0)).thenReturn("myid");
		when(row.getObject(1)).thenReturn("Walter");
		when(row.getObject(2)).thenReturn("White");

		List<User> list = template.select("SELECT * FROM users", User.class);

		assertThat(list).hasSize(1).contains(new User("myid", "Walter", "White"));
		verify(session).execute(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue())).isEqualTo("SELECT * FROM users");
	}

	@Test // DATACASS-292
	void selectShouldTranslateException() {

		when(resultSet.iterator()).thenThrow(new NoNodeAvailableException());

		try {
			template.select("SELECT * FROM users", User.class);

			fail("Missing CassandraConnectionFailureException");
		} catch (CassandraConnectionFailureException e) {
			assertThat(e).hasRootCauseInstanceOf(NoNodeAvailableException.class);
		}
	}

	@Test // DATACASS-292
	void selectOneShouldReturnMappedResults() {

		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());
		when(columnDefinitions.contains(any(CqlIdentifier.class))).thenReturn(true);
		when(columnDefinitions.get(anyInt())).thenReturn(columnDefinition);
		when(columnDefinitions.firstIndexOf("id")).thenReturn(0);
		when(columnDefinitions.firstIndexOf("firstname")).thenReturn(1);
		when(columnDefinitions.firstIndexOf("lastname")).thenReturn(2);

		when(columnDefinition.getType()).thenReturn(DataTypes.TEXT);

		when(row.getObject(0)).thenReturn("myid");
		when(row.getObject(1)).thenReturn("Walter");
		when(row.getObject(2)).thenReturn("White");

		User user = template.selectOne("SELECT * FROM users WHERE id='myid'", User.class);

		assertThat(user).isEqualTo(new User("myid", "Walter", "White"));
		verify(session).execute(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue())).isEqualTo("SELECT * FROM users WHERE id='myid'");
	}

	@Test // DATACASS-696
	void selectOneShouldNull() {

		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());

		String nullValue = template.selectOne("SELECT id FROM users WHERE id='myid'", String.class);

		assertThat(nullValue).isNull();
	}

	@Test // DATACASS-292
	void selectOneByIdShouldReturnMappedResults() {

		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());
		when(columnDefinitions.contains(any(CqlIdentifier.class))).thenReturn(true);
		when(columnDefinitions.get(anyInt())).thenReturn(columnDefinition);
		when(columnDefinitions.firstIndexOf("id")).thenReturn(0);
		when(columnDefinitions.firstIndexOf("firstname")).thenReturn(1);
		when(columnDefinitions.firstIndexOf("lastname")).thenReturn(2);

		when(columnDefinition.getType()).thenReturn(DataTypes.ASCII);

		when(row.getObject(0)).thenReturn("myid");
		when(row.getObject(1)).thenReturn("Walter");
		when(row.getObject(2)).thenReturn("White");

		User user = template.selectOneById("myid", User.class);

		assertThat(user).isEqualTo(new User("myid", "Walter", "White"));
		verify(session).execute(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue())).isEqualTo("SELECT * FROM users WHERE id='myid' LIMIT 1");
	}

	@Test // DATACASS-313
	void selectProjectedOneShouldReturnMappedResults() {

		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());
		when(columnDefinitions.contains(any(CqlIdentifier.class))).thenReturn(true);
		when(columnDefinitions.get(anyInt())).thenReturn(columnDefinition);
		when(columnDefinitions.firstIndexOf("id")).thenReturn(0);

		when(columnDefinition.getType()).thenReturn(DataTypes.ASCII);

		when(row.getObject(0)).thenReturn("Walter");

		UserProjection user = template.query(User.class).as(UserProjection.class).oneValue();

		assertThat(user.getFirstname()).isEqualTo("Walter");
		verify(session).execute(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue())).isEqualTo("SELECT firstname FROM users LIMIT 2");
	}

	@Test // DATACASS-292
	void existsShouldReturnExistingElement() {

		when(resultSet.one()).thenReturn(row);

		boolean exists = template.exists("myid", User.class);

		assertThat(exists).isTrue();
		verify(session).execute(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue())).isEqualTo("SELECT * FROM users WHERE id='myid' LIMIT 1");
	}

	@Test // DATACASS-292
	void existsShouldReturnNonExistingElement() {

		boolean exists = template.exists("myid", User.class);

		assertThat(exists).isFalse();
		verify(session).execute(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue())).isEqualTo("SELECT * FROM users WHERE id='myid' LIMIT 1");
	}

	@Test // DATACASS-512
	void existsByQueryShouldReturnExistingElement() {

		when(resultSet.one()).thenReturn(row);

		boolean exists = template.exists(Query.empty(), User.class);

		assertThat(exists).isTrue();
		verify(session).execute(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue())).isEqualTo("SELECT * FROM users LIMIT 1");
	}

	@Test // DATACASS-292
	void countShouldExecuteCountQueryElement() {

		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());
		when(row.getLong(0)).thenReturn(42L);
		when(columnDefinitions.size()).thenReturn(1);

		long count = template.count(User.class);

		assertThat(count).isEqualTo(42L);
		verify(session).execute(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue())).isEqualTo("SELECT count(1) FROM users");
	}

	@Test // DATACASS-512
	void countByQueryShouldExecuteCountQueryElement() {

		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());
		when(row.getLong(0)).thenReturn(42L);
		when(columnDefinitions.size()).thenReturn(1);

		long count = template.count(Query.empty(), User.class);

		assertThat(count).isEqualTo(42L);
		verify(session).execute(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue())).isEqualTo("SELECT count(1) FROM users");
	}

	@Test // DATACASS-292, DATACASS-618
	void insertShouldInsertEntity() {

		when(resultSet.wasApplied()).thenReturn(true);

		User user = new User("heisenberg", "Walter", "White");

		template.insert(user);

		verify(session).execute(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue()))
				.isEqualTo("INSERT INTO users (firstname,id,lastname) VALUES ('Walter','heisenberg','White')");
		assertThat(beforeConvert).isSameAs(user);
		assertThat(beforeSave).isSameAs(user);
	}

	@Test // DATACASS-618
	void insertShouldInsertVersionedEntity() {

		when(resultSet.wasApplied()).thenReturn(true);

		VersionedUser user = new VersionedUser("heisenberg", "Walter", "White");

		template.insert(user);

		verify(session).execute(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue())).isEqualTo(
				"INSERT INTO vusers (firstname,id,lastname,version) VALUES ('Walter','heisenberg','White',0) IF NOT EXISTS");
		assertThat(beforeConvert).isSameAs(user);
		assertThat(beforeSave).isSameAs(user);
	}

	@Test // DATACASS-250
	void insertShouldInsertWithOptionsEntity() {

		InsertOptions insertOptions = InsertOptions.builder().withIfNotExists().build();

		when(resultSet.wasApplied()).thenReturn(true);

		User user = new User("heisenberg", "Walter", "White");

		template.insert(user, insertOptions);

		verify(session).execute(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue()))
				.isEqualTo("INSERT INTO users (firstname,id,lastname) VALUES ('Walter','heisenberg','White') IF NOT EXISTS");
	}

	@Test // DATACASS-560
	void insertShouldInsertWithNulls() {

		InsertOptions insertOptions = InsertOptions.builder().withInsertNulls().build();

		when(resultSet.wasApplied()).thenReturn(true);

		User user = new User("heisenberg", null, null);

		template.insert(user, insertOptions);

		verify(session).execute(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue()))
				.isEqualTo("INSERT INTO users (firstname,id,lastname) VALUES (NULL,'heisenberg',NULL)");
	}

	@Test // DATACASS-292
	void insertShouldTranslateException() {

		reset(session);
		when(session.execute(any(Statement.class))).thenThrow(new NoNodeAvailableException());

		try {
			template.insert(new User("heisenberg", "Walter", "White"));

			fail("Missing CassandraConnectionFailureException");
		} catch (CassandraConnectionFailureException e) {
			assertThat(e).hasRootCauseInstanceOf(NoNodeAvailableException.class);
		}
	}

	@Test // DATACASS-292
	void insertShouldNotApplyInsert() {

		when(resultSet.wasApplied()).thenReturn(false);

		User user = new User("heisenberg", "Walter", "White");

		WriteResult writeResult = template.insert(user, InsertOptions.builder().build());

		assertThat(writeResult.wasApplied()).isFalse();
	}

	@Test // DATACASS-292, DATACASS-618
	void updateShouldUpdateEntity() {

		when(resultSet.wasApplied()).thenReturn(true);

		User user = new User("heisenberg", "Walter", "White");

		template.update(user);

		verify(session).execute(statementCaptor.capture());
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

		template.update(user);

		verify(session).execute(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue())).isEqualTo(
				"UPDATE vusers SET firstname='Walter', lastname='White', version=1 WHERE id='heisenberg' IF version=0");
		assertThat(beforeConvert).isSameAs(user);
		assertThat(beforeSave).isSameAs(user);
	}

	@Test // DATACASS-250
	void updateShouldUpdateEntityWithOptions() {

		when(resultSet.wasApplied()).thenReturn(true);

		UpdateOptions updateOptions = UpdateOptions.builder().withIfExists().build();

		User user = new User("heisenberg", "Walter", "White");

		WriteResult writeResult = template.update(user, updateOptions);

		assertThat(writeResult.wasApplied()).isTrue();
		verify(session).execute(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue()))
				.isEqualTo("UPDATE users SET firstname='Walter', lastname='White' WHERE id='heisenberg' IF EXISTS");
	}

	@Test // DATACASS-575
	void updateShouldUpdateEntityWithLwt() {

		UpdateOptions options = UpdateOptions.builder().ifCondition(where("firstname").is("Walter")).build();
		User user = new User("heisenberg", "Walter", "White");

		template.update(user, options);

		verify(session).execute(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue()))
				.isEqualTo("UPDATE users SET firstname='Walter', lastname='White' WHERE id='heisenberg' IF firstname='Walter'");
	}

	@Test // DATACASS-575
	void updateShouldApplyUpdateQuery() {

		Query query = Query.query(where("id").is("heisenberg"));
		Update update = Update.update("firstname", "Walter");

		template.update(query, update, User.class);

		verify(session).execute(statementCaptor.capture());
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

		verify(session).execute(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue())).isEqualTo(
				"UPDATE users SET firstname='Walter' WHERE id='heisenberg' IF firstname='Walter' AND lastname='White'");
	}

	@Test // DATACASS-292
	void updateShouldTranslateException() {

		reset(session);
		when(session.execute(any(Statement.class))).thenThrow(new NoNodeAvailableException());

		try {
			template.update(new User("heisenberg", "Walter", "White"));

			fail("Missing CassandraConnectionFailureException");
		} catch (CassandraConnectionFailureException e) {
			assertThat(e).hasRootCauseInstanceOf(NoNodeAvailableException.class);
		}
	}

	@Test // DATACASS-292
	void deleteByIdShouldRemoveEntity() {

		when(resultSet.wasApplied()).thenReturn(true);

		User user = new User("heisenberg", "Walter", "White");

		boolean deleted = template.deleteById(user.getId(), User.class);

		assertThat(deleted).isTrue();
		verify(session).execute(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue())).isEqualTo("DELETE FROM users WHERE id='heisenberg'");
	}

	@Test // DATACASS-292
	void deleteShouldRemoveEntity() {

		User user = new User("heisenberg", "Walter", "White");

		template.delete(user);

		verify(session).execute(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue())).isEqualTo("DELETE FROM users WHERE id='heisenberg'");
	}

	@Test // DATACASS-575
	void deleteShouldRemoveEntityWithLwt() {

		User user = new User("heisenberg", "Walter", "White");
		DeleteOptions options = DeleteOptions.builder().ifCondition(where("firstname").is("Walter")).build();

		template.delete(user, options);

		verify(session).execute(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue()))
				.isEqualTo("DELETE FROM users WHERE id='heisenberg' IF firstname='Walter'");
	}

	@Test // DATACASS-575
	void deleteShouldRemoveByQueryWithLwt() {

		DeleteOptions options = DeleteOptions.builder().ifCondition(where("firstname").is("Walter")).build();
		Query query = Query.query(where("id").is("heisenberg")).queryOptions(options);

		template.delete(query, User.class);

		verify(session).execute(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue()))
				.isEqualTo("DELETE FROM users WHERE id='heisenberg' IF firstname='Walter'");
	}

	@Test // DATACASS-292
	void deleteShouldTranslateException() {

		reset(session);
		when(session.execute(any(Statement.class))).thenThrow(new NoNodeAvailableException());

		try {
			template.delete(new User("heisenberg", "Walter", "White"));

			fail("Missing CassandraConnectionFailureException");
		} catch (CassandraConnectionFailureException e) {
			assertThat(e).hasRootCauseInstanceOf(NoNodeAvailableException.class);
		}
	}

	@Test // DATACASS-292
	void truncateShouldRemoveEntities() {

		template.truncate(User.class);

		verify(session).execute(statementCaptor.capture());
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

	private interface UserProjection {
		String getFirstname();
	}
}
