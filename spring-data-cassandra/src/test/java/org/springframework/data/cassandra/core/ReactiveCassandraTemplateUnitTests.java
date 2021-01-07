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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

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

import org.springframework.data.cassandra.ReactiveResultSet;
import org.springframework.data.cassandra.ReactiveSession;
import org.springframework.data.cassandra.core.mapping.event.ReactiveBeforeConvertCallback;
import org.springframework.data.cassandra.core.mapping.event.ReactiveBeforeSaveCallback;
import org.springframework.data.cassandra.core.query.Filter;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.cassandra.core.query.Update;
import org.springframework.data.cassandra.domain.User;
import org.springframework.data.cassandra.domain.VersionedUser;
import org.springframework.data.mapping.callback.ReactiveEntityCallbacks;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.NoNodeAvailableException;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;

/**
 * Unit tests for {@link ReactiveCassandraTemplate}.
 *
 * @author Mark Paluch
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ReactiveCassandraTemplateUnitTests {

	@Mock ReactiveSession session;
	@Mock ReactiveResultSet reactiveResultSet;
	@Mock Row row;
	@Mock ColumnDefinition columnDefinition;
	@Mock ColumnDefinitions columnDefinitions;

	@Captor ArgumentCaptor<SimpleStatement> statementCaptor;

	private ReactiveCassandraTemplate template;

	private Object beforeSave;

	private Object beforeConvert;

	@BeforeEach
	void setUp() {

		template = new ReactiveCassandraTemplate(session);
		template.setUsePreparedStatements(false);

		when(session.execute(any(Statement.class))).thenReturn(Mono.just(reactiveResultSet));
		when(row.getColumnDefinitions()).thenReturn(columnDefinitions);

		ReactiveEntityCallbacks callbacks = ReactiveEntityCallbacks.create();
		callbacks.addEntityCallback((ReactiveBeforeSaveCallback<Object>) (entity, tableName, statement) -> {

			assertThat(tableName).isNotNull();
			assertThat(statement).isNotNull();
			beforeSave = entity;
			return Mono.just(entity);
		});

		callbacks.addEntityCallback((ReactiveBeforeConvertCallback<Object>) (entity, tableName) -> {

			assertThat(tableName).isNotNull();
			beforeConvert = entity;
			return Mono.just(entity);
		});

		template.setEntityCallbacks(callbacks);
	}

	@Test // DATACASS-335
	void selectUsingCqlShouldReturnMappedResults() {

		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));
		when(columnDefinitions.contains(any(CqlIdentifier.class))).thenReturn(true);

		when(columnDefinitions.get(anyInt())).thenReturn(columnDefinition);
		when(columnDefinitions.firstIndexOf("id")).thenReturn(0);
		when(columnDefinitions.firstIndexOf("firstname")).thenReturn(1);
		when(columnDefinitions.firstIndexOf("lastname")).thenReturn(2);

		when(columnDefinition.getType()).thenReturn(DataTypes.TEXT);

		when(row.getObject(0)).thenReturn("myid");
		when(row.getObject(1)).thenReturn("Walter");
		when(row.getObject(2)).thenReturn("White");

		template.select("SELECT * FROM users", User.class).as(StepVerifier::create) //
				.expectNext(new User("myid", "Walter", "White")) //
				.verifyComplete();

		verify(session).execute(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue())).isEqualTo("SELECT * FROM users");
	}

	@Test // DATACASS-335
	void selectShouldTranslateException() {

		when(reactiveResultSet.rows()).thenThrow(new NoNodeAvailableException());

		template.select("SELECT * FROM users", User.class).as(StepVerifier::create) //
				.consumeErrorWith(e -> {
					assertThat(e).hasRootCauseInstanceOf(NoNodeAvailableException.class);
				}).verify();
	}

	@Test // DATACASS-335
	void selectOneByIdShouldReturnMappedResults() {

		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));
		when(columnDefinitions.contains(any(CqlIdentifier.class))).thenReturn(true);
		when(columnDefinitions.get(anyInt())).thenReturn(columnDefinition);
		when(columnDefinitions.firstIndexOf("id")).thenReturn(0);
		when(columnDefinitions.firstIndexOf("firstname")).thenReturn(1);
		when(columnDefinitions.firstIndexOf("lastname")).thenReturn(2);

		when(columnDefinition.getType()).thenReturn(DataTypes.ASCII);

		when(row.getObject(0)).thenReturn("myid");
		when(row.getObject(1)).thenReturn("Walter");
		when(row.getObject(2)).thenReturn("White");

		template.selectOneById("myid", User.class).as(StepVerifier::create) //
				.expectNext(new User("myid", "Walter", "White")) //
				.verifyComplete();

		verify(session).execute(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue())).isEqualTo("SELECT * FROM users WHERE id='myid' LIMIT 1");
	}

	@Test // DATACASS-313
	void selectProjectedOneShouldReturnMappedResults() {

		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));
		when(columnDefinitions.contains(any(CqlIdentifier.class))).thenReturn(true);
		when(columnDefinitions.get(anyInt())).thenReturn(columnDefinition);
		when(columnDefinitions.firstIndexOf("id")).thenReturn(0);

		when(columnDefinition.getType()).thenReturn(DataTypes.ASCII);

		when(row.getObject(0)).thenReturn("Walter");

		template.query(User.class).as(UserProjection.class).first() //
				.as(StepVerifier::create) //
				.assertNext(actual -> {

					assertThat(actual.getFirstname()).isEqualTo("Walter");
				}).verifyComplete();

		verify(session).execute(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue())).isEqualTo("SELECT firstname FROM users LIMIT 1");
	}

	@Test // DATACASS-696
	void selectOneShouldNull() {

		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));

		template.selectOne("SELECT id FROM users WHERE id='myid'", String.class).as(StepVerifier::create) //
				.verifyComplete();
	}

	@Test // DATACASS-335
	void existsShouldReturnExistingElement() {

		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));

		template.exists("myid", User.class).as(StepVerifier::create).expectNext(true).verifyComplete();

		verify(session).execute(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue())).isEqualTo("SELECT * FROM users WHERE id='myid' LIMIT 1");
	}

	@Test // DATACASS-335
	void existsShouldReturnNonExistingElement() {

		when(reactiveResultSet.rows()).thenReturn(Flux.empty());

		template.exists("myid", User.class).as(StepVerifier::create).expectNext(false).verifyComplete();

		verify(session).execute(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue())).isEqualTo("SELECT * FROM users WHERE id='myid' LIMIT 1");
	}

	@Test // DATACASS-512
	void existsByQueryShouldReturnExistingElement() {

		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));

		template.exists(Query.empty(), User.class).as(StepVerifier::create).expectNext(true).verifyComplete();

		verify(session).execute(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue())).isEqualTo("SELECT * FROM users LIMIT 1");
	}

	@Test // DATACASS-512
	void existsByQueryShouldReturnNonExistingElement() {

		when(reactiveResultSet.rows()).thenReturn(Flux.empty());

		template.exists(Query.empty(), User.class).as(StepVerifier::create).expectNext(false).verifyComplete();

		verify(session).execute(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue())).isEqualTo("SELECT * FROM users LIMIT 1");
	}

	@Test // DATACASS-335
	void countShouldExecuteCountQueryElement() {

		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));
		when(row.getLong(0)).thenReturn(42L);
		when(columnDefinitions.size()).thenReturn(1);

		template.count(User.class).as(StepVerifier::create).expectNext(42L).verifyComplete();

		verify(session).execute(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue())).isEqualTo("SELECT count(1) FROM users");
	}

	@Test // DATACASS-512
	void countByQueryShouldExecuteCountQueryElement() {

		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));
		when(row.getLong(0)).thenReturn(42L);
		when(columnDefinitions.size()).thenReturn(1);

		template.count(Query.empty(), User.class).as(StepVerifier::create).expectNext(42L).verifyComplete();

		verify(session).execute(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue())).isEqualTo("SELECT count(1) FROM users");
	}

	@Test // DATACASS-335, DATACASS-618
	void insertShouldInsertEntity() {

		when(reactiveResultSet.wasApplied()).thenReturn(true);
		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));

		User user = new User("heisenberg", "Walter", "White");
		template.insert(user).as(StepVerifier::create).expectNext(user).verifyComplete();

		verify(session).execute(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue()))
				.isEqualTo("INSERT INTO users (firstname,id,lastname) VALUES ('Walter','heisenberg','White')");
		assertThat(beforeConvert).isSameAs(user);
		assertThat(beforeSave).isSameAs(user);
	}

	@Test // DATACASS-618
	void insertShouldInsertVersionedEntity() {

		when(reactiveResultSet.wasApplied()).thenReturn(true);
		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));

		VersionedUser user = new VersionedUser("heisenberg", "Walter", "White");
		StepVerifier.create(template.insert(user)).expectNext(user).verifyComplete();

		verify(session).execute(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue())).isEqualTo(
				"INSERT INTO vusers (firstname,id,lastname,version) VALUES ('Walter','heisenberg','White',0) IF NOT EXISTS");
		assertThat(beforeConvert).isSameAs(user);
		assertThat(beforeSave).isSameAs(user);
	}

	@Test // DATACASS-335
	void insertShouldTranslateException() {

		reset(session);
		when(session.execute(any(Statement.class))).thenReturn(Mono.error(new NoNodeAvailableException()));

		template.insert(new User("heisenberg", "Walter", "White")).as(StepVerifier::create) //
				.consumeErrorWith(e -> {

					assertThat(e).hasRootCauseInstanceOf(NoNodeAvailableException.class);
				}).verify();
	}

	@Test // DATACASS-335, DATACASS-618
	void updateShouldUpdateEntity() {

		when(reactiveResultSet.wasApplied()).thenReturn(true);
		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));

		User user = new User("heisenberg", "Walter", "White");

		template.update(user).as(StepVerifier::create).expectNext(user).verifyComplete();

		verify(session).execute(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue()))
				.isEqualTo("UPDATE users SET firstname='Walter', lastname='White' WHERE id='heisenberg'");
		assertThat(beforeConvert).isSameAs(user);
		assertThat(beforeSave).isSameAs(user);
	}

	@Test // DATACASS-618
	void updateShouldUpdateVersionedEntity() {

		when(reactiveResultSet.wasApplied()).thenReturn(true);
		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));

		VersionedUser user = new VersionedUser("heisenberg", "Walter", "White");
		user.setVersion(0L);

		StepVerifier.create(template.update(user)).expectNext(user).verifyComplete();

		verify(session).execute(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue())).isEqualTo(
				"UPDATE vusers SET firstname='Walter', lastname='White', version=1 WHERE id='heisenberg' IF version=0");
		assertThat(beforeConvert).isSameAs(user);
		assertThat(beforeSave).isSameAs(user);
	}

	@Test // DATACASS-575
	void updateShouldUpdateEntityWithOptions() {

		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));

		UpdateOptions updateOptions = UpdateOptions.builder().withIfExists().build();
		User user = new User("heisenberg", "Walter", "White");

		template.update(user, updateOptions) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		verify(session).execute(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue()))
				.isEqualTo("UPDATE users SET firstname='Walter', lastname='White' WHERE id='heisenberg' IF EXISTS");
	}

	@Test // DATACASS-575
	void updateShouldUpdateEntityWithLwt() {

		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));

		UpdateOptions options = UpdateOptions.builder().ifCondition(where("firstname").is("Walter")).build();
		User user = new User("heisenberg", "Walter", "White");

		template.update(user, options) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		verify(session).execute(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue()))
				.isEqualTo("UPDATE users SET firstname='Walter', lastname='White' WHERE id='heisenberg' IF firstname='Walter'");
	}

	@Test // DATACASS-575
	void updateShouldApplyUpdateQuery() {

		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));

		Query query = Query.query(where("id").is("heisenberg"));
		Update update = Update.update("firstname", "Walter");

		template.update(query, update, User.class) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		verify(session).execute(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue()))
				.isEqualTo("UPDATE users SET firstname='Walter' WHERE id='heisenberg'");
	}

	@Test // DATACASS-575
	void updateShouldApplyUpdateQueryWitLwt() {

		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));

		Filter ifCondition = Filter.from(where("firstname").is("Walter"), where("lastname").is("White"));

		Query query = Query.query(where("id").is("heisenberg"))
				.queryOptions(UpdateOptions.builder().ifCondition(ifCondition).build());

		Update update = Update.update("firstname", "Walter");

		template.update(query, update, User.class) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		verify(session).execute(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue())).isEqualTo(
				"UPDATE users SET firstname='Walter' WHERE id='heisenberg' IF firstname='Walter' AND lastname='White'");
	}

	@Test // DATACASS-335
	void deleteShouldRemoveEntity() {

		when(reactiveResultSet.wasApplied()).thenReturn(true);
		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));

		User user = new User("heisenberg", "Walter", "White");

		template.delete(user).as(StepVerifier::create).expectNext(user).verifyComplete();

		verify(session).execute(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue())).isEqualTo("DELETE FROM users WHERE id='heisenberg'");
	}

	@Test // DATACASS-575
	void deleteShouldRemoveEntityWithLwt() {

		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));

		User user = new User("heisenberg", "Walter", "White");
		DeleteOptions options = DeleteOptions.builder().ifCondition(where("firstname").is("Walter")).build();

		template.delete(user, options) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		verify(session).execute(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue()))
				.isEqualTo("DELETE FROM users WHERE id='heisenberg' IF firstname='Walter'");
	}

	@Test // DATACASS-575
	void deleteShouldRemoveByQueryWithLwt() {

		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));

		DeleteOptions options = DeleteOptions.builder().ifCondition(where("firstname").is("Walter")).build();
		Query query = Query.query(where("id").is("heisenberg")).queryOptions(options);

		template.delete(query, User.class) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		verify(session).execute(statementCaptor.capture());
		assertThat(render(statementCaptor.getValue()))
				.isEqualTo("DELETE FROM users WHERE id='heisenberg' IF firstname='Walter'");
	}

	@Test // DATACASS-335
	void truncateShouldRemoveEntities() {

		template.truncate(User.class).as(StepVerifier::create).verifyComplete();

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
