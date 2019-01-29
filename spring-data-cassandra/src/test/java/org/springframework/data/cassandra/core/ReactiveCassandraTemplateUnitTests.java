/*
 * Copyright 2016-2019 the original author or authors.
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
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.data.cassandra.core.query.Criteria.*;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.cassandra.ReactiveResultSet;
import org.springframework.data.cassandra.ReactiveSession;
import org.springframework.data.cassandra.core.query.Filter;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.cassandra.core.query.Update;
import org.springframework.data.cassandra.domain.User;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

/**
 * Unit tests for {@link ReactiveCassandraTemplate}.
 *
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class ReactiveCassandraTemplateUnitTests {

	@Mock ReactiveSession session;
	@Mock ReactiveResultSet reactiveResultSet;
	@Mock Row row;
	@Mock ColumnDefinitions columnDefinitions;

	@Captor ArgumentCaptor<Statement> statementCaptor;

	ReactiveCassandraTemplate template;

	@Before
	public void setUp() {

		template = new ReactiveCassandraTemplate(session);

		when(session.execute(any(Statement.class))).thenReturn(Mono.just(reactiveResultSet));
		when(row.getColumnDefinitions()).thenReturn(columnDefinitions);
	}

	@Test // DATACASS-335
	public void selectUsingCqlShouldReturnMappedResults() {

		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));
		when(columnDefinitions.contains(anyString())).thenReturn(true);
		when(columnDefinitions.getType(anyInt())).thenReturn(DataType.ascii());

		when(columnDefinitions.getIndexOf("id")).thenReturn(0);
		when(columnDefinitions.getIndexOf("firstname")).thenReturn(1);
		when(columnDefinitions.getIndexOf("lastname")).thenReturn(2);

		when(row.getObject(0)).thenReturn("myid");
		when(row.getObject(1)).thenReturn("Walter");
		when(row.getObject(2)).thenReturn("White");

		StepVerifier.create(template.select("SELECT * FROM users", User.class)) //
				.expectNext(new User("myid", "Walter", "White")) //
				.verifyComplete();

		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("SELECT * FROM users");
	}

	@Test // DATACASS-335
	public void selectShouldTranslateException() {

		when(reactiveResultSet.rows()).thenThrow(new NoHostAvailableException(Collections.emptyMap()));

		StepVerifier.create(template.select("SELECT * FROM users", User.class)) //
				.consumeErrorWith(e -> {
					assertThat(e).hasRootCauseInstanceOf(NoHostAvailableException.class);
				}).verify();
	}

	@Test // DATACASS-335
	public void selectOneByIdShouldReturnMappedResults() {

		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));
		when(columnDefinitions.contains(anyString())).thenReturn(true);
		when(columnDefinitions.getType(anyInt())).thenReturn(DataType.ascii());

		when(columnDefinitions.getIndexOf("id")).thenReturn(0);
		when(columnDefinitions.getIndexOf("firstname")).thenReturn(1);
		when(columnDefinitions.getIndexOf("lastname")).thenReturn(2);

		when(row.getObject(0)).thenReturn("myid");
		when(row.getObject(1)).thenReturn("Walter");
		when(row.getObject(2)).thenReturn("White");

		StepVerifier.create(template.selectOneById("myid", User.class)) //
				.expectNext(new User("myid", "Walter", "White")) //
				.verifyComplete();

		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("SELECT * FROM users WHERE id='myid';");
	}

	@Test // DATACASS-313
	public void selectProjectedOneShouldReturnMappedResults() {

		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));
		when(columnDefinitions.contains(anyString())).thenReturn(true);
		when(columnDefinitions.getType(anyInt())).thenReturn(DataType.ascii());

		when(columnDefinitions.getIndexOf("firstname")).thenReturn(0);

		when(row.getObject(0)).thenReturn("Walter");

		template.query(User.class).as(UserProjection.class).first() //
				.as(StepVerifier::create) //
				.assertNext(actual -> {

					assertThat(actual.getFirstname()).isEqualTo("Walter");
				}).verifyComplete();

		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("SELECT firstname FROM users LIMIT 1;");
	}

	@Test // DATACASS-335
	public void existsShouldReturnExistingElement() {

		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));

		StepVerifier.create(template.exists("myid", User.class)).expectNext(true).verifyComplete();

		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("SELECT * FROM users WHERE id='myid';");
	}

	@Test // DATACASS-335
	public void existsShouldReturnNonExistingElement() {

		when(reactiveResultSet.rows()).thenReturn(Flux.empty());

		StepVerifier.create(template.exists("myid", User.class)).expectNext(false).verifyComplete();

		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("SELECT * FROM users WHERE id='myid';");
	}

	@Test // DATACASS-512
	public void existsByQueryShouldReturnExistingElement() {

		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));

		StepVerifier.create(template.exists(Query.empty(), User.class)).expectNext(true).verifyComplete();

		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("SELECT * FROM users LIMIT 1;");
	}

	@Test // DATACASS-512
	public void existsByQueryShouldReturnNonExistingElement() {

		when(reactiveResultSet.rows()).thenReturn(Flux.empty());

		StepVerifier.create(template.exists(Query.empty(), User.class)).expectNext(false).verifyComplete();

		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("SELECT * FROM users LIMIT 1;");
	}

	@Test // DATACASS-335
	public void countShouldExecuteCountQueryElement() {

		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));
		when(row.getLong(0)).thenReturn(42L);
		when(columnDefinitions.size()).thenReturn(1);

		StepVerifier.create(template.count(User.class)).expectNext(42L).verifyComplete();

		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("SELECT count(*) FROM users;");
	}

	@Test // DATACASS-512
	public void countByQueryShouldExecuteCountQueryElement() {

		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));
		when(row.getLong(0)).thenReturn(42L);
		when(columnDefinitions.size()).thenReturn(1);

		StepVerifier.create(template.count(Query.empty(), User.class)).expectNext(42L).verifyComplete();

		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("SELECT COUNT(1) FROM users;");
	}

	@Test // DATACASS-335
	public void insertShouldInsertEntity() {

		when(reactiveResultSet.wasApplied()).thenReturn(true);
		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));

		User user = new User("heisenberg", "Walter", "White");
		StepVerifier.create(template.insert(user)).expectNext(user).verifyComplete();

		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString())
				.isEqualTo("INSERT INTO users (firstname,id,lastname) VALUES ('Walter','heisenberg','White');");
	}

	@Test // DATACASS-335
	public void insertShouldTranslateException() {

		reset(session);
		when(session.execute(any(Statement.class)))
				.thenReturn(Mono.error(new NoHostAvailableException(Collections.emptyMap())));

		StepVerifier.create(template.insert(new User("heisenberg", "Walter", "White"))) //
				.consumeErrorWith(e -> {

					assertThat(e).hasRootCauseInstanceOf(NoHostAvailableException.class);
				}).verify();
	}

	@Test // DATACASS-335
	public void updateShouldUpdateEntity() {

		when(reactiveResultSet.wasApplied()).thenReturn(true);
		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));

		User user = new User("heisenberg", "Walter", "White");

		StepVerifier.create(template.update(user)).expectNext(user).verifyComplete();

		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString())
				.isEqualTo("UPDATE users SET firstname='Walter',lastname='White' WHERE id='heisenberg';");
	}

	@Test // DATACASS-575
	public void updateShouldUpdateEntityWithOptions() {

		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));

		UpdateOptions updateOptions = UpdateOptions.builder().withIfExists().build();
		User user = new User("heisenberg", "Walter", "White");

		template.update(user, updateOptions) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString())
				.isEqualTo("UPDATE users SET firstname='Walter',lastname='White' WHERE id='heisenberg' IF EXISTS;");
	}

	@Test // DATACASS-575
	public void updateShouldUpdateEntityWithLwt() {

		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));

		UpdateOptions options = UpdateOptions.builder().ifCondition(where("firstname").is("Walter")).build();
		User user = new User("heisenberg", "Walter", "White");

		template.update(user, options) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString())
				.isEqualTo("UPDATE users SET firstname='Walter',lastname='White' WHERE id='heisenberg' IF firstname='Walter';");
	}

	@Test // DATACASS-575
	public void updateShouldApplyUpdateQuery() {

		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));

		template.update(Query.query(where("id").is("heisenberg")), Update.update("firstname", "Walter"), User.class) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString())
				.isEqualTo("UPDATE users SET firstname='Walter' WHERE id='heisenberg';");
	}

	@Test // DATACASS-575
	public void updateShouldApplyUpdateQueryWitLwt() {

		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));

		Filter ifCondition = Filter.from(where("firstname").is("Walter"), where("lastname").is("White"));
		Query query = Query.query(where("id").is("heisenberg"))
				.queryOptions(UpdateOptions.builder().ifCondition(ifCondition).build());

		template.update(query, Update.update("firstname", "Walter"), User.class) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo(
				"UPDATE users SET firstname='Walter' WHERE id='heisenberg' IF firstname='Walter' AND lastname='White';");
	}

	@Test // DATACASS-335
	public void deleteShouldRemoveEntity() {

		when(reactiveResultSet.wasApplied()).thenReturn(true);
		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));

		User user = new User("heisenberg", "Walter", "White");

		StepVerifier.create(template.delete(user)).expectNext(user).verifyComplete();

		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("DELETE FROM users WHERE id='heisenberg';");
	}

	@Test // DATACASS-575
	public void deleteShouldRemoveEntityWithLwt() {

		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));

		User user = new User("heisenberg", "Walter", "White");
		DeleteOptions options = DeleteOptions.builder().ifCondition(where("firstname").is("Walter")).build();

		template.delete(user, options) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString())
				.isEqualTo("DELETE FROM users WHERE id='heisenberg' IF firstname='Walter';");
	}

	@Test // DATACASS-575
	public void deleteShouldRemoveByQueryWithLwt() {

		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));

		DeleteOptions options = DeleteOptions.builder().ifCondition(where("firstname").is("Walter")).build();
		Query query = Query.query(where("id").is("heisenberg")).queryOptions(options);

		template.delete(query, User.class) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString())
				.isEqualTo("DELETE FROM users WHERE id='heisenberg' IF firstname='Walter';");
	}

	@Test // DATACASS-335
	public void truncateShouldRemoveEntities() {

		StepVerifier.create(template.truncate(User.class)).verifyComplete();

		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("TRUNCATE users;");
	}

	interface UserProjection {
		String getFirstname();
	}
}
