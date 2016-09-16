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

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.cassandra.core.ReactiveResultSet;
import org.springframework.cassandra.core.ReactiveSession;
import org.springframework.cassandra.support.exception.CassandraConnectionFailureException;
import org.springframework.data.cassandra.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.domain.Person;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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

	private ReactiveCassandraTemplate template;

	@Before
	public void setUp() {

		template = new ReactiveCassandraTemplate(session);
		when(session.execute(anyString())).thenReturn(Mono.just(reactiveResultSet));
		when(session.execute(any(Statement.class))).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.getColumnDefinitions()).thenReturn(columnDefinitions);
		when(row.getColumnDefinitions()).thenReturn(columnDefinitions);
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
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

		Flux<Person> flux = template.select("SELECT * FROM person", Person.class);

		assertThat(flux.collectList().block()).hasSize(1).contains(new Person("myid", "Walter", "White"));
		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("SELECT * FROM person");
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void selectShouldTranslateException() {

		when(reactiveResultSet.rows()).thenThrow(new NoHostAvailableException(Collections.emptyMap()));

		Flux<Person> flux = template.select("SELECT * FROM person", Person.class);

		try {
			flux.last().block();

			fail("Missing CassandraConnectionFailureException");
		} catch (CassandraConnectionFailureException e) {
			assertThat(e).hasRootCauseInstanceOf(NoHostAvailableException.class);
		}
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
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

		Mono<Person> mono = template.selectOneById("myid", Person.class);

		assertThat(mono.block()).isEqualTo(new Person("myid", "Walter", "White"));
		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("SELECT * FROM person WHERE id='myid';");
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void existsShouldReturnExistingElement() {

		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));
		when(columnDefinitions.contains(anyString())).thenReturn(true);
		when(columnDefinitions.getType(anyInt())).thenReturn(DataType.ascii());

		Mono<Boolean> mono = template.exists("myid", Person.class);

		assertThat(mono.block()).isTrue();
		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("SELECT * FROM person WHERE id='myid';");
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void existsShouldReturnNonExistingElement() {

		when(reactiveResultSet.rows()).thenReturn(Flux.empty());

		Mono<Boolean> mono = template.exists("myid", Person.class);

		assertThat(mono.block()).isFalse();
		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("SELECT * FROM person WHERE id='myid';");
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void countShouldExecuteCountQueryElement() {

		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));
		when(row.getLong(0)).thenReturn(42L);
		when(columnDefinitions.size()).thenReturn(1);

		Mono<Long> mono = template.count(Person.class);

		assertThat(mono.block()).isEqualTo(42L);
		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("SELECT count(*) FROM person;");
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void insertShouldInsertEntity() {

		when(reactiveResultSet.wasApplied()).thenReturn(true);

		Person person = new Person("heisenberg", "Walter", "White");

		Mono<Person> mono = template.insert(person);

		assertThat(mono.block()).isEqualTo(person);
		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString())
				.isEqualTo("INSERT INTO person (firstname,id,lastname) VALUES ('Walter','heisenberg','White');");
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void insertShouldTranslateException() {

		reset(session);
		when(session.execute(any(Statement.class)))
				.thenReturn(Mono.error(new NoHostAvailableException(Collections.emptyMap())));

		Mono<Person> mono = template.insert(new Person("heisenberg", "Walter", "White"));

		try {
			mono.block();

			fail("Missing CassandraConnectionFailureException");
		} catch (CassandraConnectionFailureException e) {
			assertThat(e).hasRootCauseInstanceOf(NoHostAvailableException.class);
		}
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void insertShouldNotApplyInsert() {

		when(reactiveResultSet.wasApplied()).thenReturn(false);

		Person person = new Person("heisenberg", "Walter", "White");

		Mono<Person> mono = template.insert(person);

		assertThat(mono.block()).isNull();
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void insertEntitiesShouldInsertEntities() {

		when(reactiveResultSet.wasApplied()).thenReturn(true);

		Person person = new Person("heisenberg", "Walter", "White");

		Flux<Person> flux = template.insert(Collections.singletonList(person));

		assertThat(flux.collectList().block()).contains(person);
		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString())
				.isEqualTo("INSERT INTO person (firstname,id,lastname) VALUES ('Walter','heisenberg','White');");
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void insertEntitiesShouldNotApplyInsert() {

		when(reactiveResultSet.wasApplied()).thenReturn(false);

		Person person = new Person("heisenberg", "Walter", "White");

		Flux<Person> flux = template.insert(Collections.singletonList(person));

		assertThat(flux.collectList().block()).isEmpty();
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void updateShouldUpdateEntity() {

		when(reactiveResultSet.wasApplied()).thenReturn(true);

		Person person = new Person("heisenberg", "Walter", "White");

		Mono<Person> mono = template.update(person);

		assertThat(mono.block()).isEqualTo(person);
		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString())
				.isEqualTo("UPDATE person SET firstname='Walter',lastname='White' WHERE id='heisenberg';");
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void updateShouldTranslateException() {

		reset(session);
		when(session.execute(any(Statement.class)))
				.thenReturn(Mono.error(new NoHostAvailableException(Collections.emptyMap())));

		Mono<Person> mono = template.update(new Person("heisenberg", "Walter", "White"));

		try {
			mono.block();

			fail("Missing CassandraConnectionFailureException");
		} catch (CassandraConnectionFailureException e) {
			assertThat(e).hasRootCauseInstanceOf(NoHostAvailableException.class);
		}
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void updateShouldNotApplyUpdate() {

		when(reactiveResultSet.wasApplied()).thenReturn(false);

		Person person = new Person("heisenberg", "Walter", "White");

		Mono<Person> mono = template.update(person);

		assertThat(mono.block()).isNull();
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void updateEntitiesShouldInsertEntities() {

		when(reactiveResultSet.wasApplied()).thenReturn(true);

		Person person = new Person("heisenberg", "Walter", "White");

		Flux<Person> flux = template.update(Collections.singletonList(person));

		assertThat(flux.collectList().block()).contains(person);
		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString())
				.isEqualTo("UPDATE person SET firstname='Walter',lastname='White' WHERE id='heisenberg';");
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void updateEntitiesShouldNotApplyUpdate() {

		when(reactiveResultSet.wasApplied()).thenReturn(false);

		Person person = new Person("heisenberg", "Walter", "White");

		Flux<Person> flux = template.update(Collections.singletonList(person));

		assertThat(flux.collectList().block()).isEmpty();
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void deleteShouldRemoveEntity() {

		when(reactiveResultSet.wasApplied()).thenReturn(true);

		Person person = new Person("heisenberg", "Walter", "White");

		Mono<Person> mono = template.delete(person);

		assertThat(mono.block()).isEqualTo(person);
		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("DELETE FROM person WHERE id='heisenberg';");
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void deleteShouldTranslateException() {

		reset(session);
		when(session.execute(any(Statement.class)))
				.thenReturn(Mono.error(new NoHostAvailableException(Collections.emptyMap())));

		Mono<Person> mono = template.delete(new Person("heisenberg", "Walter", "White"));

		try {
			mono.block();

			fail("Missing CassandraConnectionFailureException");
		} catch (CassandraConnectionFailureException e) {
			assertThat(e).hasRootCauseInstanceOf(NoHostAvailableException.class);
		}
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void deleteShouldNotApplyRemoval() {

		when(reactiveResultSet.wasApplied()).thenReturn(false);

		Person person = new Person("heisenberg", "Walter", "White");

		Mono<Person> mono = template.delete(person);

		assertThat(mono.block()).isNull();
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void deleteEntitiesShouldRemoveEntities() {

		when(reactiveResultSet.wasApplied()).thenReturn(true);

		Person person = new Person("heisenberg", "Walter", "White");

		Flux<Person> flux = template.delete(Collections.singletonList(person));

		assertThat(flux.collectList().block()).contains(person);
		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("DELETE FROM person WHERE id='heisenberg';");
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void deleteEntitiesShouldNotApplyRemoval() {

		when(reactiveResultSet.wasApplied()).thenReturn(false);

		Person person = new Person("heisenberg", "Walter", "White");

		Flux<Person> flux = template.delete(Collections.singletonList(person));

		assertThat(flux.collectList().block()).isEmpty();
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void truncateShouldRemoveEntities() {

		template.truncate(Person.class).block();

		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("TRUNCATE person;");
	}
}
