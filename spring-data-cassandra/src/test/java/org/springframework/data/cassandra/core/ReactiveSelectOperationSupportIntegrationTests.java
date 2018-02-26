/*
 * Copyright 2018 the original author or authors.
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
import static org.springframework.data.cassandra.core.query.Criteria.*;
import static org.springframework.data.cassandra.core.query.Query.*;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.cql.session.DefaultBridgedReactiveSession;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.Indexed;
import org.springframework.data.cassandra.core.mapping.Table;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.cassandra.repository.support.SchemaTestUtils;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTest;

/**
 * Integration tests for {@link ExecutableSelectOperationSupport}.
 *
 * @author Mark Paluch
 */
public class ReactiveSelectOperationSupportIntegrationTests extends AbstractKeyspaceCreatingIntegrationTest {

	CassandraAdminTemplate admin;

	ReactiveCassandraTemplate template;

	Person han;
	Person luke;

	@Before
	public void setUp() {

		admin = new CassandraAdminTemplate(session, new MappingCassandraConverter());
		template = new ReactiveCassandraTemplate(new DefaultBridgedReactiveSession(session));

		SchemaTestUtils.potentiallyCreateTableFor(ExecutableSelectOperationSupportIntegrationTests.Person.class, admin);
		admin.getCqlOperations().execute("CREATE INDEX IF NOT EXISTS IX_Person_firstname_ESOSIT ON Person (firstname);");
		admin.getCqlOperations().execute("CREATE INDEX IF NOT EXISTS IX_Person_lastname_ESOSIT ON Person (lastname);");

		SchemaTestUtils.truncate(ExecutableSelectOperationSupportIntegrationTests.Person.class, admin);
		initPersons();
	}

	private void initPersons() {

		han = new Person();
		han.firstname = "han";
		han.lastname = "solo";
		han.id = "id-1";

		luke = new Person();
		luke.firstname = "luke";
		luke.lastname = "skywalker";
		luke.id = "id-2";

		admin.insert(han);
		admin.insert(luke);
	}

	@Test(expected = IllegalArgumentException.class) // DATACASS-485
	public void domainTypeIsRequired() {
		this.template.query(null);
	}

	@Test(expected = IllegalArgumentException.class) // DATACASS-485
	public void returnTypeIsRequiredOnSet() {
		this.template.query(Person.class).as(null);
	}

	@Test(expected = IllegalArgumentException.class) // DATACASS-485
	public void tableIsRequiredOnSet() {
		this.template.query(Person.class).inTable((String) null);
	}

	@Test // DATACASS-485
	public void findAll() {

		Flux<Person> result = this.template.query(Person.class).all();

		StepVerifier.create(result.collectList()).assertNext(actual ->
			assertThat(actual).containsExactlyInAnyOrder(han, luke)
		).verifyComplete();
	}

	@Test // DATACASS-485
	public void findAllWithCollection() {

		Flux<Human> result = this.template.query(Human.class).inTable("person").all();

		StepVerifier.create(result).expectNextCount(2).verifyComplete();
	}

	@Test // DATACASS-485
	public void findAllWithProjection() {

		Flux<Jedi> result = this.template.query(Person.class).as(Jedi.class).all();

		StepVerifier.create(result.collectList()).assertNext(actual ->
			assertThat(actual).hasOnlyElementsOfType(Jedi.class).hasSize(2)
		).verifyComplete();
	}

	@Test // DATACASS-485
	public void findByReturningAllValuesAsClosedInterfaceProjection() {

		Flux<PersonProjection> result = this.template.query(Person.class).as(PersonProjection.class).all();

		StepVerifier.create(result.collectList()).assertNext(actual ->
			assertThat(actual).hasOnlyElementsOfType(PersonProjection.class).hasSize(2)
		).verifyComplete();
	}

	@Test // DATACASS-485
	public void findAllBy() {

		Flux<Person> result = this.template.query(Person.class).matching(queryLuke()).all();

		StepVerifier.create(result).expectNext(luke).verifyComplete();
	}

	@Test // DATACASS-485
	public void findAllByWithCollectionUsingMappingInformation() {

		Flux<Jedi> result = this.template.query(Jedi.class).inTable("person").all();

		StepVerifier.create(result.collectList()).assertNext(actual ->
			assertThat(actual).isNotEmpty().hasOnlyElementsOfType(Jedi.class)
		).verifyComplete();
	}

	@Test // DATACASS-485
	public void findAllByWithCollection() {

		Flux<Human> result = this.template.query(Human.class).inTable("person").matching(queryLuke()).all();

		StepVerifier.create(result.collectList()).expectNextCount(1).verifyComplete();
	}

	@Test // DATACASS-485
	public void findAllByWithProjection() {

		Flux<Jedi> result = this.template.query(Person.class).as(Jedi.class).all();

		StepVerifier.create(result.collectList()).assertNext(actual ->
			assertThat(actual).isNotEmpty().hasOnlyElementsOfType(Jedi.class)
		).verifyComplete();
	}

	@Test // DATACASS-485
	public void findBy() {

		Mono<Person> result = this.template.query(Person.class).matching(queryLuke()).one();

		StepVerifier.create(result).expectNext(luke).verifyComplete();
	}

	@Test // DATACASS-485
	public void findByNoMatch() {

		Mono<Person> result = this.template.query(Person.class).matching(querySpock()).one();

		StepVerifier.create(result).verifyComplete();
	}

	@Test // DATACASS-485
	public void findByTooManyResults() {

		Mono<Person> result = this.template.query(Person.class).one();

		StepVerifier.create(result).expectError(IncorrectResultSizeDataAccessException.class).verify();
	}

	@Test // DATACASS-485
	public void findByReturningFirst() {

		Mono<Person> result = this.template.query(Person.class).matching(queryLuke()).first();

		StepVerifier.create(result).expectNext(luke).verifyComplete();
	}

	@Test // DATACASS-485
	public void findByReturningFirstForManyResults() {

		Mono<Person> result = this.template.query(Person.class).first();

		StepVerifier.create(result).assertNext(actual ->
			assertThat(actual).isIn(han, luke)
		).verifyComplete();
	}

	@Test // DATACASS-485
	public void findByReturningFirstAsClosedInterfaceProjection() {

		Mono<PersonProjection> result = this.template
				.query(Person.class)
				.as(PersonProjection.class)
				.matching(query(where("firstname").is("han")).withAllowFiltering())
				.first();

		StepVerifier.create(result).assertNext(actual -> {
			assertThat(actual).isInstanceOf(PersonProjection.class);
			assertThat(actual.getFirstname()).isEqualTo("han");
		}).verifyComplete();
	}

	@Test // DATACASS-485
	public void findByReturningFirstAsOpenInterfaceProjection() {

		Mono<PersonSpELProjection> result = this.template
				.query(Person.class)
				.as(PersonSpELProjection.class)
				.matching(query(where("firstname").is("han")).withAllowFiltering())
				.first();

		StepVerifier.create(result).assertNext(actual -> {
			assertThat(actual).isInstanceOf(PersonSpELProjection.class);
			assertThat(actual.getName()).isEqualTo("han");
		}).verifyComplete();
	}

	@Test // DATACASS-485
	public void countShouldReturnNumberOfElementsInCollectionWhenNoQueryPresent() {

		Mono<Long> count = this.template.query(Person.class).count();

		StepVerifier.create(count).expectNext(2L).verifyComplete();
	}

	@Test // DATACASS-485
	public void countShouldReturnNrOfElementsMatchingQuery() {

		Mono<Long> count = this.template
				.query(Person.class)
				.matching(query(where("firstname").is(luke.getFirstname())).withAllowFiltering())
				.count();

		StepVerifier.create(count).expectNext(1L).verifyComplete();
	}

	@Test // DATACASS-485
	public void existsShouldReturnTrueIfAtLeastOneElementExistsInCollection() {

		Mono<Boolean> exists = this.template.query(Person.class).exists();

		StepVerifier.create(exists).expectNext(true).verifyComplete();
	}

	@Test // DATACASS-485
	public void existsShouldReturnFalseIfNoElementExistsInCollection() {

		StepVerifier.create(this.template.truncate(Person.class)).verifyComplete();

		Mono<Boolean> exists = this.template.query(Person.class).exists();

		StepVerifier.create(exists).expectNext(false).verifyComplete();
	}

	@Test // DATACASS-485
	public void existsShouldReturnTrueIfAtLeastOneElementMatchesQuery() {

		Mono<Boolean> exists = this.template.query(Person.class).matching(queryLuke()).exists();

		StepVerifier.create(exists).expectNext(true).verifyComplete();
	}

	@Test // DATACASS-485
	public void existsShouldReturnFalseWhenNoElementMatchesQuery() {

		Mono<Boolean> exists = this.template.query(Person.class).matching(querySpock()).exists();

		StepVerifier.create(exists).expectNext(false).verifyComplete();
	}

	@Test // DATACASS-485
	public void returnsTargetObjectDirectlyIfProjectionInterfaceIsImplemented() {

		Flux<Contact> result = this.template.query(Person.class).as(Contact.class).all();

		StepVerifier.create(result.collectList()).assertNext(actual ->
			assertThat(actual).allMatch(it -> it instanceof Person)
		).verifyComplete();
	}

	private static Query queryLuke() {
		return query(where("firstname").is("luke")).withAllowFiltering();
	}

	private static Query querySpock() {
		return query(where("firstname").is("spock")).withAllowFiltering();
	}

	interface Contact {}

	@Data
	@Table
	static class Person implements Contact {
		@Id String id;
		@Indexed String firstname;
		@Indexed String lastname;
	}

	interface PersonProjection {
		String getFirstname();
	}

	public interface PersonSpELProjection {
		@Value("#{target.firstname}")
		String getName();
	}

	@Data
	static class Human {
		@Id String id;
	}

	@Data
	@AllArgsConstructor
	@NoArgsConstructor
	static class Jedi {
		@Column("firstname") String name;
	}

	@Data
	static class Sith {
		String rank;
	}

	interface PlanetProjection {
		String getName();
	}

	interface PlanetSpELProjection {
		@Value("#{target.name}")
		String getId();
	}
}
