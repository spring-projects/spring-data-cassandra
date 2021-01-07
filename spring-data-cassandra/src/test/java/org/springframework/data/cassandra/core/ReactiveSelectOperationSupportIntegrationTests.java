/*
 * Copyright 2018-2021 the original author or authors.
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
import static org.springframework.data.cassandra.core.query.Criteria.*;
import static org.springframework.data.cassandra.core.query.Query.*;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTests;

/**
 * Integration tests for {@link ExecutableSelectOperationSupport}.
 *
 * @author Mark Paluch
 */
class ReactiveSelectOperationSupportIntegrationTests extends AbstractKeyspaceCreatingIntegrationTests {

	private CassandraAdminTemplate admin;

	private ReactiveCassandraTemplate template;

	private Person han;
	private Person luke;

	@BeforeEach
	void setUp() {

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

	@Test // DATACASS-485
	void domainTypeIsRequired() {
		assertThatIllegalArgumentException().isThrownBy(() -> this.template.query(null));
	}

	@Test // DATACASS-485
	void returnTypeIsRequiredOnSet() {
		assertThatIllegalArgumentException().isThrownBy(() -> this.template.query(Person.class).as(null));
	}

	@Test // DATACASS-485
	void tableIsRequiredOnSet() {
		assertThatIllegalArgumentException().isThrownBy(() -> this.template.query(Person.class).inTable((String) null));
	}

	@Test // DATACASS-485
	void findAll() {

		Flux<Person> result = this.template.query(Person.class).all();

		result.collectList().as(StepVerifier::create)
				.assertNext(actual ->
			assertThat(actual).containsExactlyInAnyOrder(han, luke)
		).verifyComplete();
	}

	@Test // DATACASS-485
	void findAllWithCollection() {

		Flux<Human> result = this.template.query(Human.class).inTable("person").all();

		result.as(StepVerifier::create).expectNextCount(2).verifyComplete();
	}

	@Test // DATACASS-485
	void findAllWithProjection() {

		Flux<Jedi> result = this.template.query(Person.class).as(Jedi.class).all();

		result.collectList().as(StepVerifier::create)
				.assertNext(actual ->
			assertThat(actual).hasOnlyElementsOfType(Jedi.class).hasSize(2)
		).verifyComplete();
	}

	@Test // DATACASS-485
	void findByReturningAllValuesAsClosedInterfaceProjection() {

		Flux<PersonProjection> result = this.template.query(Person.class).as(PersonProjection.class).all();

		result.collectList().as(StepVerifier::create)
				.assertNext(actual ->
			assertThat(actual).hasOnlyElementsOfType(PersonProjection.class).hasSize(2)
		).verifyComplete();
	}

	@Test // DATACASS-485
	void findAllBy() {

		Flux<Person> result = this.template.query(Person.class).matching(queryLuke()).all();

		result.as(StepVerifier::create).expectNext(luke).verifyComplete();
	}

	@Test // DATACASS-485
	void findAllByWithCollectionUsingMappingInformation() {

		Flux<Jedi> result = this.template.query(Jedi.class).inTable("person").all();

		result.collectList().as(StepVerifier::create)
				.assertNext(actual ->
			assertThat(actual).isNotEmpty().hasOnlyElementsOfType(Jedi.class)
		).verifyComplete();
	}

	@Test // DATACASS-485
	void findAllByWithCollection() {

		Flux<Human> result = this.template.query(Human.class).inTable("person").matching(queryLuke()).all();

		result.collectList().as(StepVerifier::create).expectNextCount(1).verifyComplete();
	}

	@Test // DATACASS-485
	void findAllByWithProjection() {

		Flux<Jedi> result = this.template.query(Person.class).as(Jedi.class).all();

		result.collectList().as(StepVerifier::create)
				.assertNext(actual ->
			assertThat(actual).isNotEmpty().hasOnlyElementsOfType(Jedi.class)
		).verifyComplete();
	}

	@Test // DATACASS-485
	void findBy() {

		Mono<Person> result = this.template.query(Person.class).matching(queryLuke()).one();

		result.as(StepVerifier::create).expectNext(luke).verifyComplete();
	}

	@Test // DATACASS-485
	void findByNoMatch() {

		Mono<Person> result = this.template.query(Person.class).matching(querySpock()).one();

		result.as(StepVerifier::create).verifyComplete();
	}

	@Test // DATACASS-485
	void findByTooManyResults() {

		Mono<Person> result = this.template.query(Person.class).one();

		result.as(StepVerifier::create).expectError(IncorrectResultSizeDataAccessException.class).verify();
	}

	@Test // DATACASS-485
	void findByReturningFirst() {

		Mono<Person> result = this.template.query(Person.class).matching(queryLuke()).first();

		result.as(StepVerifier::create).expectNext(luke).verifyComplete();
	}

	@Test // DATACASS-485
	void findByReturningFirstForManyResults() {

		Mono<Person> result = this.template.query(Person.class).first();

		result.as(StepVerifier::create).assertNext(actual ->
			assertThat(actual).isIn(han, luke)
		).verifyComplete();
	}

	@Test // DATACASS-485
	void findByReturningFirstAsClosedInterfaceProjection() {

		Mono<PersonProjection> result = this.template
				.query(Person.class)
				.as(PersonProjection.class)
				.matching(query(where("firstname").is("han")).withAllowFiltering())
				.first();

		result.as(StepVerifier::create).assertNext(actual -> {
			assertThat(actual).isInstanceOf(PersonProjection.class);
			assertThat(actual.getFirstname()).isEqualTo("han");
		}).verifyComplete();
	}

	@Test // DATACASS-485
	void findByReturningFirstAsOpenInterfaceProjection() {

		Mono<PersonSpELProjection> result = this.template
				.query(Person.class)
				.as(PersonSpELProjection.class)
				.matching(query(where("firstname").is("han")).withAllowFiltering())
				.first();

		result.as(StepVerifier::create).assertNext(actual -> {
			assertThat(actual).isInstanceOf(PersonSpELProjection.class);
			assertThat(actual.getName()).isEqualTo("han");
		}).verifyComplete();
	}

	@Test // DATACASS-485
	void countShouldReturnNumberOfElementsInCollectionWhenNoQueryPresent() {

		Mono<Long> count = this.template.query(Person.class).count();

		count.as(StepVerifier::create).expectNext(2L).verifyComplete();
	}

	@Test // DATACASS-485
	void countShouldReturnNrOfElementsMatchingQuery() {

		Mono<Long> count = this.template
				.query(Person.class)
				.matching(query(where("firstname").is(luke.getFirstname())).withAllowFiltering())
				.count();

		count.as(StepVerifier::create).expectNext(1L).verifyComplete();
	}

	@Test // DATACASS-485
	void existsShouldReturnTrueIfAtLeastOneElementExistsInCollection() {

		Mono<Boolean> exists = this.template.query(Person.class).exists();

		exists.as(StepVerifier::create).expectNext(true).verifyComplete();
	}

	@Test // DATACASS-485
	void existsShouldReturnFalseIfNoElementExistsInCollection() {

		this.template.truncate(Person.class).as(StepVerifier::create).verifyComplete();

		Mono<Boolean> exists = this.template.query(Person.class).exists();

		exists.as(StepVerifier::create).expectNext(false).verifyComplete();
	}

	@Test // DATACASS-485
	void existsShouldReturnTrueIfAtLeastOneElementMatchesQuery() {

		Mono<Boolean> exists = this.template.query(Person.class).matching(queryLuke()).exists();

		exists.as(StepVerifier::create).expectNext(true).verifyComplete();
	}

	@Test // DATACASS-485
	void existsShouldReturnFalseWhenNoElementMatchesQuery() {

		Mono<Boolean> exists = this.template.query(Person.class).matching(querySpock()).exists();

		exists.as(StepVerifier::create).expectNext(false).verifyComplete();
	}

	@Test // DATACASS-485
	void returnsTargetObjectDirectlyIfProjectionInterfaceIsImplemented() {

		Flux<Contact> result = this.template.query(Person.class).as(Contact.class).all();

		result.collectList().as(StepVerifier::create)
				.assertNext(actual ->
			assertThat(actual).allMatch(it -> it instanceof Person)
		).verifyComplete();
	}

	private static Query queryLuke() {
		return query(where("firstname").is("luke")).withAllowFiltering();
	}

	private static Query querySpock() {
		return query(where("firstname").is("spock")).withAllowFiltering();
	}

	private interface Contact {}

	@Data
	@Table
	static class Person implements Contact {
		@Id String id;
		@Indexed String firstname;
		@Indexed String lastname;
	}

	private interface PersonProjection {
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
