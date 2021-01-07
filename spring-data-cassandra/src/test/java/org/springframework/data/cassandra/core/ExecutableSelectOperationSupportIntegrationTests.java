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

import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.ExecutableSelectOperation.TerminatingSelect;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
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
class ExecutableSelectOperationSupportIntegrationTests extends AbstractKeyspaceCreatingIntegrationTests {

	private CassandraAdminTemplate template;

	private Person han;
	private Person luke;

	@BeforeEach
	void setUp() {

		this.template = new CassandraAdminTemplate(session, new MappingCassandraConverter());

		SchemaTestUtils.potentiallyCreateTableFor(Person.class, this.template);
		this.template.getCqlOperations()
				.execute("CREATE INDEX IF NOT EXISTS IX_Person_firstname_ESOSIT ON Person (firstname);");
		this.template.getCqlOperations()
				.execute("CREATE INDEX IF NOT EXISTS IX_Person_lastname_ESOSIT ON Person (lastname);");

		SchemaTestUtils.truncate(Person.class, this.template);
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

		template.insert(han);
		template.insert(luke);
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
		assertThat(this.template.query(Person.class).all()).containsExactlyInAnyOrder(han, luke);
	}

	@Test // DATACASS-485
	void findAllWithCollection() {
		assertThat(this.template.query(Human.class).inTable("person").all()).hasSize(2);
	}

	@Test // DATACASS-485
	void findAllWithProjection() {
		assertThat(this.template.query(Person.class).as(Jedi.class).all()).hasOnlyElementsOfType(Jedi.class).hasSize(2);
	}

	@Test // DATACASS-485
	void findByReturningAllValuesAsClosedInterfaceProjection() {
		assertThat(this.template.query(Person.class).as(PersonProjection.class).all())
				.hasOnlyElementsOfTypes(PersonProjection.class);
	}

	@Test // DATACASS-485
	void findAllBy() {
		assertThat(this.template.query(Person.class).matching(queryLuke()).all()).containsExactlyInAnyOrder(luke);
	}

	@Test // DATACASS-485
	void findAllByWithCollectionUsingMappingInformation() {

		assertThat(this.template.query(Jedi.class).inTable("person").all())
				.isNotEmpty().hasOnlyElementsOfType(Jedi.class);
	}

	@Test // DATACASS-485
	void findAllByWithCollection() {
		assertThat(this.template.query(Human.class).inTable("person").matching(queryLuke()).all()).hasSize(1);
	}

	@Test // DATACASS-485
	void findAllByWithProjection() {

		assertThat(this.template.query(Person.class).as(Jedi.class).all())
				.hasOnlyElementsOfType(Jedi.class).isNotEmpty();
	}

	@Test // DATACASS-485
	void findBy() {
		assertThat(this.template.query(Person.class).matching(queryLuke()).one()).contains(luke);
	}

	@Test // DATACASS-485
	void findByNoMatch() {
		assertThat(this.template.query(Person.class).matching(querySpock()).one()).isEmpty();
	}

	@Test // DATACASS-485
	void findByTooManyResults() {
		assertThatExceptionOfType(IncorrectResultSizeDataAccessException.class)
				.isThrownBy(() -> this.template.query(Person.class).one());
	}

	@Test // DATACASS-485
	void findByReturningOneValue() {
		assertThat(this.template.query(Person.class).matching(queryLuke()).oneValue()).isEqualTo(luke);
	}

	@Test // DATACASS-485
	void findByReturningOneValueButTooManyResults() {
		assertThatExceptionOfType(IncorrectResultSizeDataAccessException.class)
				.isThrownBy(() -> this.template.query(Person.class).oneValue());
	}

	@Test // DATACASS-485
	void findByReturningFirstValue() {
		assertThat(this.template.query(Person.class).matching(queryLuke()).firstValue()).isEqualTo(luke);
	}

	@Test // DATACASS-485
	void findByReturningFirstValueForManyResults() {
		assertThat(this.template.query(Person.class).firstValue()).isIn(han, luke);
	}

	@Test // DATACASS-485
	void findByReturningFirstValueAsClosedInterfaceProjection() {

		PersonProjection result = this.template
				.query(Person.class)
				.as(PersonProjection.class)
				.matching(query(where("firstname").is("han")).withAllowFiltering())
				.firstValue();

		assertThat(result).isInstanceOf(PersonProjection.class);
		assertThat(result.getFirstname()).isEqualTo("han");
	}

	@Test // DATACASS-485
	void findByReturningFirstValueAsOpenInterfaceProjection() {

		PersonSpELProjection result = this.template
				.query(Person.class)
				.as(PersonSpELProjection.class)
				.matching(query(where("firstname").is("han")).withAllowFiltering())
				.firstValue();

		assertThat(result).isInstanceOf(PersonSpELProjection.class);
		assertThat(result.getName()).isEqualTo("han");
	}

	@Test // DATACASS-485
	void streamAll() {

		try (Stream<Person> stream = this.template.query(Person.class).stream()) {
			assertThat(stream).containsExactlyInAnyOrder(han, luke);
		}
	}

	@Test // DATACASS-485
	void streamAllWithCollection() {

		Stream<Human> stream = this.template.query(Human.class).inTable("person").stream();

		assertThat(stream).hasSize(2);
	}

	@Test // DATACASS-485
	void streamAllWithProjection() {

		try (Stream<Jedi> stream = this.template.query(Person.class).as(Jedi.class).stream()) {
			assertThat(stream).hasOnlyElementsOfType(Jedi.class).hasSize(2);
		}
	}

	@Test // DATACASS-485
	void streamAllReturningResultsAsClosedInterfaceProjection() {

		TerminatingSelect<PersonProjection> operation =
				this.template.query(Person.class).as(PersonProjection.class);

		assertThat(operation.stream()) //
				.hasSize(2) //
				.allSatisfy(it -> {
					assertThat(it).isInstanceOf(PersonProjection.class);
					assertThat(it.getFirstname()).isNotBlank();
				});
	}

	@Test // DATACASS-485
	void streamAllReturningResultsAsOpenInterfaceProjection() {

		TerminatingSelect<PersonSpELProjection> operation =
				this.template.query(Person.class).as(PersonSpELProjection.class);

		assertThat(operation.stream()) //
				.hasSize(2) //
				.allSatisfy(it -> {
					assertThat(it).isInstanceOf(PersonSpELProjection.class);
					assertThat(it.getName()).isNotBlank();
				});
	}

	@Test // DATACASS-485
	void streamAllBy() {

		Stream<Person> stream = this.template.query(Person.class).matching(queryLuke()).stream();

		assertThat(stream).containsExactlyInAnyOrder(luke);
	}

	@Test // DATACASS-485
	void firstShouldReturnFirstEntryInCollection() {
		assertThat(this.template.query(Person.class).first()).isNotEmpty();
	}

	@Test // DATACASS-485
	void countShouldReturnNrOfElementsInCollectionWhenNoQueryPresent() {
		assertThat(this.template.query(Person.class).count()).isEqualTo(2);
	}

	@Test // DATACASS-485
	void countShouldReturnNrOfElementsMatchingQuery() {
		assertThat(this.template.query(Person.class).matching(query(where("firstname").is(luke.getFirstname()))
				.withAllowFiltering()).count()).isEqualTo(1);
	}

	@Test // DATACASS-485
	void existsShouldReturnTrueIfAtLeastOneElementExistsInCollection() {
		assertThat(this.template.query(Person.class).exists()).isTrue();
	}

	@Test // DATACASS-485
	void existsShouldReturnFalseIfNoElementExistsInCollection() {

		this.template.truncate(Person.class);

		assertThat(this.template.query(Person.class).exists()).isFalse();
	}

	@Test // DATACASS-485
	void existsShouldReturnTrueIfAtLeastOneElementMatchesQuery() {
		assertThat(this.template.query(Person.class).matching(queryLuke()).exists()).isTrue();
	}

	@Test // DATACASS-485
	void existsShouldReturnFalseWhenNoElementMatchesQuery() {
		assertThat(this.template.query(Person.class).matching(querySpock()).exists()).isFalse();
	}

	@Test // DATACASS-485
	void returnsTargetObjectDirectlyIfProjectionInterfaceIsImplemented() {
		assertThat(this.template.query(Person.class).as(Contact.class).all()).allMatch(it -> it instanceof Person);
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
		@Value("#{target.firstname}") String getName();
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
}
