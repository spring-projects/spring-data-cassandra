/*
 * Copyright 2017-present the original author or authors.
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
package org.springframework.data.cassandra.repository;

import static org.assertj.core.api.Assertions.*;
import static org.junit.Assume.*;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.config.SchemaAction;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.cql.generator.CqlGenerator;
import org.springframework.data.cassandra.core.cql.keyspace.CreateIndexSpecification;
import org.springframework.data.cassandra.core.mapping.Embedded;
import org.springframework.data.cassandra.core.mapping.Indexed;
import org.springframework.data.cassandra.core.mapping.Table;
import org.springframework.data.cassandra.core.query.CassandraPageRequest;
import org.springframework.data.cassandra.core.query.CassandraScrollPosition;
import org.springframework.data.cassandra.domain.AddressType;
import org.springframework.data.cassandra.domain.Person;
import org.springframework.data.cassandra.repository.QueryDerivationIntegrationTests.PersonRepository.NumberOfChildren;
import org.springframework.data.cassandra.repository.QueryDerivationIntegrationTests.PersonRepository.PersonDto;
import org.springframework.data.cassandra.repository.QueryDerivationIntegrationTests.PersonRepository.PersonProjection;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;
import org.springframework.data.cassandra.repository.support.AbstractSpringDataEmbeddedCassandraIntegrationTest;
import org.springframework.data.cassandra.repository.support.IntegrationTestConfig;
import org.springframework.data.cassandra.support.CassandraVersion;
import org.springframework.data.domain.Limit;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.ScrollPosition;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Window;
import org.springframework.data.support.WindowIterator;
import org.springframework.data.util.Streamable;
import org.springframework.data.util.Version;
import org.springframework.lang.Nullable;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.util.ObjectUtils;

import com.datastax.oss.driver.api.core.CqlSession;

/**
 * Integration tests for query derivation through {@link PersonRepository}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
@SpringJUnitConfig
@SuppressWarnings("all")
class QueryDerivationIntegrationTests extends AbstractSpringDataEmbeddedCassandraIntegrationTest {

	@Configuration
	@EnableCassandraRepositories(considerNestedRepositories = true,
			includeFilters = @Filter(classes = { PersonRepository.class, EmbeddedPersonRepository.class },
					type = FilterType.ASSIGNABLE_TYPE))
	public static class Config extends IntegrationTestConfig {

		@Override
		protected Set<Class<?>> getInitialEntitySet() {
			return new HashSet<>(Arrays.asList(Person.class, PersonWithEmbedded.class));
		}

		@Override
		public SchemaAction getSchemaAction() {
			return SchemaAction.CREATE;
		}
	}

	@Autowired CassandraOperations template;
	@Autowired CqlSession session;
	@Autowired PersonRepository personRepository;
	@Autowired EmbeddedPersonRepository personWithEmbeddedRepository;

	private Person walter;
	private Person skyler;
	private Person flynn;
	private Version cassandraVersion;

	@BeforeEach
	public void before() {

		deleteAllEntities();

		Person person = new Person("Walter", "White");

		person.setNumberOfChildren(2);

		person.setMainAddress(new AddressType("Albuquerque", "USA"));

		person.setAlternativeAddresses(Arrays.asList(new AddressType("Albuquerque", "USA"),
				new AddressType("New Hampshire", "USA"), new AddressType("Grocery Store", "Mexico")));

		walter = personRepository.save(person);
		skyler = personRepository.save(new Person("Skyler", "White"));
		flynn = personRepository.save(new Person("Flynn (Walter Jr.)", "White"));
		cassandraVersion = CassandraVersion.get(session);
	}

	@Test // DATACASS-7
	public void shouldFindByLastname() {

		List<Person> result = personRepository.findByLastname("White");

		assertThat(result).contains(walter, skyler, flynn);
	}

	@Test // DATACASS-525
	public void findOneWithManyResultsShouldFail() {
		assertThatExceptionOfType(IncorrectResultSizeDataAccessException.class)
				.isThrownBy(() -> personRepository.findSomeByLastname("White"));
	}

	@Test // DATACASS-525
	public void findOneWithNoResultsShouldReturnNull() {
		assertThat(personRepository.findSomeByLastname("Foo")).isNull();
	}

	@Test // DATACASS-525
	public void findFirstWithManyResultsShouldReturnResult() {
		assertThat(personRepository.findFirstByLastname("White")).isNotNull();
	}

	@Test // DATACASS-7
	public void shouldFindByLastnameAndDynamicSort() {

		List<Person> result = personRepository.findByLastname("White", Sort.by("firstname"));

		assertThat(result).contains(flynn, skyler, walter);
	}

	@Test // DATACASS-7
	public void shouldFindByLastnameWithOrdering() {

		List<Person> result = personRepository.findByLastnameOrderByFirstnameAsc("White");

		assertThat(result).contains(flynn, skyler, walter);
	}

	@Test // DATACASS-7
	public void shouldFindByFirstnameAndLastname() {

		Person result = personRepository.findByFirstnameAndLastname("Walter", "White");

		assertThat(result).isEqualTo(walter);
	}

	@Test // DATACASS-172
	public void shouldFindByMappedUdt() throws InterruptedException {

		CreateIndexSpecification indexSpecification = CreateIndexSpecification.createIndex("person_main_address")
				.ifNotExists().tableName("person").columnName("mainaddress");

		template.getCqlOperations().execute(CqlGenerator.toCql(indexSpecification));

		// Give Cassandra some time to build the index
		Thread.sleep(500);

		Person result = personRepository.findByMainAddress(walter.getMainAddress());

		assertThat(result).isEqualTo(walter);
	}

	@Test // DATACASS-172
	public void shouldFindByMappedUdtStringQuery() throws InterruptedException {

		CreateIndexSpecification indexSpecification = CreateIndexSpecification.createIndex("person_main_address")
				.ifNotExists().tableName("person").columnName("mainaddress");

		template.getCqlOperations().execute(CqlGenerator.toCql(indexSpecification));

		// Give Cassandra some time to build the index
		Thread.sleep(500);

		Person result = personRepository.findByAddress(walter.getMainAddress());

		assertThat(result).isEqualTo(walter);
	}

	@Test // DATACASS-7
	public void executesCollectionQueryWithProjection() {

		Collection<PersonProjection> collection = personRepository.findPersonProjectedBy();

		assertThat(collection).hasSize(3).extracting("firstname").contains(flynn.getFirstname(), skyler.getFirstname(),
				walter.getFirstname());
	}

	@Test // DATACASS-359
	public void executesCollectionQueryWithDtoProjection() {

		Collection<PersonDto> collection = personRepository.findPersonDtoBy();

		assertThat(collection).hasSize(3).extracting("firstname").contains(flynn.getFirstname(), skyler.getFirstname(),
				walter.getFirstname());
	}

	@Test // DATACASS-359
	public void executesCollectionQueryWithDtoDynamicallyProjected() throws Exception {

		assumeTrue(cassandraVersion.isGreaterThanOrEqualTo(Version.parse("3.4")));

		CreateIndexSpecification indexSpecification = CreateIndexSpecification.createIndex("fn_starts_with").ifNotExists()
				.tableName("person").columnName("nickname").using("org.apache.cassandra.index.sasi.SASIIndex");

		template.getCqlOperations().execute(CqlGenerator.toCql(indexSpecification));

		// Give Cassandra some time to build the index
		Thread.sleep(500);

		walter.setNickname("Heisenberg");
		personRepository.save(walter);

		PersonDto heisenberg = personRepository.findDtoByNicknameStartsWith("Heisen", PersonDto.class);

		Assertions.assertThat(heisenberg.firstname).isEqualTo("Walter");
		Assertions.assertThat(heisenberg.lastname).isEqualTo("White");
	}

	@Test // DATACASS-7
	public void shouldFindByNumberOfChildren() throws Exception {

		CreateIndexSpecification indexSpecification = CreateIndexSpecification.createIndex("person_number_of_children")
				.ifNotExists().tableName("person").columnName("numberofchildren");

		template.getCqlOperations().execute(CqlGenerator.toCql(indexSpecification));

		// Give Cassandra some time to build the index
		Thread.sleep(500);

		Person result = personRepository.findByNumberOfChildren(NumberOfChildren.TWO);

		assertThat(result).isEqualTo(walter);
	}

	@Test // DATACASS-7
	public void shouldFindByLocalDate() throws InterruptedException {

		CreateIndexSpecification indexSpecification = CreateIndexSpecification.createIndex("person_created_date")
				.ifNotExists().tableName("person").columnName("createddate");

		template.getCqlOperations().execute(CqlGenerator.toCql(indexSpecification));

		// Give Cassandra some time to build the index
		Thread.sleep(500);

		walter.setCreatedDate(LocalDate.now());
		personRepository.save(walter);

		Person result = personRepository.findByCreatedDate(walter.getCreatedDate());

		assertThat(result).isEqualTo(walter);
	}

	@Test // DATACASS-7
	public void shouldUseQueryOverride() {

		Person otherWalter = new Person("Walter", "Black");

		personRepository.save(otherWalter);

		List<Person> result = personRepository.findByFirstname("Walter");

		assertThat(result).hasSize(1);
	}

	@Test // DATACASS-7
	public void shouldUseStartsWithQuery() throws InterruptedException {

		assumeTrue(cassandraVersion.isGreaterThanOrEqualTo(Version.parse("3.4")));

		CreateIndexSpecification indexSpecification = CreateIndexSpecification.createIndex("fn_starts_with").ifNotExists()
				.tableName("person").columnName("nickname").using("org.apache.cassandra.index.sasi.SASIIndex");

		template.getCqlOperations().execute(CqlGenerator.toCql(indexSpecification));

		// Give Cassandra some time to build the index
		Thread.sleep(500);

		walter.setNickname("Heisenberg");
		personRepository.save(walter);

		assertThat(personRepository.findByNicknameStartsWith("Heis")).isEqualTo(walter);
	}

	@Test // DATACASS-7
	public void shouldUseContainsQuery() throws InterruptedException {

		assumeTrue(cassandraVersion.isGreaterThanOrEqualTo(Version.parse("3.4")));

		CreateIndexSpecification indexSpecification = CreateIndexSpecification.createIndex("fn_contains").ifNotExists()
				.tableName("person").columnName("nickname").using("org.apache.cassandra.index.sasi.SASIIndex")
				.withOption("mode", "CONTAINS");

		template.getCqlOperations().execute(CqlGenerator.toCql(indexSpecification));

		// Give Cassandra some time to build the index
		Thread.sleep(500);

		walter.setNickname("Heisenberg");
		personRepository.save(walter);

		assertThat(personRepository.findByNicknameContains("eisenber")).isEqualTo(walter);
	}

	@Test // GH-1636
	public void shouldSelectUnpagedSlice() {

		Slice<Person> slice = personRepository.findAllSlicedByLastname("White", Pageable.unpaged());

		assertThat(slice).hasSize(3);
	}

	@Test // DATACASS-56
	public void shouldSelectSliced() {

		List<Person> result = new ArrayList<>();

		Slice<Person> firstPage = personRepository.findAllSlicedByLastname("White", CassandraPageRequest.first(2));
		Slice<Person> nextPage = personRepository.findAllSlicedByLastname("White", firstPage.nextPageable());

		result.addAll(firstPage.getContent());
		result.addAll(nextPage.getContent());

		assertThat(firstPage).hasSize(2);
		assertThat(nextPage).hasSize(1);
		assertThat(result).contains(walter, skyler, flynn);
	}

	@Test // GH-1408
	public void shouldSelectWindow() {

		List<Person> result = new ArrayList<>();

		Window<Person> firstWindow = personRepository.findAllWindowByLastname("White", CassandraScrollPosition.initial(),
				Limit.of(2));
		Window<Person> nextWindow = personRepository.findAllWindowByLastname("White",
				firstWindow.positionAt(firstWindow.size() - 1), Limit.of(10));

		result.addAll(firstWindow.getContent());
		result.addAll(nextWindow.getContent());

		assertThat(firstWindow).hasSize(2);

		assertThat(nextWindow).hasSize(1);
		assertThat(result).contains(walter, skyler, flynn);

		WindowIterator<Person> iterator = WindowIterator
				.of(scrollPosition -> personRepository.findAllWindowByLastname("White", scrollPosition, Limit.of(2)))
				.startingAt(CassandraScrollPosition.initial());

		List<Person> people = Streamable.of(() -> iterator).toList();
		assertThat(people).containsOnly(walter, skyler, flynn);
	}

	@Test // GH-1408
	public void shouldSelectWindowWithTopKeyword() {

		List<Person> result = new ArrayList<>();

		Window<Person> firstWindow = personRepository.findTop2ByLastname("White", CassandraScrollPosition.initial());
		Window<Person> nextWindow = personRepository.findTop2ByLastname("White",
				firstWindow.positionAt(firstWindow.size() - 1));

		result.addAll(firstWindow.getContent());
		result.addAll(nextWindow.getContent());

		assertThat(firstWindow).hasSize(2);
		assertThat(nextWindow).hasSize(1);
		assertThat(result).contains(walter, skyler, flynn);
	}

	@Test // GH-1407
	public void shouldSelectWithLimit() {

		List<Person> result = personRepository.findAllLimitedByLastname("White", Limit.of(2));

		assertThat(result).hasSize(2);
	}

	@Test // DATACASS-512
	public void shouldCountRecords() {

		long count = personRepository.countByLastname("White");

		assertThat(count).isEqualTo(3);
	}

	@Test // DATACASS-611
	public void shouldDeleteRecords() {

		personRepository.deleteByLastname("White");

		assertThat(personRepository.countByLastname("White")).isZero();
	}

	@Test // DATACASS-611
	public void shouldDeleteRecordsWithWasApplied() {

		boolean deleted = personRepository.deleteByLastname("White");

		assertThat(deleted).isTrue();
		assertThat(personRepository.countByLastname("White")).isZero();
	}

	@Test // DATACASS-512
	public void shouldApplyExistsProjection() {

		assertThat(personRepository.existsByLastname("White")).isTrue();
		assertThat(personRepository.existsByLastname("Schrader")).isFalse();
	}

	@Test // DATACASS-167
	public void derivedQueryOnPropertyOfEmbeddedEntity() {

		PersonWithEmbedded source = new PersonWithEmbedded();
		source.id = "id-1";
		source.name = new Name();
		source.name.firstname = "spring";
		source.name.lastname = "data";

		personWithEmbeddedRepository.save(source);

		assertThat(personWithEmbeddedRepository.findByName_Firstname("spring")).isEqualTo(source);
	}

	/**
	 * @author Mark Paluch
	 */
	static interface PersonRepository extends MapIdCassandraRepository<Person> {

		List<Person> findByLastname(String lastname);

		@Nullable
		Person findSomeByLastname(String lastname);

		Person findFirstByLastname(String lastname);

		List<Person> findByLastname(String lastname, Sort sort);

		List<Person> findByLastnameOrderByFirstnameAsc(String lastname);

		Person findByFirstnameAndLastname(String firstname, String lastname);

		Person findByMainAddress(AddressType address);

		@Query("select * from person where mainaddress = ?0")
		Person findByAddress(AddressType address);

		Person findByCreatedDate(LocalDate createdDate);

		Person findByNicknameStartsWith(String prefix);

		Person findByNicknameContains(String contains);

		Person findByNumberOfChildren(NumberOfChildren numberOfChildren);

		long countByLastname(String lastname);

		boolean deleteByLastname(String lastname);

		void deleteVoidByLastname(String lastname);

		boolean existsByLastname(String lastname);

		Slice<Person> findAllSlicedByLastname(String lastname, Pageable pageable);

		Window<Person> findAllWindowByLastname(String lastname, ScrollPosition scrollPosition, Limit limit);

		Window<Person> findTop2ByLastname(String lastname, ScrollPosition scrollPosition);

		List<Person> findAllLimitedByLastname(String lastname, Limit limit);

		Collection<PersonProjection> findPersonProjectedBy();

		Collection<PersonDto> findPersonDtoBy();

		<T> T findDtoByNicknameStartsWith(String prefix, Class<T> projectionType);

		@Query("select * from person where firstname = ?0 and lastname = 'White'")
		List<Person> findByFirstname(String firstname);

		enum NumberOfChildren {
			ZERO, ONE, TWO,
		}

		interface PersonProjection {

			String getFirstname();

			String getLastname();
		}

		class PersonDto {

			public String firstname, lastname;

			public PersonDto(String firstname, String lastname) {
				this.firstname = firstname;
				this.lastname = lastname;
			}
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	static interface EmbeddedPersonRepository extends CassandraRepository<PersonWithEmbedded, String> {

		PersonWithEmbedded findByName_Firstname(String firstname);

	}

	@Table
	static class PersonWithEmbedded {

		@Id String id;
		@Embedded.Nullable Name name;

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public Name getName() {
			return name;
		}

		public void setName(Name name) {
			this.name = name;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o)
				return true;
			if (o == null || getClass() != o.getClass())
				return false;

			PersonWithEmbedded that = (PersonWithEmbedded) o;

			if (!ObjectUtils.nullSafeEquals(id, that.id)) {
				return false;
			}
			if (!ObjectUtils.nullSafeEquals(name, that.name)) {
				return false;
			}

			return true;
		}

		@Override
		public int hashCode() {
			int result = ObjectUtils.nullSafeHashCode(id);
			result = 31 * result + ObjectUtils.nullSafeHashCode(name);
			return result;
		}
	}

	static class Name {

		@Indexed String firstname;
		String lastname;

		public String getFirstname() {
			return firstname;
		}

		public void setFirstname(String firstname) {
			this.firstname = firstname;
		}

		public String getLastname() {
			return lastname;
		}

		public void setLastname(String lastname) {
			this.lastname = lastname;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o)
				return true;
			if (o == null || getClass() != o.getClass())
				return false;

			Name name = (Name) o;

			if (!ObjectUtils.nullSafeEquals(firstname, name.firstname)) {
				return false;
			}
			if (!ObjectUtils.nullSafeEquals(lastname, name.lastname)) {
				return false;
			}

			return true;
		}

		@Override
		public int hashCode() {
			int result = ObjectUtils.nullSafeHashCode(firstname);
			result = 31 * result + ObjectUtils.nullSafeHashCode(lastname);
			return result;
		}
	}
}
