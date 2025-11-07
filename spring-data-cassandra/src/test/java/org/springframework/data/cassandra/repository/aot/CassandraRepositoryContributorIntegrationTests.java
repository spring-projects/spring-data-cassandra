/*
 * Copyright 2025 the original author or authors.
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
package org.springframework.data.cassandra.repository.aot;

import static org.assertj.core.api.Assertions.*;

import java.lang.reflect.UndeclaredThrowableException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Import;
import org.springframework.data.cassandra.config.SchemaAction;
import org.springframework.data.cassandra.core.cql.QueryOptions;
import org.springframework.data.cassandra.core.cql.SessionCallback;
import org.springframework.data.cassandra.core.query.CassandraScrollPosition;
import org.springframework.data.cassandra.domain.AddressType;
import org.springframework.data.cassandra.domain.Person;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;
import org.springframework.data.cassandra.repository.support.AbstractSpringDataEmbeddedCassandraIntegrationTest;
import org.springframework.data.cassandra.repository.support.IntegrationTestConfig;
import org.springframework.data.domain.Limit;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.ScoringFunction;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Vector;
import org.springframework.data.domain.Window;
import org.springframework.data.util.Streamable;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;

/**
 * Integration tests for AOT processing via {@link CassandraRepositoryContributor}.
 *
 * @author Mark Paluch
 */
@SpringJUnitConfig(
		classes = CassandraRepositoryContributorIntegrationTests.CassandraRepositoryContributorConfiguration.class)
class CassandraRepositoryContributorIntegrationTests extends AbstractSpringDataEmbeddedCassandraIntegrationTest {

	@Autowired PersonRepository fragment;
	private static boolean indexExists = false;

	@Configuration
	@Import(Config.class)
	static class CassandraRepositoryContributorConfiguration extends AotFragmentTestConfigurationSupport {

		public CassandraRepositoryContributorConfiguration() {
			super(PersonRepository.class, Config.class);
		}
	}

	@Configuration
	@EnableCassandraRepositories(considerNestedRepositories = true,
			includeFilters = { @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, value = Person.class) })
	public static class Config extends IntegrationTestConfig {

		@Override
		protected Set<Class<?>> getInitialEntitySet() {
			return new HashSet<>(Arrays.asList(Person.class));
		}

		@Override
		public SchemaAction getSchemaAction() {
			return SchemaAction.CREATE;
		}
	}

	private Person walter;
	private Person skyler;
	private Person flynn;

	@BeforeEach
	void before() {

		template.delete(Person.class);

		if (!indexExists) {

			template.getCqlOperations().execute(
					"CREATE CUSTOM INDEX IF NOT EXISTS person_firstname ON person (firstname) USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = {'mode': 'CONTAINS' };");
			template.getCqlOperations()
					.execute("CREATE INDEX IF NOT EXISTS person_numberofchildren ON person (numberofchildren)");

			template.getCqlOperations().execute((SessionCallback<? extends Object>) session -> {

				Awaitility.await().until(() -> {
					KeyspaceMetadata keyspace = session.getMetadata().getKeyspace(session.getKeyspace().get()).get();

					Map<CqlIdentifier, IndexMetadata> indexes = keyspace.getTable("person").get().getIndexes();
					return indexes.size() > 1;
				});

				return null;
			});
			indexExists = true;
		}

		Person person = new Person("Walter", "White");
		person.setNumberOfChildren(2);

		person.setMainAddress(new AddressType("Albuquerque", "USA"));

		person.setAlternativeAddresses(Arrays.asList(new AddressType("Albuquerque", "USA"),
				new AddressType("New Hampshire", "USA"), new AddressType("Grocery Store", "Mexico")));

		walter = template.insert(person);
		skyler = template.insert(new Person("Skyler", "White"));
		flynn = template.insert(new Person("Flynn (Walter Jr.)", "White"));
	}

	@Test // GH-1566
	void shouldFindByFirstname() {

		Person walter = fragment.findByFirstname("Walter");

		assertThat(walter).isNotNull();
		assertThat(walter.getFirstname()).isEqualTo("Walter");
	}

	@Test // GH-1566
	void shouldFindByFirstnameWithQueryOptions() {

		Person walter = fragment.findByFirstname("Walter", QueryOptions.builder().build());

		assertThat(walter).isNotNull();
		assertThat(walter.getFirstname()).isEqualTo("Walter");
	}

	@Test // GH-1566
	void shouldQuerySlice() {

		Slice<Person> first = fragment.findTop2SliceByLastname("White", Pageable.unpaged());
		Slice<Person> second = fragment.findTop2SliceByLastname("White", first.nextPageable());

		assertThat(first).hasSize(2);
		assertThat(first.hasNext()).isTrue();
		assertThat(second).hasSize(1);
		assertThat(second.hasNext()).isFalse();
	}

	@Test // GH-1566
	void shouldQueryWindow() {

		Window<Person> first = fragment.findWindowByLastname("White", CassandraScrollPosition.initial(), Limit.of(2));
		Window<Person> second = fragment.findWindowByLastname("White", first.positionAt(1), Limit.of(2));

		assertThat(first).hasSize(2);
		assertThat(first.hasNext()).isTrue();
		assertThat(second).hasSize(1);
		assertThat(second.hasNext()).isFalse();
	}

	@Test // GH-1566
	void shouldFindOptionalByFirstname() {

		assertThat(fragment.findOptionalByFirstname("Walter")).isPresent();
		assertThat(fragment.findOptionalByFirstname("Hank")).isEmpty();
	}

	@Test // GH-1566
	void shouldApplySorting() {

		assertThat(fragment.findByLastname("White", Sort.by("firstname"))).extracting(Person::getFirstname)
				.containsSequence("Flynn (Walter Jr.)", "Skyler", "Walter");
		assertThat(fragment.findByLastnameOrderByFirstnameAsc("White")).extracting(Person::getFirstname)
				.containsSequence("Flynn (Walter Jr.)", "Skyler", "Walter");
	}

	@Test // GH-1620
	void shouldConvertResultToStreamable() {

		assertThat(fragment.streamByLastname("White", Sort.by("firstname")))
			.isInstanceOf(Streamable.class) //
			.extracting(Person::getFirstname) //
			.containsExactly("Flynn (Walter Jr.)", "Skyler", "Walter");
	}

	@Test // GH-1620
	void shouldConvertResultToStreamableWhenPageableParameterIsUsed() {

		assertThat(fragment.streamByLastname("White", PageRequest.of(0, 2, Sort.by("firstname"))))
			.isInstanceOf(Streamable.class) //
			.extracting(Person::getFirstname) //
			.containsExactly("Flynn (Walter Jr.)", "Skyler");
	}

	@Test // GH-1566
	void shouldFindByFirstnameContains() {

		Person walter = fragment.findByFirstnameContains("Walter Jr");

		assertThat(walter).isNotNull();
		assertThat(walter.getFirstname()).isEqualTo("Flynn (Walter Jr.)");
	}

	@Test // GH-1566
	void shouldFindByGteLte() {

		assertThat(fragment.findByNumberOfChildrenGreaterThan(1)).hasSize(1);
		assertThat(fragment.findByNumberOfChildrenGreaterThan(2)).isEmpty();

		assertThat(fragment.findByNumberOfChildrenGreaterThanEqual(2)).hasSize(1);
		assertThat(fragment.findByNumberOfChildrenGreaterThanEqual(3)).isEmpty();

		assertThat(fragment.findByNumberOfChildrenLessThan(3)).hasSize(3);
		assertThat(fragment.findByNumberOfChildrenLessThan(1)).hasSize(2);

		assertThat(fragment.findByNumberOfChildrenLessThanEqual(2)).hasSize(3);
		assertThat(fragment.findByNumberOfChildrenLessThanEqual(1)).hasSize(2);
	}

	@Test // GH-1566
	void shouldFindTrueFalse() {

		assertThat(fragment.findByCoolIsTrue()).isEmpty();
		assertThat(fragment.findByCoolIsFalse()).hasSize(3);
	}

	@Test // GH-1566
	void shouldFindContaining() {

		assertThat(fragment.findByAlternativeAddressesContaining(walter.getAlternativeAddresses().get(0)))
				.containsOnly(walter);

		assertThat(fragment.findByAlternativeAddressesContaining(new AddressType())).isEmpty();
	}

	@Test // GH-1566
	void shouldApplyExistsCountProjection() {

		assertThat(fragment.existsByLastname("White")).isTrue();
		assertThat(fragment.countByLastname("White")).isEqualTo(3);
	}

	@Test // GH-1566
	void shouldFindByDeclaredFirstname() {

		Person walter = fragment.findDeclaredByFirstname("Walter");

		assertThat(walter).isNotNull();
		assertThat(walter.getFirstname()).isEqualTo("Walter");
	}

	@Test // GH-1566
	void shouldFindByDeclaredPositionalFirstname() {

		Person walter = fragment.findDeclaredByPositionalFirstname("Walter");

		assertThat(walter).isNotNull();
		assertThat(walter.getFirstname()).isEqualTo("Walter");
	}

	@Test // GH-1566
	void shouldFindByDeclaredExpression() {

		Person walter = fragment.findDeclaredByExpression("Walter");

		assertThat(walter).isNotNull();
		assertThat(walter.getFirstname()).isEqualTo("Walter");
	}

	@Test // GH-1566
	void shouldFindByDeclaredPropertyPlaceholderExpression() {

		Person walter = fragment.findDeclaredByExpression();

		assertThat(walter).isNull();
	}

	@Test // GH-1566
	void shouldApplyDeclaredExistsCountProjection() {

		assertThat(fragment.existsDeclaredByLastname("White")).isTrue();
		assertThat(fragment.countDeclaredByLastname("White")).isEqualTo(3);
	}

	@Test // GH-1566
	void shouldQueryDeclaredSlice() {

		Slice<Person> first = fragment.findDeclaredSliceByLastname("White", Pageable.ofSize(2));
		Slice<Person> second = fragment.findDeclaredSliceByLastname("White", first.nextPageable());

		assertThat(first).hasSize(2);
		assertThat(first.hasNext()).isTrue();
		assertThat(second).hasSize(1);
		assertThat(second.hasNext()).isFalse();
	}

	@Test // GH-1566
	void shouldQueryDeclaredWindow() {

		Window<Person> first = fragment.findDeclaredWindowByLastname("White", CassandraScrollPosition.initial(), 3,
				Limit.of(2));
		Window<Person> second = fragment.findDeclaredWindowByLastname("White", first.positionAt(1), 3, Limit.of(2));

		assertThat(first).hasSize(2);
		assertThat(first.hasNext()).isTrue();
		assertThat(second).hasSize(1);
		assertThat(second.hasNext()).isFalse();
	}

	@Test // GH-1566
	void shouldFindNamedQuery() {

		Person walter = fragment.findNamedByFirstname("Walter");

		assertThat(walter).isNotNull();
	}

	@Test // GH-1566
	void shouldReturnResultSet() {

		ResultSet resultSet = fragment.findResultSetByFirstname("Walter");

		assertThat(resultSet).isNotNull();
		assertThat(resultSet.one().getString("firstname")).isEqualTo("Walter");
	}

	@Test // GH-1566
	void shouldReturnDeclaredResultSet() {

		ResultSet resultSet = fragment.findDeclaredResultSetByFirstname("Walter");

		assertThat(resultSet).isNotNull();
		assertThat(resultSet.one().getString("firstname")).isEqualTo("Walter");
	}

	@Test // GH-1566
	void shouldReturnMap() {

		Map<String, Object> map = fragment.findMapByFirstname("Walter");

		assertThat(map).isNotNull();
		assertThat(map.get("firstname")).isEqualTo("Walter");
	}

	@Test // GH-1566
	void shouldReturnDeclaredMap() {

		Map<String, Object> map = fragment.findDeclaredMapByFirstname("Walter");

		assertThat(map).isNotNull();
		assertThat(map.get("firstname")).isEqualTo("Walter");
	}

	@Test // GH-1566
	void shouldProjectToDto() {

		PersonRepository.PersonDto walter = fragment.findOneDtoProjectionByFirstname("Walter");

		assertThat(walter).isNotNull();
		assertThat(walter.firstname).isEqualTo("Walter");
	}

	@Test // GH-1566
	void shouldProjectManyToDto() {

		List<PersonRepository.PersonDto> walter = fragment.findDtoProjectionByFirstname("Walter");

		assertThat(walter).hasSize(1);
		assertThat(walter).extracting(PersonRepository.PersonDto::getFirstname).containsOnly("Walter");
	}

	@Test // GH-1566
	void shouldProjectStreamToDto() {

		Stream<PersonRepository.PersonDto> walter = fragment.streamDtoProjectionByFirstname("Walter");

		assertThat(walter).hasSize(1).extracting(PersonRepository.PersonDto::getFirstname).containsOnly("Walter");
	}

	@Test // GH-1566
	void shouldProjectDeclaredToDto() {

		PersonRepository.PersonDto walter = fragment.findOneDeclaredDtoProjectionByFirstname("Walter");

		assertThat(walter).isNotNull();
		assertThat(walter.getFirstname()).isEqualTo("Walter");
	}

	@Test // GH-1566
	void shouldProjectDeclaredSimpleType() {

		assertThat(fragment.findDeclaredNumberOfChildrenByFirstname("Walter")).isEqualTo(2);
		assertThat(fragment.findDeclaredNumberOfChildrenByFirstname("Flynn (Walter Jr.)")).isEqualTo(0);
		assertThat(fragment.findDeclaredListNumberOfChildrenByFirstname("Walter")).containsOnly(2);
	}

	@Test // GH-1566
	void shouldProjectToInterface() {

		PersonRepository.PersonProjection walter = fragment.findOneInterfaceProjectionByFirstname("Walter");

		assertThat(walter).isNotNull();
		assertThat(walter.getFirstname()).isEqualTo("Walter");
	}

	@Test // GH-1566
	void shouldProjectManyToInterface() {

		List<PersonRepository.PersonProjection> walter = fragment.findInterfaceProjectionByFirstname("Walter");

		assertThat(walter).hasSize(1);
		assertThat(walter).extracting(PersonRepository.PersonProjection::getFirstname).containsOnly("Walter");
	}

	@Test // GH-1566
	void shouldProjectStreamToInterface() {

		Stream<PersonRepository.PersonProjection> walter = fragment.streamInterfaceProjectionByFirstname("Walter");

		assertThat(walter).hasSize(1).extracting(PersonRepository.PersonProjection::getFirstname).containsOnly("Walter");
	}

	@Test // GH-1566
	void shouldProjectDeclaredToInterface() {

		PersonRepository.PersonProjection walter = fragment.findOneDeclaredInterfaceProjectionByFirstname("Walter");

		assertThat(walter).isNotNull();
		assertThat(walter.getFirstname()).isEqualTo("Walter");
	}

	@Test // GH-1566
	void shouldProjectDynamically() {

		PersonRepository.PersonProjection projection = fragment.findOneProjectionByFirstname("Walter",
				PersonRepository.PersonProjection.class);

		assertThat(projection).isNotNull();
		assertThat(projection.getFirstname()).isEqualTo("Walter");

		List<PersonRepository.PersonProjection> projections = fragment.findProjectionByFirstname("Walter",
				PersonRepository.PersonProjection.class);

		assertThat(projections).hasSize(1);
		assertThat(projections).extracting(PersonRepository.PersonProjection::getFirstname).containsOnly("Walter");
	}

	@Test // GH-1566
	@Disabled("not supported")
	void shouldProjectDeclaredDynamically() {

		PersonRepository.PersonProjection projection = fragment.findOneDeclaredProjectionByFirstname("Walter",
				PersonRepository.PersonProjection.class);

		assertThat(projection).isNotNull();
		assertThat(projection.getFirstname()).isEqualTo("Walter");

		List<PersonRepository.PersonProjection> projections = fragment.findDeclaredProjectionByFirstname("Walter",
				PersonRepository.PersonProjection.class);

		assertThat(projections).hasSize(1);
		assertThat(projections).extracting(PersonRepository.PersonProjection::getFirstname).containsOnly("Walter");
	}

	@Test // GH-1566
	void shouldProjectToDtoDynamically() {

		PersonRepository.PersonDto projection = fragment.findOneProjectionByFirstname("Walter",
				PersonRepository.PersonDto.class);

		assertThat(projection).isNotNull();
		assertThat(projection.getFirstname()).isEqualTo("Walter");

		List<PersonRepository.PersonDto> projections = fragment.findProjectionByFirstname("Walter",
				PersonRepository.PersonDto.class);

		assertThat(projections).hasSize(1);
		assertThat(projections).extracting(PersonRepository.PersonDto::getFirstname).containsOnly("Walter");
	}

	@Test // GH-1566
	void shouldProjectDeclaredToDtoDynamically() {

		PersonRepository.PersonDto projection = fragment.findOneDeclaredProjectionByFirstname("Walter",
				PersonRepository.PersonDto.class);

		assertThat(projection).isNotNull();
		assertThat(projection.getFirstname()).isEqualTo("Walter");

		List<PersonRepository.PersonDto> projections = fragment.findDeclaredProjectionByFirstname("Walter",
				PersonRepository.PersonDto.class);

		assertThat(projections).hasSize(1);
		assertThat(projections).extracting(PersonRepository.PersonDto::getFirstname).containsOnly("Walter");
	}

	// TODO: Vector Search

	@Test // GH-1566
	void vectorSearchNotSupportedYet() {
		assertThatExceptionOfType(UndeclaredThrowableException.class)
				.isThrownBy(() -> fragment.findAllByVector(Vector.of(1f), ScoringFunction.cosine()))
				.withCauseInstanceOf(NoSuchMethodException.class);
	}
}
