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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.springframework.data.cassandra.core.cql.QueryOptions;
import org.springframework.data.cassandra.domain.AddressType;
import org.springframework.data.cassandra.domain.Person;
import org.springframework.data.cassandra.repository.AllowFiltering;
import org.springframework.data.cassandra.repository.Consistency;
import org.springframework.data.cassandra.repository.CountQuery;
import org.springframework.data.cassandra.repository.ExistsQuery;
import org.springframework.data.cassandra.repository.Query;
import org.springframework.data.domain.Limit;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.ScoringFunction;
import org.springframework.data.domain.ScrollPosition;
import org.springframework.data.domain.SearchResults;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Vector;
import org.springframework.data.domain.Window;
import org.springframework.data.repository.CrudRepository;

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import org.springframework.data.util.Streamable;

/**
 * AOT repository interface for {@link Person} entities.
 *
 * @author Mark Paluch
 */
public interface PersonRepository extends CrudRepository<Person, String> {

	@Query(idempotent = Query.Idempotency.IDEMPOTENT)
	Person findByFirstname(String firstname);

	@Consistency(DefaultConsistencyLevel.ONE)
	Person findByFirstname(String firstname, QueryOptions queryOptions);

	Slice<Person> findTop2SliceByLastname(String lastname, Pageable pageable);

	Window<Person> findWindowByLastname(String lastname, ScrollPosition scrollPosition, Limit limit);

	Optional<Person> findOptionalByFirstname(String firstname);

	List<Person> findByLastname(String lastname, Sort sort);

	Streamable<Person> streamByLastname(String lastname, Sort sort);

	Streamable<Person> streamByLastname(String lastname, Pageable pageable);

	List<Person> findByLastnameOrderByFirstnameAsc(String lastname);

	Person findByFirstnameStartsWith(String prefix);

	Person findByFirstnameContains(String contains);

	@AllowFiltering
	List<Person> findByNumberOfChildrenGreaterThan(int ch);

	@AllowFiltering
	List<Person> findByNumberOfChildrenGreaterThanEqual(int ch);

	@AllowFiltering
	List<Person> findByNumberOfChildrenLessThan(int ch);

	@AllowFiltering
	List<Person> findByNumberOfChildrenLessThanEqual(int ch);

	@AllowFiltering
	List<Person> findByCoolIsTrue();

	@AllowFiltering
	List<Person> findByCoolIsFalse();

	@AllowFiltering
	List<Person> findByAlternativeAddressesContaining(AddressType addressType);

	int countByLastname(String lastname);

	boolean existsByLastname(String lastname);

	// -------------------------------------------------------------------------
	// Declared Queries
	// -------------------------------------------------------------------------

	@Query(value = "select * from person where firstname = :firstname", idempotent = Query.Idempotency.IDEMPOTENT)
	Person findDeclaredByFirstname(String firstname);

	@Query(value = "select * from person where firstname = ?0")
	Person findDeclaredByPositionalFirstname(String firstname);

	@CountQuery(value = "select COUNT(*) from person where lastname = ?0")
	int countDeclaredByLastname(String lastname);

	@ExistsQuery(value = "select COUNT(*) from person where lastname = ?0")
	boolean existsDeclaredByLastname(String lastname);

	@Query(value = "select * from person where lastname = ?0 LIMIT 3")
	Slice<Person> findDeclaredSliceByLastname(String lastname, Pageable pageable);

	@Query(value = "select * from person where lastname = :lastname LIMIT :sliceLimit")
	Window<Person> findDeclaredWindowByLastname(String lastname, ScrollPosition scrollPosition, int sliceLimit,
			Limit pageSize);

	// -------------------------------------------------------------------------
	// Value Expressions
	// -------------------------------------------------------------------------

	@Query(value = "select * from person where firstname = :#{#firstname}")
	Person findDeclaredByExpression(String firstname);

	@Query(value = "select * from person where firstname = :${user.dir}")
	Person findDeclaredByExpression();

	// -------------------------------------------------------------------------
	// Named Queries
	// -------------------------------------------------------------------------

	Person findNamedByFirstname(String firstname);

	// -------------------------------------------------------------------------
	// Projections: ResultSet, Map
	// -------------------------------------------------------------------------

	ResultSet findResultSetByFirstname(String firstname);

	@Query(value = "select * from person where firstname = ?0")
	ResultSet findDeclaredResultSetByFirstname(String firstname);

	Map<String, Object> findMapByFirstname(String firstname);

	@Query(value = "select * from person where firstname = ?0")
	Map<String, Object> findDeclaredMapByFirstname(String firstname);

	// -------------------------------------------------------------------------
	// Projections: DTO
	// -------------------------------------------------------------------------

	PersonDto findOneDtoProjectionByFirstname(String firstname);

	List<PersonDto> findDtoProjectionByFirstname(String firstname);

	Stream<PersonDto> streamDtoProjectionByFirstname(String firstname);

	@Query(value = "select * from person where firstname = :#{#firstname}")
	PersonDto findOneDeclaredDtoProjectionByFirstname(String firstname);

	// -------------------------------------------------------------------------
	// Projections: Single-field
	// -------------------------------------------------------------------------

	@Query("select numberOfChildren from person where firstname = :firstname")
	int findDeclaredNumberOfChildrenByFirstname(String firstname);

	@Query("select numberOfChildren from person where firstname = :firstname")
	List<Integer> findDeclaredListNumberOfChildrenByFirstname(String firstname);

	// -------------------------------------------------------------------------
	// Projections: Interface
	// -------------------------------------------------------------------------

	PersonProjection findOneInterfaceProjectionByFirstname(String firstname);

	List<PersonProjection> findInterfaceProjectionByFirstname(String firstname);

	Stream<PersonProjection> streamInterfaceProjectionByFirstname(String firstname);

	@Query(value = "select * from person where firstname = :#{#firstname}")
	PersonProjection findOneDeclaredInterfaceProjectionByFirstname(String firstname);

	// -------------------------------------------------------------------------
	// Projections: Dynamic
	// -------------------------------------------------------------------------

	<T> T findOneProjectionByFirstname(String firstname, Class<T> projectionType);

	@Query(value = "select * from person where firstname = :firstname")
	<T> T findOneDeclaredProjectionByFirstname(String firstname, Class<T> projectionType);

	<T> List<T> findProjectionByFirstname(String firstname, Class<T> projectionType);

	@Query(value = "select * from person where firstname = :firstname")
	<T> List<T> findDeclaredProjectionByFirstname(String firstname, Class<T> projectionType);

	// -------------------------------------------------------------------------
	// Excluded
	// -------------------------------------------------------------------------

	SearchResults<Person> findAllByVector(Vector vector, ScoringFunction scoringFunction);

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
	}
}
