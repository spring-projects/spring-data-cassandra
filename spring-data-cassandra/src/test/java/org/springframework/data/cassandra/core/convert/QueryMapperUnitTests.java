/*
 * Copyright 2017-2018 the original author or authors.
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
package org.springframework.data.cassandra.core.convert;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Collections;
import java.util.Currency;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.AllArgsConstructor;
import org.joda.time.LocalDate;
import org.joda.time.LocalTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.cql.CqlIdentifier;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.Element;
import org.springframework.data.cassandra.core.mapping.Tuple;
import org.springframework.data.cassandra.core.mapping.UserDefinedType;
import org.springframework.data.cassandra.core.mapping.UserTypeResolver;
import org.springframework.data.cassandra.core.query.ColumnName;
import org.springframework.data.cassandra.core.query.Columns;
import org.springframework.data.cassandra.core.query.Columns.Selector;
import org.springframework.data.cassandra.core.query.Criteria;
import org.springframework.data.cassandra.core.query.CriteriaDefinition;
import org.springframework.data.cassandra.core.query.CriteriaDefinition.Operators;
import org.springframework.data.cassandra.core.query.Filter;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.cassandra.domain.TypeWithKeyClass;
import org.springframework.data.cassandra.support.UserTypeBuilder;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.domain.Sort.Order;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;

/**
 * Unit tests for {@link QueryMapper}.
 *
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class QueryMapperUnitTests {

	CassandraMappingContext mappingContext = new CassandraMappingContext();

	CassandraPersistentEntity<?> persistentEntity;

	MappingCassandraConverter cassandraConverter;

	QueryMapper queryMapper;

	UserType userType = UserTypeBuilder.forName("address").withField("street", DataType.varchar()).build();

	@Mock UserTypeResolver userTypeResolver;

	@Before
	public void before() {

		CassandraCustomConversions customConversions = new CassandraCustomConversions(
				Collections.singletonList(CurrencyConverter.INSTANCE));

		mappingContext.setCustomConversions(customConversions);
		mappingContext.setUserTypeResolver(userTypeResolver);

		cassandraConverter = new MappingCassandraConverter(mappingContext);
		cassandraConverter.setCustomConversions(customConversions);
		cassandraConverter.afterPropertiesSet();

		queryMapper = new QueryMapper(cassandraConverter);

		when(userTypeResolver.resolveType(any(CqlIdentifier.class))).thenReturn(userType);

		persistentEntity = mappingContext.getRequiredPersistentEntity(Person.class);
	}

	@Test // DATACASS-343
	public void shouldMapSimpleQuery() {

		Query query = Query.query(Criteria.where("foo_name").is("bar"));

		Filter mappedObject = queryMapper.getMappedObject(query, persistentEntity);

		CriteriaDefinition mappedCriteriaDefinition = mappedObject.iterator().next();

		assertThat(mappedCriteriaDefinition.getPredicate().getOperator()).isEqualTo(Operators.EQ);
		assertThat(mappedCriteriaDefinition.getPredicate().getValue()).isEqualTo("bar");
	}

	@Test // DATACASS-343
	public void shouldMapEnumToString() {

		Query query = Query.query(Criteria.where("foo_name").is(State.Active));

		Filter mappedObject = queryMapper.getMappedObject(query, persistentEntity);

		CriteriaDefinition mappedCriteriaDefinition = mappedObject.iterator().next();

		assertThat(mappedCriteriaDefinition.getPredicate().getValue()).isInstanceOf(String.class).isEqualTo("Active");
	}

	@Test // DATACASS-343
	public void shouldMapEnumToNumber() {

		Query query = Query.query(Criteria.where("number").is(State.Inactive));

		Filter mappedObject = queryMapper.getMappedObject(query, persistentEntity);

		CriteriaDefinition mappedCriteriaDefinition = mappedObject.iterator().next();

		assertThat(mappedCriteriaDefinition.getPredicate().getValue()).isInstanceOf(Integer.class).isEqualTo(1);
	}

	@Test // DATACASS-343
	public void shouldMapEnumToNumberIn() {

		Query query = Query.query(Criteria.where("number").in(State.Inactive));

		Filter mappedObject = queryMapper.getMappedObject(query, persistentEntity);

		CriteriaDefinition mappedCriteriaDefinition = mappedObject.iterator().next();

		assertThat(mappedCriteriaDefinition.getPredicate().getValue()).isInstanceOf(Collection.class)
				.isEqualTo(Collections.singletonList(1));
	}

	@Test // DATACASS-343
	public void shouldMapApplyingCustomConversion() {

		Query query = Query.query(Criteria.where("foo_name").is(Currency.getInstance("EUR")));

		Filter mappedObject = queryMapper.getMappedObject(query, persistentEntity);

		CriteriaDefinition mappedCriteriaDefinition = mappedObject.iterator().next();

		assertThat(mappedCriteriaDefinition.getPredicate().getOperator()).isEqualTo(Operators.EQ);
		assertThat(mappedCriteriaDefinition.getPredicate().getValue()).isEqualTo("Euro");
	}

	@Test // DATACASS-343
	public void shouldMapApplyingCustomConversionInCollection() {

		Query query = Query.query(Criteria.where("foo_name").in(Currency.getInstance("EUR")));

		Filter mappedObject = queryMapper.getMappedObject(query, persistentEntity);

		CriteriaDefinition mappedCriteriaDefinition = mappedObject.iterator().next();

		assertThat(mappedCriteriaDefinition.getPredicate().getOperator()).isEqualTo(Operators.IN);
		assertThat(mappedCriteriaDefinition.getPredicate().getValue()).isEqualTo(Collections.singletonList("Euro"));
	}

	@Test // DATACASS-343
	public void shouldMapApplyingUdtValueConversion() {

		Query query = Query.query(Criteria.where("address").is(new Address("21 Jump-Street")));

		Filter mappedObject = queryMapper.getMappedObject(query, persistentEntity);

		CriteriaDefinition mappedCriteriaDefinition = mappedObject.iterator().next();

		assertThat(mappedCriteriaDefinition.getPredicate().getOperator()).isEqualTo(Operators.EQ);
		assertThat(mappedCriteriaDefinition.getPredicate().getValue()).isInstanceOf(UDTValue.class);
		assertThat(mappedCriteriaDefinition.getPredicate().getValue().toString()).isEqualTo("{street:'21 Jump-Street'}");
	}

	@Test // DATACASS-343
	public void shouldMapApplyingUdtValueCollectionConversion() {

		Query query = Query.query(Criteria.where("address").in(new Address("21 Jump-Street")));

		Filter mappedObject = queryMapper.getMappedObject(query, persistentEntity);

		CriteriaDefinition mappedCriteriaDefinition = mappedObject.iterator().next();

		assertThat(mappedCriteriaDefinition.getPredicate().getOperator()).isEqualTo(Operators.IN);
		assertThat(mappedCriteriaDefinition.getPredicate().getValue()).isInstanceOf(Collection.class);
		assertThat(mappedCriteriaDefinition.getPredicate().getValue().toString()).isEqualTo("[{street:'21 Jump-Street'}]");
	}

	@Test // DATACASS-343
	public void shouldMapCollectionApplyingUdtValueCollectionConversion() {

		Query query = Query.query(Criteria.where("addresses").in(new Address("21 Jump-Street")));

		Filter mappedObject = queryMapper.getMappedObject(query, persistentEntity);

		CriteriaDefinition mappedCriteriaDefinition = mappedObject.iterator().next();

		assertThat(mappedCriteriaDefinition.getPredicate().getOperator()).isEqualTo(Operators.IN);
		assertThat(mappedCriteriaDefinition.getPredicate().getValue()).isInstanceOf(Collection.class);
		assertThat(mappedCriteriaDefinition.getPredicate().getValue().toString()).isEqualTo("[{street:'21 Jump-Street'}]");
	}

	@Test // DATACASS-487
	public void shouldMapUdtMapContainsKey() {

		Query query = Query.query(Criteria.where("relocations").containsKey(new Address("21 Jump-Street")));

		Filter mappedObject = queryMapper.getMappedObject(query, persistentEntity);

		CriteriaDefinition mappedCriteriaDefinition = mappedObject.iterator().next();

		assertThat(mappedCriteriaDefinition.getPredicate().getOperator()).isEqualTo(Operators.CONTAINS_KEY);
		assertThat(mappedCriteriaDefinition.getPredicate().getValue()).isInstanceOf(UDTValue.class);
		assertThat(mappedCriteriaDefinition.getPredicate().getValue().toString()).isEqualTo("{street:'21 Jump-Street'}");
	}

	@Test // DATACASS-487
	public void shouldMapUdtMapContains() {

		Query query = Query.query(Criteria.where("relocations").contains(new Address("21 Jump-Street")));

		Filter mappedObject = queryMapper.getMappedObject(query, persistentEntity);

		CriteriaDefinition mappedCriteriaDefinition = mappedObject.iterator().next();

		assertThat(mappedCriteriaDefinition.getPredicate().getOperator()).isEqualTo(Operators.CONTAINS);
		assertThat(mappedCriteriaDefinition.getPredicate().getValue()).isInstanceOf(UDTValue.class);
		assertThat(mappedCriteriaDefinition.getPredicate().getValue().toString()).isEqualTo("{street:'21 Jump-Street'}");
	}

	@Test // DATACASS-343
	public void shouldMapPropertyToColumnName() {

		Query query = Query.query(Criteria.where("firstName").is("bar"));

		Filter mappedObject = queryMapper.getMappedObject(query, persistentEntity);

		CriteriaDefinition mappedCriteriaDefinition = mappedObject.iterator().next();

		assertThat(mappedCriteriaDefinition.getColumnName()).isEqualTo(ColumnName.from(CqlIdentifier.of("first_name")));
		assertThat(mappedCriteriaDefinition.getColumnName().toString()).isEqualTo("first_name");
	}

	@Test // DATACASS-343
	public void shouldCreateSelectExpression() {

		List<Selector> selectors = queryMapper.getMappedSelectors(Columns.empty(), persistentEntity);

		assertThat(selectors).isEmpty();
	}

	@Test // DATACASS-343
	public void shouldCreateSelectExpressionWithTTL() {

		List<String> selectors = queryMapper
				.getMappedSelectors(Columns.from("number", "foo").ttl("firstName"),
						mappingContext.getRequiredPersistentEntity(Person.class))
				.stream().map(Selector::toString).collect(Collectors.toList());

		assertThat(selectors).contains("number").contains("foo").contains("TTL(first_name)");
	}

	@Test // DATACASS-343
	public void shouldIncludeColumnsSelectExpressionWithTTL() {

		List<String> selectors = queryMapper.getMappedColumnNames(Columns.from("number", "foo").ttl("firstName"),
				persistentEntity);

		assertThat(selectors).contains("number").contains("foo").hasSize(2);
	}

	@Test // DATACASS-343
	public void shouldMapQueryWithCompositePrimaryKeyClass() {

		Filter filter = Filter.from(Criteria.where("key.firstname").is("foo"));

		Filter mappedObject = queryMapper.getMappedObject(filter,
				mappingContext.getRequiredPersistentEntity(TypeWithKeyClass.class));

		assertThat(mappedObject).contains(Criteria.where("first_name").is("foo"));
	}

	@Test // DATACASS-343
	public void shouldMapSortWithCompositePrimaryKeyClass() {

		Sort sort = Sort.by("key.firstname");

		Sort mappedObject = queryMapper.getMappedSort(sort,
				mappingContext.getRequiredPersistentEntity(TypeWithKeyClass.class));

		assertThat(mappedObject).contains(new Order(Direction.ASC, "first_name"));
	}

	@Test(expected = IllegalArgumentException.class) // DATACASS-343
	public void shouldFailMappingSortByCompositePrimaryKeyClass() {

		Sort sort = Sort.by("key");

		queryMapper.getMappedSort(sort, mappingContext.getRequiredPersistentEntity(TypeWithKeyClass.class));
	}

	@Test // DATACASS-343
	public void shouldMapColumnWithCompositePrimaryKeyClass() {

		Columns columnNames = Columns.from("key.firstname");

		List<String> mappedObject = queryMapper.getMappedColumnNames(columnNames,
				mappingContext.getRequiredPersistentEntity(TypeWithKeyClass.class));

		assertThat(mappedObject).contains("first_name");
	}

	@Test // DATACASS-523
	public void shouldMapTuple() {

		MappedTuple tuple = new MappedTuple("foo");

		Filter filter = Filter.from(Criteria.where("tuple").is(tuple));

		Filter mappedObject = this.queryMapper.getMappedObject(filter,
				this.mappingContext.getRequiredPersistentEntity(Person.class));

		TupleValue tupleValue = this.mappingContext.getRequiredPersistentEntity(MappedTuple.class)
				.getTupleType().newValue();

		tupleValue.setString(0, "foo");

		assertThat(mappedObject).contains(Criteria.where("tuple").is(tupleValue));
	}

	@Test // DATACASS-302
	public void shouldMapTime() {

		Filter filter = Filter.from(Criteria.where("localDate").gt(LocalTime.fromMillisOfDay(1000)));

		Filter mappedObject = this.queryMapper.getMappedObject(filter,
				this.mappingContext.getRequiredPersistentEntity(Person.class));

		assertThat(mappedObject).contains(Criteria.where("localdate").gt(1000L));
	}

	@Test(expected = IllegalArgumentException.class) // DATACASS-523
	public void referencingTupleElementsInQueryShouldFail() {

		this.queryMapper.getMappedObject(Filter.from(Criteria.where("tuple.zip").is("123")),
				this.mappingContext.getRequiredPersistentEntity(Person.class));
	}

	static class Person {

		@Id String id;

		Address address;
		List<Address> addresses;
		Map<Address, Address> relocations;
		Currency currency;
		State state;

		Integer number;

		LocalDate localDate;

		MappedTuple tuple;

		@Column("first_name") String firstName;
	}

	@Tuple
	@AllArgsConstructor
	static class MappedTuple {
		@Element(0) String zip;
	}

	@UserDefinedType
	@AllArgsConstructor
	static class Address {
		String street;
	}

	enum State {
		Active, Inactive;
	}
}
