/*
 * Copyright 2017 the original author or authors.
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
package org.springframework.data.cassandra.convert;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

import lombok.AllArgsConstructor;

import java.util.Collection;
import java.util.Collections;
import java.util.Currency;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.cassandra.core.cql.CqlIdentifier;
import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.query.ColumnName;
import org.springframework.data.cassandra.core.query.Columns;
import org.springframework.data.cassandra.core.query.Columns.Selector;
import org.springframework.data.cassandra.core.query.Criteria;
import org.springframework.data.cassandra.core.query.CriteriaDefinition;
import org.springframework.data.cassandra.core.query.Filter;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.cassandra.domain.TypeWithKeyClass;
import org.springframework.data.cassandra.mapping.BasicCassandraMappingContext;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.mapping.Column;
import org.springframework.data.cassandra.mapping.UserDefinedType;
import org.springframework.data.cassandra.mapping.UserTypeResolver;
import org.springframework.data.cassandra.support.UserTypeBuilder;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.domain.Sort.Order;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;

/**
 * Unit tests for {@link QueryMapper}.
 *
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class QueryMapperUnitTests {

	BasicCassandraMappingContext mappingContext = new BasicCassandraMappingContext();
	CassandraPersistentEntity<?> persistentEntity;
	MappingCassandraConverter cassandraConverter;
	QueryMapper queryMapper;

	@Mock UserTypeResolver userTypeResolver;

	UserType userType = UserTypeBuilder.forName("address").withField("street", DataType.varchar()).build();

	@Before
	public void before() throws Exception {

		CustomConversions customConversions = new CustomConversions(Collections.singletonList(CurrencyConverter.INSTANCE));

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

		Query query = Query.from(Criteria.where("foo_name").is("bar"));

		Filter mappedObject = queryMapper.getMappedObject(query, persistentEntity);

		CriteriaDefinition mappedCriteriaDefinition = mappedObject.iterator().next();

		assertThat(mappedCriteriaDefinition.getPredicate().getOperator()).isEqualTo("=");
		assertThat(mappedCriteriaDefinition.getPredicate().getValue()).isEqualTo("bar");
	}

	@Test // DATACASS-343
	public void shouldMapEnumToString() {

		Query query = Query.from(Criteria.where("foo_name").is(State.Active));

		Filter mappedObject = queryMapper.getMappedObject(query, persistentEntity);

		CriteriaDefinition mappedCriteriaDefinition = mappedObject.iterator().next();

		assertThat(mappedCriteriaDefinition.getPredicate().getValue()).isInstanceOf(String.class).isEqualTo("Active");
	}

	@Test // DATACASS-343
	public void shouldMapEnumToNumber() {

		Query query = Query.from(Criteria.where("number").is(State.Inactive));

		Filter mappedObject = queryMapper.getMappedObject(query, persistentEntity);

		CriteriaDefinition mappedCriteriaDefinition = mappedObject.iterator().next();

		assertThat(mappedCriteriaDefinition.getPredicate().getValue()).isInstanceOf(Integer.class).isEqualTo(1);
	}

	@Test // DATACASS-343
	public void shouldMapEnumToNumberIn() {

		Query query = Query.from(Criteria.where("number").in(State.Inactive));

		Filter mappedObject = queryMapper.getMappedObject(query, persistentEntity);

		CriteriaDefinition mappedCriteriaDefinition = mappedObject.iterator().next();

		assertThat(mappedCriteriaDefinition.getPredicate().getValue()).isInstanceOf(Collection.class)
				.isEqualTo(Collections.singletonList(1));
	}

	@Test // DATACASS-343
	public void shouldMapApplyingCustomConversion() {

		Query query = Query.from(Criteria.where("foo_name").is(Currency.getInstance("EUR")));

		Filter mappedObject = queryMapper.getMappedObject(query, persistentEntity);

		CriteriaDefinition mappedCriteriaDefinition = mappedObject.iterator().next();

		assertThat(mappedCriteriaDefinition.getPredicate().getOperator()).isEqualTo("=");
		assertThat(mappedCriteriaDefinition.getPredicate().getValue()).isEqualTo("Euro");
	}

	@Test // DATACASS-343
	public void shouldMapApplyingCustomConversionInCollection() {

		Query query = Query.from(Criteria.where("foo_name").in(Currency.getInstance("EUR")));

		Filter mappedObject = queryMapper.getMappedObject(query, persistentEntity);

		CriteriaDefinition mappedCriteriaDefinition = mappedObject.iterator().next();

		assertThat(mappedCriteriaDefinition.getPredicate().getOperator()).isEqualTo("IN");
		assertThat(mappedCriteriaDefinition.getPredicate().getValue()).isEqualTo(Collections.singletonList("Euro"));
	}

	@Test // DATACASS-343
	public void shouldMapApplyingUdtValueConversion() {

		Query query = Query.from(Criteria.where("address").is(new Address("21 Jump-Street")));

		Filter mappedObject = queryMapper.getMappedObject(query, persistentEntity);

		CriteriaDefinition mappedCriteriaDefinition = mappedObject.iterator().next();

		assertThat(mappedCriteriaDefinition.getPredicate().getOperator()).isEqualTo("=");
		assertThat(mappedCriteriaDefinition.getPredicate().getValue()).isInstanceOf(UDTValue.class);
		assertThat(mappedCriteriaDefinition.getPredicate().getValue().toString()).isEqualTo("{street:'21 Jump-Street'}");
	}

	@Test // DATACASS-343
	public void shouldMapApplyingUdtValueCollectionConversion() {

		Query query = Query.from(Criteria.where("address").in(new Address("21 Jump-Street")));

		Filter mappedObject = queryMapper.getMappedObject(query, persistentEntity);

		CriteriaDefinition mappedCriteriaDefinition = mappedObject.iterator().next();

		assertThat(mappedCriteriaDefinition.getPredicate().getOperator()).isEqualTo("IN");
		assertThat(mappedCriteriaDefinition.getPredicate().getValue()).isInstanceOf(Collection.class);
		assertThat(mappedCriteriaDefinition.getPredicate().getValue().toString()).isEqualTo("[{street:'21 Jump-Street'}]");
	}

	@Test // DATACASS-343
	public void shouldMapCollectionApplyingUdtValueCollectionConversion() {

		Query query = Query.from(Criteria.where("addresses").in(new Address("21 Jump-Street")));

		Filter mappedObject = queryMapper.getMappedObject(query, persistentEntity);

		CriteriaDefinition mappedCriteriaDefinition = mappedObject.iterator().next();

		assertThat(mappedCriteriaDefinition.getPredicate().getOperator()).isEqualTo("IN");
		assertThat(mappedCriteriaDefinition.getPredicate().getValue()).isInstanceOf(Collection.class);
		assertThat(mappedCriteriaDefinition.getPredicate().getValue().toString()).isEqualTo("[{street:'21 Jump-Street'}]");
	}

	@Test // DATACASS-343
	public void shouldMapPropertyToColumnName() {

		Query query = Query.from(Criteria.where("firstName").is("bar"));

		Filter mappedObject = queryMapper.getMappedObject(query, persistentEntity);

		CriteriaDefinition mappedCriteriaDefinition = mappedObject.iterator().next();

		assertThat(mappedCriteriaDefinition.getColumnName()).isEqualTo(ColumnName.from(CqlIdentifier.cqlId("first_name")));
		assertThat(mappedCriteriaDefinition.getColumnName().toString()).isEqualTo("first_name");
	}

	@Test // DATACASS-343
	public void shouldCreateSelectExpression() {

		List<Selector> selectors = queryMapper.getMappedSelectors(Columns.empty(), persistentEntity);

		assertThat(selectors).isEmpty();
	}

	@Test // DATACASS-343
	public void shouldCreateSelectExpressionWithExclusion() {

		List<String> selectors = queryMapper.getMappedColumnNames(Columns.empty().exclude("id").exclude("number"),
				persistentEntity);

		assertThat(selectors).contains("address").contains("first_name").doesNotContain("id").doesNotContain("number");
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

		Sort sort = new Sort("key.firstname");

		Sort mappedObject = queryMapper.getMappedSort(sort,
				mappingContext.getRequiredPersistentEntity(TypeWithKeyClass.class));

		assertThat(mappedObject).contains(new Order(Direction.ASC, "first_name"));
	}

	@Test(expected = IllegalArgumentException.class) // DATACASS-343
	public void shouldFailMappingSortByCompositePrimaryKeyClass() {

		Sort sort = new Sort("key");

		queryMapper.getMappedSort(sort, mappingContext.getRequiredPersistentEntity(TypeWithKeyClass.class));
	}

	@Test // DATACASS-343
	public void shouldMapColumnWithCompositePrimaryKeyClass() {

		Columns columnNames = Columns.from("key.firstname");

		List<String> mappedObject = queryMapper.getMappedColumnNames(columnNames,
				mappingContext.getRequiredPersistentEntity(TypeWithKeyClass.class));

		assertThat(mappedObject).contains("first_name");
	}

	static class Person {

		@Id String id;

		Address address;
		List<Address> addresses;
		Currency currency;
		State state;

		Integer number;

		@Column("first_name") String firstName;
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
