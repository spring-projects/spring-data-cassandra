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
package org.springframework.data.cassandra.core.convert;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.springframework.data.domain.Sort.Order.*;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Currency;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.StatementFactory;
import org.springframework.data.cassandra.core.cql.util.StatementBuilder;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.Element;
import org.springframework.data.cassandra.core.mapping.Embedded;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.core.mapping.Tuple;
import org.springframework.data.cassandra.core.mapping.UserDefinedType;
import org.springframework.data.cassandra.core.mapping.UserTypeResolver;
import org.springframework.data.cassandra.core.mapping.VectorType;
import org.springframework.data.cassandra.core.query.ColumnName;
import org.springframework.data.cassandra.core.query.Columns;
import org.springframework.data.cassandra.core.query.Columns.Selector;
import org.springframework.data.cassandra.core.query.Criteria;
import org.springframework.data.cassandra.core.query.CriteriaDefinition;
import org.springframework.data.cassandra.core.query.CriteriaDefinition.Operators;
import org.springframework.data.cassandra.core.query.Filter;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.cassandra.domain.TypeWithKeyClass;
import org.springframework.data.cassandra.support.UserDefinedTypeBuilder;
import org.springframework.data.convert.PropertyValueConverter;
import org.springframework.data.convert.ValueConverter;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.domain.Sort.Order;
import org.springframework.lang.Nullable;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.data.CqlVector;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataTypes;

/**
 * Unit tests for {@link QueryMapper}.
 *
 * @author Mark Paluch
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class QueryMapperUnitTests {

	private final CassandraMappingContext mappingContext = new CassandraMappingContext();

	private MappingCassandraConverter cassandraConverter;

	private CassandraPersistentEntity<?> personPersistentEntity;

	private QueryMapper queryMapper;

	private final com.datastax.oss.driver.api.core.type.UserDefinedType userType = UserDefinedTypeBuilder
			.forName("address").withField("street", DataTypes.TEXT).build();

	@Mock UserTypeResolver userTypeResolver;

	@BeforeEach
	void before() {

		CassandraCustomConversions customConversions = new CassandraCustomConversions(
				Collections.singletonList(CurrencyConverter.INSTANCE));

		when(userTypeResolver.resolveType(any(CqlIdentifier.class))).thenReturn(userType);

		mappingContext.setCustomConversions(customConversions);
		mappingContext.setUserTypeResolver(userTypeResolver);

		cassandraConverter = new MappingCassandraConverter(mappingContext);

		cassandraConverter.setCustomConversions(customConversions);
		cassandraConverter.afterPropertiesSet();

		queryMapper = new QueryMapper(cassandraConverter);
		personPersistentEntity = mappingContext.getRequiredPersistentEntity(Person.class);
	}

	@Test // DATACASS-343
	void shouldMapSimpleQuery() {

		Query query = Query.query(Criteria.where("foo_name").is("bar"));

		Filter mappedObject = queryMapper.getMappedObject(query, personPersistentEntity);

		CriteriaDefinition mappedCriteriaDefinition = mappedObject.iterator().next();

		assertThat(mappedCriteriaDefinition.getPredicate().getOperator()).isEqualTo(Operators.EQ);
		assertThat(mappedCriteriaDefinition.getPredicate().getValue()).isEqualTo("bar");
	}

	@Test // DATACASS-343
	void shouldMapEnumToString() {

		Query query = Query.query(Criteria.where("foo_name").is(State.Active));

		Filter mappedObject = queryMapper.getMappedObject(query, personPersistentEntity);

		CriteriaDefinition mappedCriteriaDefinition = mappedObject.iterator().next();

		assertThat(mappedCriteriaDefinition.getPredicate().getValue()).isInstanceOf(String.class).isEqualTo("Active");
	}

	@Test // DATACASS-343
	void shouldMapEnumToNumber() {

		Query query = Query.query(Criteria.where("number").is(State.Inactive));

		Filter mappedObject = queryMapper.getMappedObject(query, personPersistentEntity);

		CriteriaDefinition mappedCriteriaDefinition = mappedObject.iterator().next();

		assertThat(mappedCriteriaDefinition.getPredicate().getValue()).isInstanceOf(Integer.class).isEqualTo(1);
	}

	@Test // DATACASS-343
	void shouldMapEnumToNumberIn() {

		Query query = Query.query(Criteria.where("number").in(State.Inactive));

		Filter mappedObject = queryMapper.getMappedObject(query, personPersistentEntity);

		CriteriaDefinition mappedCriteriaDefinition = mappedObject.iterator().next();

		assertThat(mappedCriteriaDefinition.getPredicate().getValue()).isInstanceOf(Collection.class)
				.isEqualTo(Collections.singletonList(1));
	}

	@Test // DATACASS-343
	void shouldMapApplyingCustomConversion() {

		Query query = Query.query(Criteria.where("foo_name").is(Currency.getInstance("EUR")));

		Filter mappedObject = queryMapper.getMappedObject(query, personPersistentEntity);

		CriteriaDefinition mappedCriteriaDefinition = mappedObject.iterator().next();

		assertThat(mappedCriteriaDefinition.getPredicate().getOperator()).isEqualTo(Operators.EQ);
		assertThat(mappedCriteriaDefinition.getPredicate().getValue()).isEqualTo("Euro");
	}

	@Test // DATACASS-343
	void shouldMapApplyingCustomConversionInCollection() {

		Query query = Query.query(Criteria.where("foo_name").in(Currency.getInstance("EUR")));

		Filter mappedObject = queryMapper.getMappedObject(query, personPersistentEntity);

		CriteriaDefinition mappedCriteriaDefinition = mappedObject.iterator().next();

		assertThat(mappedCriteriaDefinition.getPredicate().getOperator()).isEqualTo(Operators.IN);
		assertThat(mappedCriteriaDefinition.getPredicate().getValue()).isEqualTo(Collections.singletonList("Euro"));
	}

	@Test // DATACASS-343
	void shouldMapApplyingUdtValueConversion() {

		Query query = Query.query(Criteria.where("address").is(new Address("21 Jump-Street")));

		Filter mappedObject = queryMapper.getMappedObject(query, personPersistentEntity);

		CriteriaDefinition mappedCriteriaDefinition = mappedObject.iterator().next();
		CriteriaDefinition.Predicate predicate = mappedCriteriaDefinition.getPredicate();

		assertThat(predicate.getOperator()).isEqualTo(Operators.EQ);
		assertThat(predicate.getValue()).isInstanceOf(UdtValue.class);
		assertThat(predicate.as(UdtValue.class::cast).getFormattedContents()).isEqualTo("{street:'21 Jump-Street'}");
	}

	@Test // DATACASS-343
	@SuppressWarnings("unchecked")
	void shouldMapApplyingUdtValueCollectionConversion() {

		Query query = Query.query(Criteria.where("address").in(new Address("21 Jump-Street")));

		Filter mappedObject = queryMapper.getMappedObject(query, personPersistentEntity);

		CriteriaDefinition mappedCriteriaDefinition = mappedObject.iterator().next();

		CriteriaDefinition.Predicate predicate = mappedCriteriaDefinition.getPredicate();

		assertThat(predicate.getOperator()).isEqualTo(Operators.IN);
		assertThat(predicate.getValue()).isInstanceOf(Collection.class);
		assertThat((List<UdtValue>) predicate.getValue()).extracting(UdtValue::getFormattedContents)
				.contains("{street:'21 Jump-Street'}");
	}

	@Test // DATACASS-343
	@SuppressWarnings("unchecked")
	void shouldMapCollectionApplyingUdtValueCollectionConversion() {

		Query query = Query.query(Criteria.where("address").in(new Address("21 Jump-Street")));

		Filter mappedObject = queryMapper.getMappedObject(query, personPersistentEntity);

		CriteriaDefinition mappedCriteriaDefinition = mappedObject.iterator().next();
		CriteriaDefinition.Predicate predicate = mappedCriteriaDefinition.getPredicate();

		assertThat(predicate.getOperator()).isEqualTo(Operators.IN);
		assertThat(predicate.getValue()).isInstanceOf(Collection.class);
		assertThat((List<UdtValue>) predicate.getValue()).extracting(UdtValue::getFormattedContents)
				.contains("{street:'21 Jump-Street'}");
	}

	@Test // DATACASS-487
	void shouldMapUdtMapContainsKey() {

		Query query = Query.query(Criteria.where("relocations").containsKey(new Address("21 Jump-Street")));

		Filter mappedObject = queryMapper.getMappedObject(query, personPersistentEntity);

		CriteriaDefinition mappedCriteriaDefinition = mappedObject.iterator().next();

		assertThat(mappedCriteriaDefinition.getPredicate().getOperator()).isEqualTo(Operators.CONTAINS_KEY);
		assertThat(mappedCriteriaDefinition.getPredicate().getValue()).isInstanceOf(UdtValue.class);
		assertThat(((UdtValue) mappedCriteriaDefinition.getPredicate().getValue()).getFormattedContents())
				.isEqualTo("{street:'21 Jump-Street'}");
	}

	@Test // DATACASS-487
	void shouldMapUdtMapContains() {

		Query query = Query.query(Criteria.where("relocations").contains(new Address("21 Jump-Street")));

		Filter mappedObject = queryMapper.getMappedObject(query, personPersistentEntity);

		CriteriaDefinition mappedCriteriaDefinition = mappedObject.iterator().next();

		assertThat(mappedCriteriaDefinition.getPredicate().getOperator()).isEqualTo(Operators.CONTAINS);
		assertThat(mappedCriteriaDefinition.getPredicate().getValue()).isInstanceOf(UdtValue.class);
		assertThat(((UdtValue) mappedCriteriaDefinition.getPredicate().getValue()).getFormattedContents())
				.isEqualTo("{street:'21 Jump-Street'}");
	}

	@Test // DATACASS-343
	void shouldMapPropertyToColumnName() {

		Query query = Query.query(Criteria.where("firstName").is("bar"));

		Filter mappedObject = queryMapper.getMappedObject(query, personPersistentEntity);

		CriteriaDefinition mappedCriteriaDefinition = mappedObject.iterator().next();

		assertThat(mappedCriteriaDefinition.getColumnName())
				.isEqualTo(ColumnName.from(CqlIdentifier.fromCql("first_name")));
		assertThat(mappedCriteriaDefinition.getColumnName().toString()).isEqualTo("first_name");
	}

	@Test // GH-1449
	void shouldConsiderPropertyValueConverter() {

		Query query = Query.query(Criteria.where("reverseName").is("Heisenberg"));

		Filter mappedObject = queryMapper.getMappedObject(query, personPersistentEntity);

		CriteriaDefinition mappedCriteriaDefinition = mappedObject.iterator().next();
		assertThat(mappedCriteriaDefinition.getPredicate().getValue()).isEqualTo("grebnesieH");
	}

	@Test // DATACASS-343
	void shouldCreateSelectExpression() {

		List<Selector> selectors = queryMapper.getMappedSelectors(Columns.empty(), personPersistentEntity);

		assertThat(selectors).isEmpty();
	}

	@Test // DATACASS-343
	void shouldCreateSelectExpressionWithTTL() {

		List<String> selectors = queryMapper
				.getMappedSelectors(Columns.from("number", "foo").ttl("firstName"), personPersistentEntity).stream()
				.map(Selector::toString).collect(Collectors.toList());

		assertThat(selectors).contains("number").contains("foo").contains("TTL(first_name)");
	}

	@Test // DATACASS-343
	void shouldIncludeColumnsSelectExpressionWithTTL() {

		List<CqlIdentifier> selectors = queryMapper.getMappedColumnNames(Columns.from("number", "foo").ttl("firstName"),
				personPersistentEntity);

		assertThat(selectors).contains(CqlIdentifier.fromCql("number"), CqlIdentifier.fromCql("foo")).hasSize(2);
	}

	@Test // DATACASS-343
	void shouldMapQueryWithCompositePrimaryKeyClass() {

		Filter filter = Filter.from(Criteria.where("key.firstname").is("foo"));

		Filter mappedObject = queryMapper.getMappedObject(filter,
				mappingContext.getRequiredPersistentEntity(TypeWithKeyClass.class));

		assertThat(mappedObject).contains(Criteria.where("first_name").is("foo"));
	}

	@Test // DATACASS-343
	void shouldMapSortWithCompositePrimaryKeyClass() {

		Sort sort = Sort.by("key.firstname");

		Sort mappedObject = queryMapper.getMappedSort(sort,
				mappingContext.getRequiredPersistentEntity(TypeWithKeyClass.class));

		assertThat(mappedObject).contains(new Order(Direction.ASC, "first_name"));
	}

	@Test // DATACASS-828
	void allowSortByCompositeKey() {

		Sort sort = Sort.by("key");
		Query.empty().columns(Columns.from("key"));

		Sort mappedSort = queryMapper.getMappedSort(sort,
				mappingContext.getRequiredPersistentEntity(TypeWithKeyClass.class));
		assertThat(mappedSort).isEqualTo(Sort.by(asc("first_name"), asc("lastname")));
	}

	@Test // DATACASS-343
	void shouldMapColumnWithCompositePrimaryKeyClass() {

		Columns columnNames = Columns.from("key.firstname");

		List<CqlIdentifier> mappedObject = queryMapper.getMappedColumnNames(columnNames,
				mappingContext.getRequiredPersistentEntity(TypeWithKeyClass.class));

		assertThat(mappedObject).contains(CqlIdentifier.fromCql("first_name"));
	}

	@Test // DATACASS-523
	@SuppressWarnings("all")
	void shouldMapTuple() {

		MappedTuple tuple = new MappedTuple("foo");

		Filter filter = Filter.from(Criteria.where("tuple").is(tuple));

		Filter mappedObject = this.queryMapper.getMappedObject(filter, this.personPersistentEntity);

		TupleValue tupleValue = DataTypes.tupleOf(DataTypes.TEXT).newValue();

		tupleValue.setString(0, "foo");

		assertThat(mappedObject).contains(Criteria.where("tuple").is(tupleValue));
	}

	@Test // DATACASS-302
	// NOTE: DataStax Cassandra OSS Java Driver is already capable of handling Java Time types since 4.x. (at least).
	// see https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/type/codec/TypeCodecs.html
	// Therefore java.time.LocalTime on an entity property type maps to java.time.LocalTime in a CQL query.
	void shouldMapTime() {

		LocalTime time = LocalTime.ofNanoOfDay(1000);

		Filter filter = Filter.from(Criteria.where("localTime").gt(time));

		Filter mappedFilter = this.queryMapper.getMappedObject(filter, this.personPersistentEntity);

		assertThat(mappedFilter).contains(Criteria.where("localtime").gt(time));
	}

	@Test // DATACASS-523
	void referencingTupleElementsInQueryShouldFail() {
		assertThatIllegalArgumentException().isThrownBy(() -> this.queryMapper
				.getMappedObject(Filter.from(Criteria.where("tuple.zip").is("123")), this.personPersistentEntity));
	}

	@Test // DATACASS-167
	void shouldMapEmbeddedType() {

		Filter filter = Filter.from(Criteria.where("nested.firstname").is("spring"));

		Filter mappedObject = this.queryMapper.getMappedObject(filter,
				this.mappingContext.getRequiredPersistentEntity(WithNullableEmbeddedType.class));

		assertThat(mappedObject.iterator().next().getColumnName()).isEqualTo(ColumnName.from("firstname"));
	}

	@Test // DATACASS-167
	void shouldMapPrefixedEmbeddedType() {

		Filter filter = Filter.from(Criteria.where("nested.firstname").is("spring"));

		Filter mappedObject = this.queryMapper.getMappedObject(filter,
				this.mappingContext.getRequiredPersistentEntity(WithPrefixedNullableEmbeddedType.class));

		assertThat(mappedObject.iterator().next().getColumnName()).isEqualTo(ColumnName.from("prefixfirstname"));
	}

	@Test // GH-1383
	void shouldConvertPrimaryKeyValues() {

		Filter filter = Filter.from(Criteria.where("id.foo").is(42));

		Filter mappedObject = this.queryMapper.getMappedObject(filter,
				this.mappingContext.getRequiredPersistentEntity(WithPrimaryKeyClass.class));

		assertThat(mappedObject.iterator().next().getColumnName()).isEqualTo(ColumnName.from("foo"));
		assertThat(mappedObject.iterator().next().getPredicate().getValue()).isEqualTo(42L);
	}

	@Test //
	void shouldConvertVectorValues() {

		Filter filter = Filter.from(Criteria.where("array").is(new float[] { 1.1f, 2.2f }));

		Filter mappedObject = this.queryMapper.getMappedObject(filter,
				this.mappingContext.getRequiredPersistentEntity(WithVector.class));

		assertThat(mappedObject.iterator().next().getColumnName()).isEqualTo(ColumnName.from("array"));
		assertThat(mappedObject.iterator().next().getPredicate().getValue()).isEqualTo(new float[] { 1.1f, 2.2f });
	}

	@Test // GH-1504
	void shouldConvertVectorValuesFromList() {

		Filter filter = Filter.from(Criteria.where("list").is(Arrays.asList(1.1f, 2.2f)));

		Filter mappedObject = this.queryMapper.getMappedObject(filter,
				this.mappingContext.getRequiredPersistentEntity(WithVector.class));

		assertThat(mappedObject.iterator().next().getColumnName()).isEqualTo(ColumnName.from("list"));
		assertThat(mappedObject.iterator().next().getPredicate().getValue()).isEqualTo(CqlVector.newInstance(1.1f, 2.2f));
	}

	@Test // GH-1504
	void shouldConvertVectorSelectorFunction() {

		Columns columns = Columns.empty();
		Columns.FunctionCall similarity = Columns.FunctionCall.from("similarity_cosine", CqlIdentifier.fromCql("array"), Arrays.asList(1.1f, 2.2f));

		Query query = Query.empty().columns(columns.select("array", similarity));

		StatementFactory factory = new StatementFactory(queryMapper, new UpdateMapper(cassandraConverter));

		SimpleStatement statement = factory.select(query, this.mappingContext.getRequiredPersistentEntity(WithVector.class))
				.build(StatementBuilder.ParameterHandling.BY_NAME);

		assertThat(statement.getQuery()).isEqualTo("SELECT similarity_cosine(array,[1.1, 2.2]) FROM withvector");
		assertThat(statement.getNamedValues()).isEmpty();
	}

	@SuppressWarnings("unused")
	static class Person {

		@Id String id;

		Address address;
		List<Address> addresses;
		Map<Address, Address> relocations;
		Currency currency;
		State state;

		Integer number;

		LocalDate localDate;
		LocalTime localTime;

		MappedTuple tuple;

		@Column("first_name") String firstName;

		@ValueConverter(ReversingValueConverter.class) String reverseName;

	}

	static class ReversingValueConverter implements PropertyValueConverter<String, String, CassandraConversionContext> {

		@Nullable
		@Override
		public String read(@Nullable String value, CassandraConversionContext context) {
			return reverse(value);
		}

		@Nullable
		@Override
		public String write(@Nullable String value, CassandraConversionContext context) {
			return reverse(value);
		}

		private String reverse(String source) {

			if (source == null) {
				return null;
			}

			return new StringBuilder(source).reverse().toString();
		}
	}

	static class WithPrimaryKeyClass {

		@Id MyPrimaryKeyClass id;

	}

	static class MyPrimaryKeyClass {

		@PrimaryKeyColumn long foo;

		@PrimaryKeyColumn long bar;
	}

	@Tuple
	record MappedTuple(@Element(0) String zip) {
	}

	@UserDefinedType
	record Address(String street) {

	}

	enum State {
		Active, Inactive
	}

	static class WithNullableEmbeddedType {

		@Id String id;

		@Embedded.Nullable EmbeddedWithSimpleTypes nested;
	}

	static class WithPrefixedNullableEmbeddedType {

		@Id String id;

		@Embedded.Nullable(prefix = "prefix") EmbeddedWithSimpleTypes nested;
	}

	static class EmbeddedWithSimpleTypes {

		String firstname;
		Integer age;

		public String getFirstname() {
			return this.firstname;
		}

		public Integer getAge() {
			return this.age;
		}

		public void setFirstname(String firstname) {
			this.firstname = firstname;
		}

		public void setAge(Integer age) {
			this.age = age;
		}
	}

	static class WithVector {

		@VectorType(dimensions = 12) float[] array;
		@VectorType(dimensions = 12) List<Float> list;
	}
}
