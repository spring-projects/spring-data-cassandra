/*
 * Copyright 2016-2021 the original author or authors.
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
package org.springframework.data.cassandra.repository.query;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.springframework.data.cassandra.repository.query.StubParameterAccessor.*;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.StatementFactory;
import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.convert.UpdateMapper;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.cql.util.StatementBuilder;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.PrimaryKey;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyClass;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.core.mapping.Table;
import org.springframework.data.cassandra.core.mapping.UserTypeResolver;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.cassandra.domain.Person;
import org.springframework.data.domain.Range;
import org.springframework.data.repository.query.parser.PartTree;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;

/**
 * Unit tests for {@link CassandraQueryCreator}.
 *
 * @author Mark Paluch
 */
class CassandraQueryCreatorUnitTests {

	private CassandraMappingContext context;
	private CassandraConverter converter;

	@BeforeEach
	void setUp() {

		context = new CassandraMappingContext();
		context.setUserTypeResolver(mock(UserTypeResolver.class));

		converter = new MappingCassandraConverter(context);
	}

	@Test // DATACASS-7
	void createsQueryCorrectly() {

		String query = createQuery("findByFirstname", Person.class, "Walter");

		assertThat(query).isEqualTo("SELECT * FROM person WHERE firstname='Walter'");
	}

	@Test // DATACASS-7
	void createsQueryWithSortCorrectly() {

		String query = createQuery("findByFirstnameOrderByLastname", Person.class, "Walter");

		assertThat(query).isEqualTo("SELECT * FROM person WHERE firstname='Walter' ORDER BY lastname ASC");
	}

	@Test // DATACASS-7
	void createsAndQueryCorrectly() {

		String query = createQuery("findByFirstnameAndLastname", Person.class, "Walter", "White");

		assertThat(query).isEqualTo("SELECT * FROM person WHERE firstname='Walter' AND lastname='White'");
	}

	@Test // DATACASS-7
	void rejectsNegatingQuery() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
				.isThrownBy(() -> createQuery("findByFirstnameNot", Person.class, "Walter"));
	}

	@Test // DATACASS-7
	void rejectsOrQuery() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
				.isThrownBy(() -> createQuery("findByFirstnameOrLastname", Person.class, "Walter", "White"));
	}

	@Test // DATACASS-7
	void createsGreaterThanQueryCorrectly() {

		String query = createQuery("findByFirstnameGreaterThan", Person.class, "Walter");

		assertThat(query).isEqualTo("SELECT * FROM person WHERE firstname>'Walter'");
	}

	@Test // DATACASS-7
	void createsGreaterThanEqualQueryCorrectly() {

		String query = createQuery("findByFirstnameGreaterThanEqual", Person.class, "Walter");

		assertThat(query).isEqualTo("SELECT * FROM person WHERE firstname>='Walter'");
	}

	@Test // DATACASS-7
	void createsLessThanQueryCorrectly() {

		String query = createQuery("findByFirstnameLessThan", Person.class, "Walter");

		assertThat(query).isEqualTo("SELECT * FROM person WHERE firstname<'Walter'");
	}

	@Test // DATACASS-7
	void createsLessThanEqualQueryCorrectly() {

		String query = createQuery("findByFirstnameLessThanEqual", Person.class, "Walter");

		assertThat(query).isEqualTo("SELECT * FROM person WHERE firstname<='Walter'");
	}

	@Test // DATACASS-627
	void createsBetweenQueryCorrectly() {

		String query = createQuery("findByFirstnameBetween", Person.class, 1, 2);

		assertThat(query).isEqualTo("SELECT * FROM person WHERE firstname>1 AND firstname<2");
	}

	@Test // DATACASS-627
	void createsBetweenQueryWithRangeCorrectly() {

		String query = createQuery("findByFirstnameBetween", Person.class,
				Range.from(Range.Bound.inclusive(1)).to(Range.Bound.exclusive(2)));

		assertThat(query).isEqualTo("SELECT * FROM person WHERE firstname>=1 AND firstname<2");
	}

	@Test // DATACASS-7
	void createsInQueryCorrectly() {

		String query = createQuery("findByFirstnameIn", Person.class, "Walter");

		assertThat(query).isEqualTo("SELECT * FROM person WHERE firstname IN ('Walter')");
	}

	@Test // DATACASS-7
	void createsInQueryWithListCorrectly() {

		String query = createQuery("findByFirstnameIn", Person.class, Arrays.asList("Walter", "Gus"));

		assertThat(query).isEqualTo("SELECT * FROM person WHERE firstname IN ('Walter','Gus')");
	}

	@Test // DATACASS-7
	void createsInQueryWithArrayCorrectly() {

		String query = createQuery("findByFirstnameInAndLastname", Person.class, new String[] { "Walter", "Gus" }, "Fring");

		assertThat(query).isEqualTo("SELECT * FROM person WHERE firstname IN ('Walter','Gus') AND lastname='Fring'");
	}

	@Test // DATACASS-7
	void createsLikeQueryCorrectly() {

		assertThat(createQuery("findByFirstnameLike", Person.class, "Wal%ter"))
				.isEqualTo("SELECT * FROM person WHERE firstname LIKE 'Wal%ter'");

		assertThat(createQuery("findByFirstnameLike", Person.class, "Walter"))
				.isEqualTo("SELECT * FROM person WHERE firstname LIKE 'Walter'");
	}

	@Test // DATACASS-7
	void createsStartsWithQueryCorrectly() {

		String query = createQuery("findByFirstnameStartsWith", Person.class, "Walter");

		assertThat(query).isEqualTo("SELECT * FROM person WHERE firstname LIKE 'Walter%'");
	}

	@Test // DATACASS-7
	void createsEndsWithQueryCorrectly() {

		String query = createQuery("findByFirstnameEndsWith", Person.class, "Walter");

		assertThat(query).isEqualTo("SELECT * FROM person WHERE firstname LIKE '%Walter'");
	}

	@Test // DATACASS-7
	void createsContainsQueryOnSimplePropertyCorrectly() {

		String query = createQuery("findByFirstnameContains", Person.class, "Walter");

		assertThat(query).isEqualTo("SELECT * FROM person WHERE firstname LIKE '%Walter%'");
	}

	@Test // DATACASS-7
	void createsContainsQueryOnSetPropertyCorrectly() {

		String query = createQuery("findByMysetContains", TypeWithSet.class, "Walter");

		assertThat(query).isEqualTo("SELECT * FROM typewithset WHERE myset CONTAINS 'Walter'");
	}

	@Test // DATACASS-7
	void createsContainsQueryOnListPropertyCorrectly() {

		String query = createQuery("findByMylistContains", TypeWithList.class, "Walter");

		assertThat(query).isEqualTo("SELECT * FROM typewithlist WHERE mylist CONTAINS 'Walter'");
	}

	@Test // DATACASS-7
	void createsContainsQueryOnMapPropertyCorrectly() {

		String query = createQuery("findByMymapContains", TypeWithMap.class, "Walter");

		assertThat(query).isEqualTo("SELECT * FROM typewithmap WHERE mymap CONTAINS 'Walter'");
	}

	@Test // DATACASS-7
	void createsIsTrueQueryCorrectly() {

		String query = createQuery("findByFirstnameIsTrue", Person.class, "Walter");

		assertThat(query).isEqualTo("SELECT * FROM person WHERE firstname=true");
	}

	@Test // DATACASS-7
	void createsIsFalseQueryCorrectly() {

		String query = createQuery("findByFirstnameIsFalse", Person.class, "Walter");

		assertThat(query).isEqualTo("SELECT * FROM person WHERE firstname=false");
	}

	@Test // DATACASS-7
	void createsQueryUsingQuotingCorrectly() {

		String query = createQuery("findByIdAndSet", QuotedType.class, "Walter", "White");

		assertThat(query).isEqualTo("SELECT * FROM \"myTable\" WHERE my_id='Walter' AND \"set\"='White'");
	}

	@Test // DATACASS-7
	void createsFindByPrimaryKeyPartCorrectly() {

		String query = createQuery("findByKeyFirstname", TypeWithCompositeId.class, "Walter");

		assertThat(query).isEqualTo("SELECT * FROM typewithcompositeid WHERE firstname='Walter'");
	}

	@Test // DATACASS-7
	void createsFindByPrimaryKeyPartWithSortCorrectly() {

		String query = createQuery("findByKeyFirstnameOrderByKeyLastnameAsc", TypeWithCompositeId.class, "Walter");

		assertThat(query).isEqualTo("SELECT * FROM typewithcompositeid WHERE firstname='Walter' ORDER BY lastname ASC");
	}

	@Test // DATACASS-7
	void createsFindByPrimaryKeyPartOfPrimaryKeyClassCorrectly() {

		String query = createQuery("findByFirstname", Key.class, "Walter");

		// ⊙_ʘ rly? ヾ( •́д•̀ ;)ﾉ
		assertThat(query).isEqualTo("SELECT * FROM key WHERE firstname='Walter'");
	}

	@Test // DATACASS-7
	void createsFindByPrimaryKey2PartCorrectly() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> createQuery("findByKey", TypeWithCompositeId.class, new Key()));
	}

	private String createQuery(String source, Class<?> entityClass, Object... values) {

		PartTree tree = new PartTree(source, entityClass);
		CassandraQueryCreator creator = new CassandraQueryCreator(tree, getAccessor(converter, values), context);

		StatementFactory factory = new StatementFactory(new UpdateMapper(converter));
		Query query = creator.createQuery();

		SimpleStatement statement = factory.select(query, context.getRequiredPersistentEntity(entityClass))
				.build(StatementBuilder.ParameterHandling.INLINE, CodecRegistry.DEFAULT);
		return statement.getQuery();
	}

	@Table
	private static class TypeWithSet {

		@Id String id;
		Set<String> myset;
	}

	@Table
	private static class TypeWithList {

		@Id String id;
		List<String> mylist;
	}

	@Table
	private static class TypeWithMap {

		@Id String id;
		Map<String, String> mymap;
	}

	@Table(value = "myTable", forceQuote = true)
	private static class QuotedType {

		@PrimaryKey(value = "my_id", forceQuote = true) String id;

		@Column(value = "set") Set<String> set;
	}

	@PrimaryKeyClass
	private static class Key implements Serializable {

		@PrimaryKeyColumn(type = PrimaryKeyType.PARTITIONED, ordinal = 1) String firstname;

		@PrimaryKeyColumn(type = PrimaryKeyType.PARTITIONED, ordinal = 1) String lastname;
	}

	@Table
	private static class TypeWithCompositeId {

		@PrimaryKey Key key;

		String city;
	}
}
