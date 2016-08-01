/*
 * Copyright 2016 the original author or authors.
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
package org.springframework.data.cassandra.repository.query;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.cassandra.repository.query.StubParameterAccessor.*;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.cassandra.core.PrimaryKeyType;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.domain.Person;
import org.springframework.data.cassandra.mapping.BasicCassandraMappingContext;
import org.springframework.data.cassandra.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.mapping.Column;
import org.springframework.data.cassandra.mapping.PrimaryKey;
import org.springframework.data.cassandra.mapping.PrimaryKeyClass;
import org.springframework.data.cassandra.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.mapping.Table;
import org.springframework.data.repository.core.EntityMetadata;
import org.springframework.data.repository.query.parser.PartTree;

/**
 * Unit tests for {@link CassandraQueryCreator}.
 *
 * @author Mark Paluch
 */
public class CassandraQueryCreatorUnitTests {

	CassandraMappingContext context;
	CassandraConverter converter;

	@Rule public ExpectedException exception = ExpectedException.none();

	@Before
	public void setUp() throws SecurityException, NoSuchMethodException {
		context = new BasicCassandraMappingContext();
		converter = new MappingCassandraConverter(context);
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-7">DATACASS-7</a>
	 */
	@Test
	public void createsQueryCorrectly() {

		String query = createQuery("findByFirstname", Person.class, "Walter");

		assertThat(query).isEqualTo("SELECT * FROM person WHERE firstname='Walter';");
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-7">DATACASS-7</a>
	 */
	@Test
	public void createsQueryWithSortCorrectly() {

		String query = createQuery("findByFirstnameOrderByLastname", Person.class, "Walter");

		assertThat(query).isEqualTo("SELECT * FROM person WHERE firstname='Walter' ORDER BY lastname ASC;");
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-7">DATACASS-7</a>
	 */
	@Test
	public void createsAndQueryCorrectly() {

		String query = createQuery("findByFirstnameAndLastname", Person.class, "Walter", "White");

		assertThat(query).isEqualTo("SELECT * FROM person WHERE firstname='Walter' AND lastname='White';");
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-7">DATACASS-7</a>
	 */
	@Test(expected = InvalidDataAccessApiUsageException.class)
	public void rejectsNegatingQuery() {
		createQuery("findByFirstnameNot", Person.class, "Walter");
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-7">DATACASS-7</a>
	 */
	@Test(expected = InvalidDataAccessApiUsageException.class)
	public void rejectsOrQuery() {
		createQuery("findByFirstnameOrLastname", Person.class, "Walter", "White");
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-7">DATACASS-7</a>
	 */
	@Test
	public void createsGreaterThanQueryCorrectly() {

		String query = createQuery("findByFirstnameGreaterThan", Person.class, "Walter");

		assertThat(query).isEqualTo("SELECT * FROM person WHERE firstname>'Walter';");
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-7">DATACASS-7</a>
	 */
	@Test
	public void createsGreaterThanEqualQueryCorrectly() {

		String query = createQuery("findByFirstnameGreaterThanEqual", Person.class, "Walter");

		assertThat(query).isEqualTo("SELECT * FROM person WHERE firstname>='Walter';");
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-7">DATACASS-7</a>
	 */
	@Test
	public void createsLessThanQueryCorrectly() {

		String query = createQuery("findByFirstnameLessThan", Person.class, "Walter");

		assertThat(query).isEqualTo("SELECT * FROM person WHERE firstname<'Walter';");
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-7">DATACASS-7</a>
	 */
	@Test
	public void createsLessThanEqualQueryCorrectly() {

		String query = createQuery("findByFirstnameLessThanEqual", Person.class, "Walter");

		assertThat(query).isEqualTo("SELECT * FROM person WHERE firstname<='Walter';");
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-7">DATACASS-7</a>
	 */
	@Test
	public void createsInQueryCorrectly() {

		String query = createQuery("findByFirstnameIn", Person.class, "Walter");

		assertThat(query).isEqualTo("SELECT * FROM person WHERE firstname IN ('Walter');");
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-7">DATACASS-7</a>
	 */
	@Test
	public void createsInQueryWithListCorrectly() {

		String query = createQuery("findByFirstnameIn", Person.class, Arrays.asList("Walter", "Gus"));

		assertThat(query).isEqualTo("SELECT * FROM person WHERE firstname IN ('Walter','Gus');");
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-7">DATACASS-7</a>
	 */
	@Test
	public void createsInQueryWithArrayCorrectly() {

		String query = createQuery("findByFirstnameInAndLastname", Person.class, new String[] { "Walter", "Gus" }, "Fring");

		assertThat(query).isEqualTo("SELECT * FROM person WHERE firstname IN ('Walter','Gus') AND lastname='Fring';");
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-7">DATACASS-7</a>
	 */
	@Test
	public void createsLikeQueryCorrectly() {

		assertThat(createQuery("findByFirstnameLike", Person.class, "Wal%ter"))
				.isEqualTo("SELECT * FROM person WHERE firstname LIKE 'Wal%ter';");

		assertThat(createQuery("findByFirstnameLike", Person.class, "Walter"))
				.isEqualTo("SELECT * FROM person WHERE firstname LIKE 'Walter';");
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-7">DATACASS-7</a>
	 */
	@Test
	public void createsStartsWithQueryCorrectly() {

		String query = createQuery("findByFirstnameStartsWith", Person.class, "Walter");

		assertThat(query).isEqualTo("SELECT * FROM person WHERE firstname LIKE 'Walter%';");
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-7">DATACASS-7</a>
	 */
	@Test
	public void createsEndsWithQueryCorrectly() {

		String query = createQuery("findByFirstnameEndsWith", Person.class, "Walter");

		assertThat(query).isEqualTo("SELECT * FROM person WHERE firstname LIKE '%Walter';");
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-7">DATACASS-7</a>
	 */
	@Test
	public void createsContainsQueryOnSimplePropertyCorrectly() {

		String query = createQuery("findByFirstnameContains", Person.class, "Walter");

		assertThat(query).isEqualTo("SELECT * FROM person WHERE firstname LIKE '%Walter%';");
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-7">DATACASS-7</a>
	 */
	@Test
	public void createsContainsQueryOnSetPropertyCorrectly() {

		String query = createQuery("findByMysetContains", TypeWithSet.class, "Walter");

		assertThat(query).isEqualTo("SELECT * FROM typewithset WHERE myset CONTAINS 'Walter';");
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-7">DATACASS-7</a>
	 */
	@Test
	public void createsContainsQueryOnListPropertyCorrectly() {

		String query = createQuery("findByMylistContains", TypeWithList.class, "Walter");

		assertThat(query).isEqualTo("SELECT * FROM typewithlist WHERE mylist CONTAINS 'Walter';");
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-7">DATACASS-7</a>
	 */
	@Test
	public void createsContainsQueryOnMapPropertyCorrectly() {

		String query = createQuery("findByMymapContains", TypeWithMap.class, "Walter");

		assertThat(query).isEqualTo("SELECT * FROM typewithmap WHERE mymap CONTAINS 'Walter';");
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-7">DATACASS-7</a>
	 */
	@Test
	public void createsIsTrueQueryCorrectly() {

		String query = createQuery("findByFirstnameIsTrue", Person.class, "Walter");

		assertThat(query).isEqualTo("SELECT * FROM person WHERE firstname=true;");
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-7">DATACASS-7</a>
	 */
	@Test
	public void createsIsFalseQueryCorrectly() {

		String query = createQuery("findByFirstnameIsFalse", Person.class, "Walter");

		assertThat(query).isEqualTo("SELECT * FROM person WHERE firstname=false;");
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-7">DATACASS-7</a>
	 */
	@Test
	public void createsQueryUsingQuotingCorrectly() {

		String query = createQuery("findByIdAndSet", QuotedType.class, "Walter", "White");

		assertThat(query).isEqualTo("SELECT * FROM \"myTable\" WHERE \"my_id\"='Walter' AND \"set\"='White';");
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-7">DATACASS-7</a>
	 */
	@Test
	public void createsFindByPrimaryKeyPartCorrectly() {

		String query = createQuery("findByKeyFirstname", TypeWithCompositeId.class, "Walter");

		assertThat(query).isEqualTo("SELECT * FROM typewithcompositeid WHERE firstname='Walter';");
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-7">DATACASS-7</a>
	 */
	@Test
	public void createsFindByPrimaryKeyPartWithSortCorrectly() {

		String query = createQuery("findByKeyFirstnameOrderByKeyLastnameAsc", TypeWithCompositeId.class, "Walter");

		assertThat(query).isEqualTo("SELECT * FROM typewithcompositeid WHERE firstname='Walter' ORDER BY lastname ASC;");
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-7">DATACASS-7</a>
	 */
	@Test
	public void createsFindByPrimaryKeyPartOfPrimaryKeyClassCorrectly() {

		String query = createQuery("findByFirstname", Key.class, "Walter");

		// ⊙_ʘ rly? ヾ( •́д•̀ ;)ﾉ
		assertThat(query).isEqualTo("SELECT * FROM key WHERE firstname='Walter';");
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-7">DATACASS-7</a>
	 */
	@Test(expected = IllegalStateException.class)
	public void createsFindByPrimaryKey2PartCorrectly() {
		createQuery("findByKey", TypeWithCompositeId.class, new Key());
	}

	private String createQuery(String source, Class<?> entityClass, Object... values) {

		PartTree tree = new PartTree(source, entityClass);
		CassandraQueryCreator creator = new CassandraQueryCreator(tree, getAccessor(converter, values), context,
				getEntityInformation(entityClass));
		return creator.createQuery().toString();
	}

	private <T> EntityMetadata<T> getEntityInformation(final Class<T> entityClass) {
		return new EntityMetadata<T>() {
			@Override
			public Class<T> getJavaType() {
				return entityClass;
			}
		};
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
