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

package org.springframework.data.cassandra.convert;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assume.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.cassandra.core.PrimaryKeyType;
import org.springframework.core.SpringVersion;
import org.springframework.core.convert.ConverterNotFoundException;
import org.springframework.data.cassandra.mapping.CassandraType;
import org.springframework.data.cassandra.mapping.PrimaryKey;
import org.springframework.data.cassandra.mapping.PrimaryKeyClass;
import org.springframework.data.cassandra.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.mapping.Table;
import org.springframework.test.util.ReflectionTestUtils;

import com.datastax.driver.core.DataType.Name;
import com.datastax.driver.core.querybuilder.Assignment;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Delete.Where;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;
import com.datastax.driver.core.querybuilder.Update.Assignments;

/**
 * Unit tests for {@link MappingCassandraConverter}.
 *
 * @author Mark Paluch
 * @soundtrack Outlandich - Dont Leave Me Feat Cyt (Sun Kidz Electrocore Mix)
 */
public class MappingCassandraConverterUnitTests {

	@Rule public final ExpectedException expectedException = ExpectedException.none();

	private MappingCassandraConverter mappingCassandraConverter = new MappingCassandraConverter();

	/**
	 * @see DATACASS-260
	 */
	@Test
	public void insertEnumShouldMapToString() {

		WithEnumColumns withEnumColumns = new WithEnumColumns();
		withEnumColumns.setCondition(Condition.MINT);

		Insert insert = QueryBuilder.insertInto("table");

		mappingCassandraConverter.write(withEnumColumns, insert);

		assertThat(getValues(insert), contains((Object) "MINT"));
	}

	/**
	 * @see DATACASS-260
	 */
	@Test
	public void insertEnumDoesNotMapToOrdinalBeforeSpring43() {

		assumeThat(SpringVersion.getVersion(), not(startsWith("4.3")));

		expectedException.expect(ConverterNotFoundException.class);
		expectedException.expectMessage(allOf(containsString("No converter found"), containsString("java.lang.Integer")));

		UnsupportedEnumToOrdinalMapping unsupportedEnumToOrdinalMapping = new UnsupportedEnumToOrdinalMapping();
		unsupportedEnumToOrdinalMapping.setAsOrdinal(Condition.MINT);

		Insert insert = QueryBuilder.insertInto("table");

		mappingCassandraConverter.write(unsupportedEnumToOrdinalMapping, insert);
	}

	/**
	 * @see DATACASS-255
	 */
	@Test
	public void insertEnumMapsToOrdinalWithSpring43() {

		assumeThat(SpringVersion.getVersion(), startsWith("4.3"));

		UnsupportedEnumToOrdinalMapping unsupportedEnumToOrdinalMapping = new UnsupportedEnumToOrdinalMapping();
		unsupportedEnumToOrdinalMapping.setAsOrdinal(Condition.USED);

		Insert insert = QueryBuilder.insertInto("table");

		mappingCassandraConverter.write(unsupportedEnumToOrdinalMapping, insert);

		assertThat(getValues(insert), contains((Object) Integer.valueOf(Condition.USED.ordinal())));
	}

	/**
	 * @see DATACASS-260
	 */
	@Test
	public void insertEnumAsPrimaryKeyShouldMapToString() {

		EnumPrimaryKey key = new EnumPrimaryKey();
		key.setCondition(Condition.MINT);

		Insert insert = QueryBuilder.insertInto("table");

		mappingCassandraConverter.write(key, insert);

		assertThat(getValues(insert), contains((Object) "MINT"));
	}

	/**
	 * @see DATACASS-260
	 */
	@Test
	public void insertEnumInCompositePrimaryKeyShouldMapToString() {

		EnumCompositePrimaryKey key = new EnumCompositePrimaryKey();
		key.setCondition(Condition.MINT);

		CompositeKeyThing composite = new CompositeKeyThing();
		composite.setKey(key);

		Insert insert = QueryBuilder.insertInto("table");

		mappingCassandraConverter.write(composite, insert);

		assertThat(getValues(insert), contains((Object) "MINT"));
	}

	/**
	 * @see DATACASS-260
	 */
	@Test
	public void updateEnumShouldMapToString() {

		WithEnumColumns withEnumColumns = new WithEnumColumns();
		withEnumColumns.setCondition(Condition.MINT);

		Update update = QueryBuilder.update("table");

		mappingCassandraConverter.write(withEnumColumns, update);

		assertThat(getAssignmentValues(update), contains((Object) "MINT"));
	}

	/**
	 * @see DATACASS-260
	 */
	@Test
	public void updateEnumAsPrimaryKeyShouldMapToString() {

		EnumPrimaryKey key = new EnumPrimaryKey();
		key.setCondition(Condition.MINT);

		Update update = QueryBuilder.update("table");

		mappingCassandraConverter.write(key, update);

		assertThat(getWhereValues(update), contains((Object) "MINT"));
	}

	/**
	 * @see DATACASS-260
	 */
	@Test
	public void updateEnumInCompositePrimaryKeyShouldMapToString() {

		EnumCompositePrimaryKey key = new EnumCompositePrimaryKey();
		key.setCondition(Condition.MINT);

		CompositeKeyThing composite = new CompositeKeyThing();
		composite.setKey(key);

		Update update = QueryBuilder.update("table");

		mappingCassandraConverter.write(composite, update);

		assertThat(getWhereValues(update), contains((Object) "MINT"));
	}

	/**
	 * @see DATACASS-260
	 */
	@Test
	public void whereEnumAsPrimaryKeyShouldMapToString() {

		EnumPrimaryKey key = new EnumPrimaryKey();
		key.setCondition(Condition.MINT);

		Where where = QueryBuilder.delete().from("table").where();

		mappingCassandraConverter.write(key, where);

		assertThat(getWhereValues(where), contains((Object) "MINT"));
	}

	/**
	 * @see DATACASS-260
	 */
	@Test
	public void whereEnumInCompositePrimaryKeyShouldMapToString() {

		EnumCompositePrimaryKey key = new EnumCompositePrimaryKey();
		key.setCondition(Condition.MINT);

		CompositeKeyThing composite = new CompositeKeyThing();
		composite.setKey(key);

		Where where = QueryBuilder.delete().from("table").where();

		mappingCassandraConverter.write(composite, where);

		assertThat(getWhereValues(where), contains((Object) "MINT"));
	}

	@SuppressWarnings("unchecked")
	private List<Object> getValues(Insert statement) {
		return (List<Object>) ReflectionTestUtils.getField(statement, "values");
	}

	@SuppressWarnings("unchecked")
	private List<Object> getAssignmentValues(Update statement) {

		List<Object> result = new ArrayList<Object>();

		Assignments assignments = (Assignments) ReflectionTestUtils.getField(statement, "assignments");
		List<Assignment> listOfAssignments = (List<Assignment>) ReflectionTestUtils.getField(assignments, "assignments");
		for (Assignment assignment : listOfAssignments) {
			result.add(ReflectionTestUtils.getField(assignment, "value"));
		}

		return result;
	}

	private List<Object> getWhereValues(Update statement) {
		return getWhereValues(statement.where());
	}

	private List<Object> getWhereValues(BuiltStatement where) {

		List<Object> result = new ArrayList<Object>();

		List<Clause> clauses = (List<Clause>) ReflectionTestUtils.getField(where, "clauses");
		for (Clause clause : clauses) {
			result.add(ReflectionTestUtils.getField(clause, "value"));
		}

		return result;
	}

	@Table
	public static class UnsupportedEnumToOrdinalMapping {

		@PrimaryKey private String id;

		@CassandraType(type = Name.INT) private Condition asOrdinal;

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public Condition getAsOrdinal() {
			return asOrdinal;
		}

		public void setAsOrdinal(Condition asOrdinal) {
			this.asOrdinal = asOrdinal;
		}

	}

	@Table
	public static class WithEnumColumns {

		@PrimaryKey private String id;

		private Condition condition;

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public Condition getCondition() {
			return condition;
		}

		public void setCondition(Condition condition) {
			this.condition = condition;
		}

	}

	@PrimaryKeyClass
	public static class EnumCompositePrimaryKey implements Serializable {

		@PrimaryKeyColumn(ordinal = 1, type = PrimaryKeyType.PARTITIONED) private Condition condition;

		public EnumCompositePrimaryKey() {}

		public EnumCompositePrimaryKey(Condition condition) {
			this.condition = condition;
		}

		public Condition getCondition() {
			return condition;
		}

		public void setCondition(Condition condition) {
			this.condition = condition;
		}

	}

	@Table
	public static class EnumPrimaryKey {

		@PrimaryKey private Condition condition;

		public Condition getCondition() {
			return condition;
		}

		public void setCondition(Condition condition) {
			this.condition = condition;
		}

	}

	@Table
	public static class CompositeKeyThing {

		@PrimaryKey private EnumCompositePrimaryKey key;

		public CompositeKeyThing() {}

		public CompositeKeyThing(EnumCompositePrimaryKey key) {
			this.key = key;
		}

		public EnumCompositePrimaryKey getKey() {
			return key;
		}

		public void setKey(EnumCompositePrimaryKey key) {
			this.key = key;
		}

	}

	public static enum Condition {
		MINT, USED;
	}

}
