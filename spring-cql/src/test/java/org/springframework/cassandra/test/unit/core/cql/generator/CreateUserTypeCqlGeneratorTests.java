/*
 * Copyright 2013-2014 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.cassandra.test.unit.core.cql.generator;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;
import static org.springframework.cassandra.core.cql.CqlIdentifier.cqlId;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;
import org.springframework.cassandra.core.ReservedKeyword;
import org.springframework.cassandra.core.cql.CqlIdentifier;
import org.springframework.cassandra.core.cql.generator.CreateUserTypeCqlGenerator;
import org.springframework.cassandra.core.keyspace.CreateUserTypeSpecification;

import com.datastax.driver.core.DataType;

public class CreateUserTypeCqlGeneratorTests {
	
	public static class RegexMatcher extends TypeSafeMatcher<String> {

	    private final String regex;

	    public RegexMatcher(final String regex) {
	        this.regex = regex;
	    }

	    @Override
	    public void describeTo(final Description description) {
	        description.appendText("matches regex=`" + regex + "`");
	    }

	    @Override
	    public boolean matchesSafely(final String string) {
	        return string.matches(regex);
	    }


	    public static RegexMatcher matchesRegex(final String regex) {
	        return new RegexMatcher(regex);
	    }
	}

	/**
	 * Asserts that the preamble is first & correctly formatted in the given CQL string.
	 */
	public static void assertPreamble(CqlIdentifier typeName, String cql) {
		assertThat("Assert query sintax", cql, 
				RegexMatcher.matchesRegex("CREATE TYPE \"?\\w+\"? \\((\\w+ \\w+, )*\\w+ \\w+\\);"));
		assertThat(cql, startsWith("CREATE TYPE " + typeName + " "));
	}

	/**
	 * Asserts that the given list of columns definitions are contained in the given CQL string properly.
	 * 
	 * @param columnSpec IE, "foo text, bar blob"
	 */
	public static void assertColumns(String columnSpec, String cql) {
		assertThat(cql, containsString("(" + columnSpec + ");"));
	}

	/**
	 * Convenient base class that other test classes can use so as not to repeat the generics declarations or
	 * {@link #generator()} method.
	 */
	public static abstract class CreateUserTypeTest extends
			UserTypeOperationCqlGeneratorTest<CreateUserTypeSpecification, CreateUserTypeCqlGenerator> {

		@Override
		public CreateUserTypeCqlGenerator generator() {
			return new CreateUserTypeCqlGenerator(specification);
		}
	}

	public static class BasicTest extends CreateUserTypeTest {

		public CqlIdentifier name = cqlId("mytype");
		public DataType columnType1 = DataType.text();
		public String column1 = "column1";
		public DataType columnType2 = DataType.cint();
		public String column2 = "column2";

		@Override
		public CreateUserTypeSpecification specification() {
			return CreateUserTypeSpecification.createType().name(name)
					.column(column1, columnType1)
					.column(column2, columnType2);
		}

		@Test
		public void test() {
			prepare();

			assertPreamble(name, cql);
			assertColumns(String.format("%s %s, %s %s", column1, columnType1, column2, columnType2), cql);
		}
	}

	public static class FunkyUserTypeNameTest {

		public static final List<String> FUNKY_LEGAL_NAMES;

		static {
			List<String> funkies = new ArrayList<String>(Arrays.asList(new String[] { /* TODO */}));
			// TODO: should these work? "a \"\" x", "a\"\"\"\"x", "a b"
			for (ReservedKeyword funky : ReservedKeyword.values()) {
				funkies.add(funky.name());
			}
			FUNKY_LEGAL_NAMES = Collections.unmodifiableList(funkies);
		}

		@Test
		public void test() {
			for (String name : FUNKY_LEGAL_NAMES) {
				new TableNameTest(name).test();
			}
		}
	}

	/**
	 * This class is supposed to be used by other test classes.
	 */
	public static class TableNameTest extends CreateUserTypeTest {

		public String typeName;

		public TableNameTest(String typeName) {
			this.typeName = typeName;
		}

		@Override
		public CreateUserTypeSpecification specification() {
			return CreateUserTypeSpecification.createType().name(typeName).column(cqlId("value"), DataType.text());
		}

		/**
		 * There is no @Test annotation on this method on purpose! It's supposed to be called by another test class's @Test
		 * method so that you can loop, calling this test method as many times as are necessary.
		 */
		public void test() {
			prepare();
			assertPreamble(cqlId(typeName), cql);
		}
	}
}
