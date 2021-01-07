/*
 * Copyright 2019-2021 the original author or authors.
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
package org.springframework.data.cassandra.core.cql.session.init;

import static org.assertj.core.api.Assertions.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.support.EncodedResource;

/**
 * Unit tests for {@link ScriptUtils}.
 *
 * @author Mark Paluch
 */
class ScriptUtilsUnitTests {

	@Test // DATACASS-704
	void splitCqlScriptDelimitedWithSemicolon() {

		String rawStatement1 = "insert into customer (id, name)\nvalues (1, 'Walter ; White'), (2, 'Hank \n Schrader')";
		String cleanedStatement1 = "insert into customer (id, name) values (1, 'Walter ; White'), (2, 'Hank \n Schrader')";
		String rawStatement2 = "insert into orders(id, order_date, customer_id)\nvalues (1, '2008-01-02', 2)";
		String cleanedStatement2 = "insert into orders(id, order_date, customer_id) values (1, '2008-01-02', 2)";
		String rawStatement3 = "insert into orders(id, order_date, customer_id) values (1, '2008-01-02', 2)";
		String cleanedStatement3 = "insert into orders(id, order_date, customer_id) values (1, '2008-01-02', 2)";

		char delim = ';';

		String script = rawStatement1 + delim + rawStatement2 + delim + rawStatement3 + delim;
		List<String> statements = new ArrayList<>();
		ScriptUtils.splitCqlScript(script, delim, statements);

		assertThat(statements).containsExactly(cleanedStatement1, cleanedStatement2, cleanedStatement3);
	}

	@Test // DATACASS-704
	void splitCqlScriptDelimitedWithNewLine() {

		String statement1 = "insert into customer (id, name) values (1, 'Walter ; White'), (2, 'Hank \n Schrader')";
		String statement2 = "insert into orders(id, order_date, customer_id) values (1, '2008-01-02', 2)";
		String statement3 = "insert into orders(id, order_date, customer_id) values (1, '2008-01-02', 2)";

		char delim = '\n';

		String script = statement1 + delim + statement2 + delim + statement3 + delim;
		List<String> statements = new ArrayList<>();
		ScriptUtils.splitCqlScript(script, delim, statements);

		assertThat(statements).containsExactly(statement1, statement2, statement3);
	}

	@Test // DATACASS-704
	void splitCqlScriptDelimitedWithNewLineButDefaultDelimiterSpecified() {

		String statement1 = "do something";
		String statement2 = "do something else";

		char delim = '\n';

		String script = statement1 + delim + statement2 + delim;
		List<String> statements = new ArrayList<>();
		ScriptUtils.splitCqlScript(script, ScriptUtils.DEFAULT_STATEMENT_SEPARATOR, statements);

		assertThat(statements).as("stripped but not split statements").containsExactly(script.replace('\n', ' '));
	}

	@Test // DATACASS-704
	void splitScriptWithSingleQuotesNestedInsideDoubleQuotes() {

		String statement1 = "select '1' as \"Hank's owner's\" from dual";
		String statement2 = "select '2' as \"Hank's\" from dual";
		char delim = ';';
		String script = statement1 + delim + statement2 + delim;
		List<String> statements = new ArrayList<>();

		ScriptUtils.splitCqlScript(script, ';', statements);

		assertThat(statements).containsExactly(statement1, statement2);
	}

	@Test // DATACASS-704
	void readAndSplitScriptWithMultipleNewlinesAsSeparator() throws IOException {

		String script = readScript("db-test-data-multi-newline.cql");
		List<String> statements = new ArrayList<>();

		ScriptUtils.splitCqlScript(script, "\n\n", statements);

		String statement1 = "insert into T_TEST (NAME) values ('Hank')";
		String statement2 = "insert into T_TEST (NAME) values ('Walter')";

		assertThat(statements).containsExactly(statement1, statement2);
	}

	@Test // DATACASS-704
	void readAndSplitScriptContainingComments() throws Exception {

		String script = readScript("test-data-with-comments.cql");

		splitScriptContainingComments(script, ScriptUtils.DEFAULT_COMMENT_PREFIXES);
	}

	@Test // DATACASS-704
	void readAndSplitScriptContainingCommentsWithWindowsLineEnding() throws Exception {
		String script = readScript("test-data-with-comments.cql").replaceAll("\n", "\r\n");
		splitScriptContainingComments(script, ScriptUtils.DEFAULT_COMMENT_PREFIXES);
	}

	@Test // DATACASS-704
	void readAndSplitScriptContainingCommentsWithMultiplePrefixes() throws Exception {
		String script = readScript("test-data-with-multi-prefix-comments.cql");
		splitScriptContainingComments(script, "--", "#", "^");
	}

	private void splitScriptContainingComments(String script, String... commentPrefixes) {

		List<String> statements = new ArrayList<>();

		ScriptUtils.splitCqlScript(null, script, ";", commentPrefixes, ScriptUtils.DEFAULT_BLOCK_COMMENT_START_DELIMITER,
				ScriptUtils.DEFAULT_BLOCK_COMMENT_END_DELIMITER, statements);

		String statement1 = "insert into customer (id, name) values (1, 'Walter; White'), (2, 'Hank Schrader')";
		String statement2 = "insert into orders(id, order_date, customer_id) values (1, '2008-01-02', 2)";
		String statement3 = "insert into orders(id, order_date, customer_id) values (1, '2008-01-02', 2)";
		// Statement 4 addresses the error described in SPR-9982.
		String statement4 = "INSERT INTO persons( person_id , name) VALUES( 1 , 'Name' )";

		assertThat(statements).containsExactly(statement1, statement2, statement3, statement4);
	}

	@Test // DATACASS-704
	void readAndSplitScriptContainingCommentsWithLeadingTabs() throws Exception {

		String script = readScript("test-data-with-comments-and-leading-tabs.cql");
		List<String> statements = new ArrayList<>();

		ScriptUtils.splitCqlScript(script, ';', statements);

		String statement1 = "insert into customer (id, name) values (1, 'Hank Schrader')";
		String statement2 = "insert into orders(id, order_date, customer_id) values (1, '2013-06-08', 1)";
		String statement3 = "insert into orders(id, order_date, customer_id) values (2, '2013-06-08', 1)";

		assertThat(statements).containsExactly(statement1, statement2, statement3);
	}

	@Test // DATACASS-704
	void readAndSplitScriptContainingMultiLineComments() throws Exception {

		String script = readScript("test-data-with-multi-line-comments.cql");
		List<String> statements = new ArrayList<>();

		ScriptUtils.splitCqlScript(script, ';', statements);

		String statement1 = "INSERT INTO users(first_name, last_name) VALUES('Walter', 'White')";
		String statement2 = "INSERT INTO users(first_name, last_name) VALUES( 'Hank' , 'Schrader' )";

		assertThat(statements).containsExactly(statement1, statement2);
	}

	@Test // DATACASS-704
	void readAndSplitScriptContainingMultiLineNestedComments() throws Exception {

		String script = readScript("test-data-with-multi-line-nested-comments.cql");
		List<String> statements = new ArrayList<>();

		ScriptUtils.splitCqlScript(script, ';', statements);

		String statement1 = "INSERT INTO users(first_name, last_name) VALUES('Walter', 'White')";
		String statement2 = "INSERT INTO users(first_name, last_name) VALUES( 'Hank' , 'Schrader' )";

		assertThat(statements).containsExactly(statement1, statement2);
	}

	@Test // DATACASS-704
	void containsDelimiters() {

		assertThat(ScriptUtils.containsCqlScriptDelimiters("select 1\n select ';'", ";")).isFalse();
		assertThat(ScriptUtils.containsCqlScriptDelimiters("select 1; select 2", ";")).isTrue();
		assertThat(ScriptUtils.containsCqlScriptDelimiters("select 1; select '\\n\n';", "\n")).isFalse();
		assertThat(ScriptUtils.containsCqlScriptDelimiters("select 1\n select 2", "\n")).isTrue();
		assertThat(ScriptUtils.containsCqlScriptDelimiters("select 1\n select 2", "\n\n")).isFalse();
		assertThat(ScriptUtils.containsCqlScriptDelimiters("select 1\n\n select 2", "\n\n")).isTrue();
		assertThat(
				ScriptUtils.containsCqlScriptDelimiters("insert into users(first_name, last_name)\nvalues('a\\\\', 'b;')", ";"))
						.isFalse();
		assertThat(ScriptUtils.containsCqlScriptDelimiters(
				"insert into users(first_name, last_name)\nvalues('Charles', 'd\\'Artagnan'); select 1;", ";")).isTrue();
	}

	private String readScript(String path) throws IOException {
		EncodedResource resource = new EncodedResource(new ClassPathResource(path, getClass()));
		return ScriptUtils.readScript(resource);
	}
}
