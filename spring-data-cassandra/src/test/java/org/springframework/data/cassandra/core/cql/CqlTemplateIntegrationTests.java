/*
 * Copyright 2016-2020 the original author or authors.
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
package org.springframework.data.cassandra.core.cql;

import static org.assertj.core.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Before;
import org.junit.Test;

import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTest;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;

/**
 * Integration tests for {@link CqlTemplate}.
 *
 * @author Mark Paluch
 */
public class CqlTemplateIntegrationTests extends AbstractKeyspaceCreatingIntegrationTest {

	static final AtomicBoolean initialized = new AtomicBoolean();
	CqlTemplate template;

	@Before
	public void before() {

		if (initialized.compareAndSet(false, true)) {
			session.execute("CREATE TABLE IF NOT EXISTS user (id text PRIMARY KEY, username text);");
		}

		session.execute("TRUNCATE user;");
		session.execute("INSERT INTO user (id, username) VALUES ('WHITE', 'Walter');");

		template = new CqlTemplate();
		template.setSession(getSession());
	}

	@Test // DATACASS-292
	public void executeShouldRemoveRecords() {

		template.execute("DELETE FROM user WHERE id = 'WHITE'");

		assertThat(session.execute("SELECT * FROM user").one()).isNull();
	}

	@Test // DATACASS-292
	public void queryShouldInvokeCallback() {

		List<String> result = new ArrayList<>();
		template.query("SELECT id FROM user;", row -> {
			result.add(row.getString(0));
		});

		assertThat(result).contains("WHITE");
	}

	@Test // DATACASS-292
	public void queryForObjectShouldReturnFirstColumn() {

		String id = template.queryForObject("SELECT id FROM user;", String.class);

		assertThat(id).isEqualTo("WHITE");
	}

	@Test // DATACASS-292
	public void queryForObjectShouldReturnMap() {

		Map<String, Object> map = template.queryForMap("SELECT * FROM user;");

		assertThat(map).containsEntry("id", "WHITE").containsEntry("username", "Walter");
	}

	@Test // DATACASS-292
	public void executeStatementShouldRemoveRecords() {

		template.execute(SimpleStatement.newInstance("DELETE FROM user WHERE id = 'WHITE'"));

		assertThat(session.execute("SELECT * FROM user").one()).isNull();
	}

	@Test // DATACASS-292
	public void queryStatementShouldInvokeCallback() {

		List<String> result = new ArrayList<>();
		template.query(SimpleStatement.newInstance("SELECT id FROM user"), row -> {
			result.add(row.getString(0));
		});

		assertThat(result).contains("WHITE");
	}

	@Test // DATACASS-292
	public void queryForObjectStatementShouldReturnFirstColumn() {

		String id = template.queryForObject(SimpleStatement.newInstance("SELECT id FROM user"), String.class);

		assertThat(id).isEqualTo("WHITE");
	}

	@Test // DATACASS-292
	public void queryForObjectStatementShouldReturnMap() {

		Map<String, Object> map = template.queryForMap(SimpleStatement.newInstance("SELECT * FROM user"));

		assertThat(map).containsEntry("id", "WHITE").containsEntry("username", "Walter");
	}

	@Test // DATACASS-292
	public void executeWithArgsShouldRemoveRecords() {

		template.execute("DELETE FROM user WHERE id = ?", "WHITE");

		assertThat(session.execute("SELECT * FROM user").one()).isNull();
	}

	@Test // DATACASS-292
	public void queryPreparedStatementShouldInvokeCallback() {

		List<String> result = new ArrayList<>();
		template.query("SELECT id FROM user WHERE id = ?;", row -> {
			result.add(row.getString(0));
		}, "WHITE");

		assertThat(result).contains("WHITE");
	}

	@Test // DATACASS-292
	public void queryPreparedStatementCreatorShouldInvokeCallback() {

		List<String> result = new ArrayList<>();
		template.query(session -> session.prepare("SELECT id FROM user WHERE id = ?;"), ps -> ps.bind("WHITE"), row -> {
			result.add(row.getString(0));
		});

		assertThat(result).contains("WHITE");
	}

	@Test // DATACASS-292
	public void queryForObjectWithArgsShouldReturnFirstColumn() {

		String id = template.queryForObject("SELECT id FROM user WHERE id = ?;", String.class, "WHITE");

		assertThat(id).isEqualTo("WHITE");
	}

	@Test // DATACASS-292
	public void queryForObjectWithArgsShouldReturnMap() {

		Map<String, Object> map = template.queryForMap("SELECT * FROM user WHERE id = ?;", "WHITE");

		assertThat(map).containsEntry("id", "WHITE").containsEntry("username", "Walter");
	}
}
