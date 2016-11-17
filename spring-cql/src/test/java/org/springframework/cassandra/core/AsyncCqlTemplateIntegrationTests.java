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
package org.springframework.cassandra.core;

import static org.assertj.core.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Before;
import org.junit.Test;
import org.springframework.cassandra.test.integration.AbstractKeyspaceCreatingIntegrationTest;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

/**
 * Integration tests for {@link AsyncCqlTemplate}.
 * 
 * @author Mark Paluch
 */
public class AsyncCqlTemplateIntegrationTests extends AbstractKeyspaceCreatingIntegrationTest {

	private static final AtomicBoolean initialized = new AtomicBoolean();
	private AsyncCqlTemplate template;

	@Before
	public void before() throws Exception {

		if (initialized.compareAndSet(false, true)) {
			getSession().execute("CREATE TABLE IF NOT EXISTS user (id text PRIMARY KEY, username text);");
		} else {
			session.execute("TRUNCATE user;");
		}

		session.execute("INSERT INTO user (id, username) VALUES ('WHITE', 'Walter');");

		template = new AsyncCqlTemplate();
		template.setSession(getSession());
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void executeShouldRemoveRecords() throws Exception {

		template.execute("DELETE FROM user WHERE id = 'WHITE'").get();

		assertThat(session.execute("SELECT * FROM user").one()).isNull();
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void queryShouldInvokeCallback() throws Exception {

		List<String> result = new ArrayList<>();
		template.query("SELECT id FROM user;", row -> {
			result.add(row.getString(0));
		}).get();

		assertThat(result).contains("WHITE");
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void queryForObjectShouldReturnFirstColumn() throws Exception {

		String id = template.queryForObject("SELECT id FROM user;", String.class).get();

		assertThat(id).isEqualTo("WHITE");
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void queryForObjectShouldReturnMap() throws Exception {

		Map<String, Object> map = template.queryForMap("SELECT * FROM user;").get();

		assertThat(map).containsEntry("id", "WHITE").containsEntry("username", "Walter");
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void executeStatementShouldRemoveRecords() throws Exception {

		template.execute(QueryBuilder.delete().from("user").where(QueryBuilder.eq("id", "WHITE"))).get();

		assertThat(session.execute("SELECT * FROM user").one()).isNull();
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void queryStatementShouldInvokeCallback() throws Exception {

		List<String> result = new ArrayList<>();
		template.query(QueryBuilder.select("id").from("user"), row -> {
			result.add(row.getString(0));
		}).get();

		assertThat(result).contains("WHITE");
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void queryForObjectStatementShouldReturnFirstColumn() throws Exception {

		String id = template.queryForObject(QueryBuilder.select("id").from("user"), String.class).get();

		assertThat(id).isEqualTo("WHITE");
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void queryForObjectStatementShouldReturnMap() throws Exception {

		Map<String, Object> map = template.queryForMap(QueryBuilder.select().from("user")).get();

		assertThat(map).containsEntry("id", "WHITE").containsEntry("username", "Walter");
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void executeWithArgsShouldRemoveRecords() throws Exception {

		template.execute("DELETE FROM user WHERE id = ?", "WHITE").get();

		assertThat(session.execute("SELECT * FROM user").one()).isNull();
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void queryPreparedStatementShouldInvokeCallback() throws Exception {

		List<String> result = new ArrayList<>();
		template.query("SELECT id FROM user WHERE id = ?;", row -> {
			result.add(row.getString(0));
		}, "WHITE").get();

		assertThat(result).contains("WHITE");
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void queryPreparedStatementCreatorShouldInvokeCallback() throws Exception {

		List<String> result = new ArrayList<>();
		template.query(
				session -> new GuavaListenableFutureAdapter<PreparedStatement>(
						session.prepareAsync("SELECT id FROM user WHERE id = ?;"), template.getExceptionTranslator()),
				ps -> ps.bind("WHITE"), row -> {
					result.add(row.getString(0));
				}).get();

		assertThat(result).contains("WHITE");
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void queryForObjectWithArgsShouldReturnFirstColumn() throws Exception {

		String id = template.queryForObject("SELECT id FROM user WHERE id = ?;", String.class, "WHITE").get();

		assertThat(id).isEqualTo("WHITE");
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void queryForObjectWithArgsShouldReturnMap() throws Exception {

		Map<String, Object> map = template.queryForMap("SELECT * FROM user WHERE id = ?;", "WHITE").get();

		assertThat(map).containsEntry("id", "WHITE").containsEntry("username", "Walter");
	}
}
