/*
 * Copyright 2016-2017 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Before;
import org.junit.Test;
import org.springframework.cassandra.core.session.DefaultBridgedReactiveSession;
import org.springframework.cassandra.core.session.DefaultReactiveSessionFactory;
import org.springframework.cassandra.core.session.ReactiveSession;
import org.springframework.cassandra.test.integration.AbstractKeyspaceCreatingIntegrationTest;

import reactor.core.scheduler.Schedulers;

import com.datastax.driver.core.querybuilder.QueryBuilder;

/**
 * Integration tests for {@link ReactiveCqlTemplate}.
 *
 * @author Mark Paluch
 */
public class ReactiveCqlTemplateIntegrationTests extends AbstractKeyspaceCreatingIntegrationTest {

	private static final AtomicBoolean initialized = new AtomicBoolean();
	private ReactiveSession reactiveSession;
	private ReactiveCqlTemplate template;

	@Before
	public void before() throws Exception {

		reactiveSession = new DefaultBridgedReactiveSession(getSession(), Schedulers.elastic());

		if (initialized.compareAndSet(false, true)) {
			getSession().execute("CREATE TABLE IF NOT EXISTS user (id text PRIMARY KEY, username text);");
		} else {
			getSession().execute("TRUNCATE user;");
		}

		getSession().execute("INSERT INTO user (id, username) VALUES ('WHITE', 'Walter');");

		template = new ReactiveCqlTemplate(new DefaultReactiveSessionFactory(reactiveSession));
	}

	@Test // DATACASS-335
	public void executeShouldRemoveRecords() throws Exception {

		template.execute("DELETE FROM user WHERE id = 'WHITE'").block();

		assertThat(getSession().execute("SELECT * FROM user").one()).isNull();
	}

	@Test // DATACASS-335
	public void queryForObjectShouldReturnFirstColumn() throws Exception {

		String id = template.queryForObject("SELECT id FROM user;", String.class).block();

		assertThat(id).isEqualTo("WHITE");
	}

	@Test // DATACASS-335
	public void queryForObjectShouldReturnMap() throws Exception {

		Map<String, Object> map = template.queryForMap("SELECT * FROM user;").block();

		assertThat(map).containsEntry("id", "WHITE").containsEntry("username", "Walter");
	}

	@Test // DATACASS-335
	public void executeStatementShouldRemoveRecords() throws Exception {

		template.execute(QueryBuilder.delete().from("user").where(QueryBuilder.eq("id", "WHITE"))).block();

		assertThat(getSession().execute("SELECT * FROM user").one()).isNull();
	}

	@Test // DATACASS-335
	public void queryForObjectStatementShouldReturnFirstColumn() throws Exception {

		String id = template.queryForObject(QueryBuilder.select("id").from("user"), String.class).block();

		assertThat(id).isEqualTo("WHITE");
	}

	@Test // DATACASS-335
	public void queryForObjectStatementShouldReturnMap() throws Exception {

		Map<String, Object> map = template.queryForMap(QueryBuilder.select().from("user")).block();

		assertThat(map).containsEntry("id", "WHITE").containsEntry("username", "Walter");
	}

	@Test // DATACASS-335
	public void executeWithArgsShouldRemoveRecords() throws Exception {

		template.execute("DELETE FROM user WHERE id = ?", "WHITE").block();

		assertThat(getSession().execute("SELECT * FROM user").one()).isNull();
	}

	@Test // DATACASS-335
	public void queryForObjectWithArgsShouldReturnFirstColumn() throws Exception {

		String id = template.queryForObject("SELECT id FROM user WHERE id = ?;", String.class, "WHITE").block();

		assertThat(id).isEqualTo("WHITE");
	}

	@Test // DATACASS-335
	public void queryForObjectWithArgsShouldReturnMap() throws Exception {

		Map<String, Object> map = template.queryForMap("SELECT * FROM user WHERE id = ?;", "WHITE").block();

		assertThat(map).containsEntry("id", "WHITE").containsEntry("username", "Walter");
	}
}
