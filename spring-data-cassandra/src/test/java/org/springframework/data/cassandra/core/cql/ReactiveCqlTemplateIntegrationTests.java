/*
 * Copyright 2016-2018 the original author or authors.
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
package org.springframework.data.cassandra.core.cql;

import static org.assertj.core.api.Assertions.*;

import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.cassandra.ReactiveSession;
import org.springframework.data.cassandra.core.cql.session.DefaultBridgedReactiveSession;
import org.springframework.data.cassandra.core.cql.session.DefaultReactiveSessionFactory;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTest;

import com.datastax.driver.core.querybuilder.QueryBuilder;

/**
 * Integration tests for {@link ReactiveCqlTemplate}.
 *
 * @author Mark Paluch
 */
public class ReactiveCqlTemplateIntegrationTests extends AbstractKeyspaceCreatingIntegrationTest {

	private static final AtomicBoolean initialized = new AtomicBoolean();

	ReactiveSession reactiveSession;
	ReactiveCqlTemplate template;

	@Before
	public void before() {

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
	public void executeShouldRemoveRecords() {

		StepVerifier.create(template.execute("DELETE FROM user WHERE id = 'WHITE'")).expectNext(true).verifyComplete();

		assertThat(getSession().execute("SELECT * FROM user").one()).isNull();
	}

	@Test // DATACASS-335
	public void queryForObjectShouldReturnFirstColumn() {

		StepVerifier.create(template.queryForObject("SELECT id FROM user;", String.class)) //
				.expectNext("WHITE") //
				.verifyComplete();
	}

	@Test // DATACASS-335
	public void queryForObjectShouldReturnMap() {

		StepVerifier.create(template.queryForMap("SELECT * FROM user;")) //
				.consumeNextWith(actual -> {

					assertThat(actual).containsEntry("id", "WHITE").containsEntry("username", "Walter");
				}).verifyComplete();
	}

	@Test // DATACASS-335
	public void executeStatementShouldRemoveRecords() {

		StepVerifier
				.create(template.execute(QueryBuilder.delete() //
						.from("user") //
						.where(QueryBuilder.eq("id", "WHITE")))) //
				.expectNext(true) //
				.verifyComplete();

		assertThat(getSession().execute("SELECT * FROM user").one()).isNull();
	}

	@Test // DATACASS-335
	public void queryForObjectStatementShouldReturnFirstColumn() {

		StepVerifier
				.create(template.queryForObject(QueryBuilder //
						.select("id") //
						.from("user"), String.class)) //
				.expectNext("WHITE") //
				.verifyComplete();
	}

	@Test // DATACASS-335
	public void queryForObjectStatementShouldReturnMap() {

		StepVerifier.create(template.queryForMap(QueryBuilder.select().from("user"))) //
				.consumeNextWith(actual -> {

					assertThat(actual).containsEntry("id", "WHITE").containsEntry("username", "Walter");
				}).verifyComplete();
	}

	@Test // DATACASS-335
	public void executeWithArgsShouldRemoveRecords() {

		StepVerifier.create(template.execute("DELETE FROM user WHERE id = ?", "WHITE")).expectNext(true).verifyComplete();

		assertThat(getSession().execute("SELECT * FROM user").one()).isNull();
	}

	@Test // DATACASS-335
	public void queryForObjectWithArgsShouldReturnFirstColumn() {

		StepVerifier.create(template.queryForObject("SELECT id FROM user WHERE id = ?;", String.class, "WHITE")) //
				.expectNext("WHITE") //
				.verifyComplete();
	}

	@Test // DATACASS-335
	public void queryForObjectWithArgsShouldReturnMap() {

		StepVerifier.create(template.queryForMap("SELECT * FROM user WHERE id = ?;", "WHITE")) //
				.consumeNextWith(actual -> {

					assertThat(actual).containsEntry("id", "WHITE").containsEntry("username", "Walter");
				}).verifyComplete();
	}
}
