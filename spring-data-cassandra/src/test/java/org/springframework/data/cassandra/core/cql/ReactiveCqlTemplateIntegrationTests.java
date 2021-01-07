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
package org.springframework.data.cassandra.core.cql;

import static org.assertj.core.api.Assertions.*;
import static org.junit.Assume.*;

import reactor.test.StepVerifier;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.data.cassandra.CassandraInvalidQueryException;
import org.springframework.data.cassandra.ReactiveSession;
import org.springframework.data.cassandra.core.cql.session.DefaultBridgedReactiveSession;
import org.springframework.data.cassandra.core.cql.session.DefaultReactiveSessionFactory;
import org.springframework.data.cassandra.support.CassandraVersion;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTests;
import org.springframework.data.util.Version;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

/**
 * Integration tests for {@link ReactiveCqlTemplate}.
 *
 * @author Mark Paluch
 * @author Tomasz Lelek
 */
class ReactiveCqlTemplateIntegrationTests extends AbstractKeyspaceCreatingIntegrationTests {
	private static final Version CASSANDRA_4 = Version.parse("4.0");
	private static final AtomicBoolean initialized = new AtomicBoolean();

	private ReactiveSession reactiveSession;
	private ReactiveCqlTemplate template;
	private Version cassandraVersion;

	@BeforeEach
	void before() {

		reactiveSession = new DefaultBridgedReactiveSession(getSession());

		if (initialized.compareAndSet(false, true)) {
			getSession().execute("CREATE TABLE IF NOT EXISTS user (id text PRIMARY KEY, username text);");
		}

		getSession().execute("TRUNCATE user;");
		getSession().execute("INSERT INTO user (id, username) VALUES ('WHITE', 'Walter');");

		template = new ReactiveCqlTemplate(new DefaultReactiveSessionFactory(reactiveSession));
		cassandraVersion = CassandraVersion.get(getSession());
	}

	@Test // DATACASS-335
	void executeShouldRemoveRecords() {

		template.execute("DELETE FROM user WHERE id = 'WHITE'").as(StepVerifier::create).expectNext(true).verifyComplete();

		assertThat(getSession().execute("SELECT * FROM user").one()).isNull();
	}

	@Test // DATACASS-335
	void queryForObjectShouldReturnFirstColumn() {

		template.queryForObject("SELECT id FROM user;", String.class).as(StepVerifier::create) //
				.expectNext("WHITE") //
				.verifyComplete();
	}

	@Test // DATACASS-335
	void queryForObjectShouldReturnMap() {

		template.queryForMap("SELECT * FROM user;").as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					assertThat(actual).containsEntry("id", "WHITE").containsEntry("username", "Walter");
				}).verifyComplete();
	}

	@Test // DATACASS-335
	void executeStatementShouldRemoveRecords() {

		template.execute(SimpleStatement.newInstance("DELETE FROM user WHERE id = 'WHITE'")).as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();

		assertThat(getSession().execute("SELECT * FROM user").one()).isNull();
	}

	@Test // DATACASS-335
	void queryForObjectStatementShouldReturnFirstColumn() {

		template.queryForObject(SimpleStatement.newInstance("SELECT id FROM user"), String.class).as(StepVerifier::create) //
				.expectNext("WHITE") //
				.verifyComplete();
	}

	@Test // DATACASS-335
	void queryForObjectStatementShouldReturnMap() {

		template.queryForMap(SimpleStatement.newInstance("SELECT * FROM user")).as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					assertThat(actual).containsEntry("id", "WHITE").containsEntry("username", "Walter");
				}).verifyComplete();
	}

	@Test // DATACASS-335
	void executeWithArgsShouldRemoveRecords() {

		template.execute("DELETE FROM user WHERE id = ?", "WHITE").as(StepVerifier::create).expectNext(true)
				.verifyComplete();

		assertThat(getSession().execute("SELECT * FROM user").one()).isNull();
	}

	@Test // DATACASS-335
	void queryForObjectWithArgsShouldReturnFirstColumn() {

		template.queryForObject("SELECT id FROM user WHERE id = ?;", String.class, "WHITE").as(StepVerifier::create) //
				.expectNext("WHITE") //
				.verifyComplete();
	}

	@Test // DATACASS-335
	void queryForObjectWithArgsShouldReturnMap() {

		template.queryForMap("SELECT * FROM user WHERE id = ?;", "WHITE").as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					assertThat(actual).containsEntry("id", "WHITE").containsEntry("username", "Walter");
				}).verifyComplete();
	}

	@Test // DATACASS-767
	void selectByQueryWithKeyspaceShouldRetrieveData() {
		assumeTrue(cassandraVersion.isGreaterThanOrEqualTo(CASSANDRA_4));

		template.setKeyspace(CqlIdentifier.fromCql(keyspace));

		template.queryForMap("SELECT * FROM user;").as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					assertThat(actual).containsEntry("id", "WHITE").containsEntry("username", "Walter");
				}).verifyComplete();
	}

	@Test // DATACASS-767
	void selectByQueryWithNonExistingKeyspaceShouldThrowThatKeyspaceDoesNotExists() {
		assumeTrue(cassandraVersion.isGreaterThanOrEqualTo(CASSANDRA_4));

		template.setKeyspace(CqlIdentifier.fromCql("non_existing"));

		template.queryForMap("SELECT * FROM user;").as(StepVerifier::create) //
				.consumeErrorWith(e -> {
					assertThat(e).isInstanceOf(CassandraInvalidQueryException.class)
							.hasMessageContaining("Keyspace 'non_existing' does not exist");
				}).verify();

	}
}
