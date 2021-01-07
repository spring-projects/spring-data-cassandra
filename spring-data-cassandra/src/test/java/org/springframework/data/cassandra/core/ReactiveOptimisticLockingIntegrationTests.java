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
package org.springframework.data.cassandra.core;

import static org.assertj.core.api.Assertions.*;

import lombok.Data;
import lombok.experimental.Wither;
import reactor.test.StepVerifier;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.data.annotation.Version;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.cql.CqlTemplate;
import org.springframework.data.cassandra.core.cql.session.DefaultBridgedReactiveSession;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.cassandra.repository.support.SchemaTestUtils;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTests;

/**
 * Integration tests for optimistic locking through {@link ReactiveCassandraTemplate}.
 *
 * @author Mark Paluch
 */
class ReactiveOptimisticLockingIntegrationTests extends AbstractKeyspaceCreatingIntegrationTests {

	private ReactiveCassandraTemplate template;

	@BeforeEach
	void setUp() {

		MappingCassandraConverter converter = new MappingCassandraConverter();

		converter.afterPropertiesSet();

		template = new ReactiveCassandraTemplate(new DefaultBridgedReactiveSession(session), converter);

		CassandraTemplate syncTemplate = new CassandraTemplate(new CqlTemplate(session), converter);

		SchemaTestUtils.potentiallyCreateTableFor(VersionedEntity.class, syncTemplate);
		SchemaTestUtils.truncate(VersionedEntity.class, syncTemplate);
	}

	@Test // DATACASS-576
	void shouldInsertVersioned() {

		VersionedEntity versionedEntity = new VersionedEntity(42);

		template.insert(versionedEntity) //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> assertThat(actual.version).isEqualTo(1))
				.verifyComplete();

		template.selectOne(Query.empty(), VersionedEntity.class) //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> assertThat(actual.version).isEqualTo(1))
				.verifyComplete();
	}

	@Test // DATACASS-576
	void duplicateInsertShouldFail() {

		template.insert(new VersionedEntity(42)) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		template.insert(new VersionedEntity(42)) //
				.as(StepVerifier::create) //
				.verifyError(OptimisticLockingFailureException.class);
	}

	@Test // DATACASS-576
	void shouldUpdateVersioned() {

		VersionedEntity versionedEntity = new VersionedEntity(42);

		template.insert(versionedEntity).flatMap(template::update) //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> assertThat(actual.version).isEqualTo(2))
				.verifyComplete();

		template.selectOne(Query.empty(), VersionedEntity.class) //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> assertThat(actual.version).isEqualTo(2))
				.verifyComplete();
	}

	@Test // DATACASS-576
	void updateForOutdatedEntityShouldFail() {

		template.insert(new VersionedEntity(42)) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		template.update(new VersionedEntity(42, 5, "f")) //
				.as(StepVerifier::create) //
				.verifyError(OptimisticLockingFailureException.class);
	}

	@Test // DATACASS-576
	void shouldDeleteVersionedEntity() {

		VersionedEntity versionedEntity = new VersionedEntity(42);

		template.insert(versionedEntity).flatMap(template::delete) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		template.query(VersionedEntity.class).first() //
				.as(StepVerifier::create) //
				.verifyComplete();
	}

	@Test // DATACASS-576
	void deleteForOutdatedEntityShouldFail() {

		template.insert(new VersionedEntity(42))//
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		template.delete(new VersionedEntity(42)) //
				.as(StepVerifier::create) //
				.verifyError(OptimisticLockingFailureException.class);

		template.query(VersionedEntity.class).first() //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();
	}

	@Data
	@Wither
	static class VersionedEntity {

		@Id final long id;

		@Version final long version;

		final String name;

		private VersionedEntity(long id) {
			this(id, 0, null);
		}

		@PersistenceConstructor
		private VersionedEntity(long id, long version, String name) {
			this.id = id;
			this.version = version;
			this.name = name;
		}
	}
}
