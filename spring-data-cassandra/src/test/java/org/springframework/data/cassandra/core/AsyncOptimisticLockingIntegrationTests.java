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

import java.util.concurrent.Future;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.data.annotation.Version;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.cql.CqlTemplate;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.cassandra.repository.support.SchemaTestUtils;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTests;

/**
 * Integration tests for optimistic locking through {@link AsyncCassandraTemplate}.
 *
 * @author Mark Paluch
 */
class AsyncOptimisticLockingIntegrationTests extends AbstractKeyspaceCreatingIntegrationTests {

	private AsyncCassandraTemplate template;

	@BeforeEach
	void setUp() {

		MappingCassandraConverter converter = new MappingCassandraConverter();

		converter.afterPropertiesSet();

		template = new AsyncCassandraTemplate(session, converter);

		CassandraTemplate syncTemplate = new CassandraTemplate(new CqlTemplate(session), converter);

		SchemaTestUtils.potentiallyCreateTableFor(VersionedEntity.class, syncTemplate);
		SchemaTestUtils.truncate(VersionedEntity.class, syncTemplate);
	}

	@Test // DATACASS-576
	void shouldInsertVersioned() {

		VersionedEntity versionedEntity = new VersionedEntity(42);

		VersionedEntity saved = getUninterruptibly(template.insert(versionedEntity));
		VersionedEntity loaded = getUninterruptibly(template.selectOne(Query.empty(), VersionedEntity.class));

		assertThat(saved.version).isEqualTo(1);
		assertThat(loaded).isNotNull();
		assertThat(loaded.version).isEqualTo(1);
	}

	@Test // DATACASS-576
	void duplicateInsertShouldFail() {

		getUninterruptibly(template.insert(new VersionedEntity(42)));

		assertThatThrownBy(() -> getUninterruptibly(template.insert(new VersionedEntity(42))))
				.hasRootCauseInstanceOf(OptimisticLockingFailureException.class);
	}

	@Test // DATACASS-576
	void shouldUpdateVersioned() {

		VersionedEntity versionedEntity = new VersionedEntity(42);

		VersionedEntity saved = getUninterruptibly(template.insert(versionedEntity));
		VersionedEntity updated = getUninterruptibly(template.update(saved));
		VersionedEntity loaded = getUninterruptibly(template.selectOne(Query.empty(), VersionedEntity.class));

		assertThat(saved.version).isEqualTo(1);
		assertThat(updated.version).isEqualTo(2);
		assertThat(loaded).isNotNull();
		assertThat(loaded.version).isEqualTo(2);
	}

	@Test // DATACASS-576
	void updateForOutdatedEntityShouldFail() {

		VersionedEntity versionedEntity = new VersionedEntity(42);

		getUninterruptibly(template.insert(versionedEntity));
		assertThatThrownBy(() -> getUninterruptibly(template.update(new VersionedEntity(42, 5, "f"))))
				.hasRootCauseInstanceOf(OptimisticLockingFailureException.class);
	}

	@Test // DATACASS-576
	void shouldDeleteVersionedEntity() {

		VersionedEntity versionedEntity = new VersionedEntity(42);

		VersionedEntity saved = getUninterruptibly(template.insert(versionedEntity));

		getUninterruptibly(template.delete(saved));

		VersionedEntity loaded = getUninterruptibly(template.selectOne(Query.empty(), VersionedEntity.class));

		assertThat(loaded).isNull();
	}

	@Test // DATACASS-576
	void deleteForOutdatedEntityShouldFail() {

		getUninterruptibly(template.insert(new VersionedEntity(42)));

		assertThatThrownBy(() -> getUninterruptibly(template.delete(new VersionedEntity(42))))
				.hasRootCauseInstanceOf(OptimisticLockingFailureException.class);

		VersionedEntity loaded = getUninterruptibly(template.selectOne(Query.empty(), VersionedEntity.class));

		assertThat(loaded).isNotNull();
	}

	private static <T> T getUninterruptibly(Future<T> future) {

		try {
			return future.get();
		} catch (Exception cause) {
			throw new IllegalStateException(cause);
		}
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
