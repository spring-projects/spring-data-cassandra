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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.data.annotation.Version;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.repository.support.SchemaTestUtils;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTests;

/**
 * Integration tests for optimistic locking through {@link CassandraTemplate}.
 *
 * @author Mark Paluch
 */
class OptimisticLockingIntegrationTests extends AbstractKeyspaceCreatingIntegrationTests {

	private CassandraTemplate template;

	@BeforeEach
	void setUp() {

		MappingCassandraConverter converter = new MappingCassandraConverter();

		converter.afterPropertiesSet();

		template = new CassandraTemplate(session, converter);

		SchemaTestUtils.potentiallyCreateTableFor(VersionedEntity.class, template);
		SchemaTestUtils.truncate(VersionedEntity.class, template);
	}

	@Test // DATACASS-576
	void shouldInsertVersioned() {

		VersionedEntity versionedEntity = new VersionedEntity(42);

		VersionedEntity saved = template.insert(versionedEntity);
		VersionedEntity loaded = template.query(VersionedEntity.class).firstValue();

		assertThat(saved.version).isEqualTo(1);
		assertThat(loaded).isNotNull();
		assertThat(loaded.version).isEqualTo(1);
	}

	@Test // DATACASS-576
	void duplicateInsertShouldFail() {

		template.insert(new VersionedEntity(42));

		assertThatThrownBy(() -> template.insert(new VersionedEntity(42)))
				.isInstanceOf(OptimisticLockingFailureException.class);
	}

	@Test // DATACASS-576
	void shouldUpdateVersioned() {

		VersionedEntity versionedEntity = new VersionedEntity(42);

		VersionedEntity saved = template.insert(versionedEntity);
		VersionedEntity updated = template.update(saved);
		VersionedEntity loaded = template.query(VersionedEntity.class).firstValue();

		assertThat(saved.version).isEqualTo(1);
		assertThat(updated.version).isEqualTo(2);
		assertThat(loaded).isNotNull();
		assertThat(loaded.version).isEqualTo(2);
	}

	@Test // DATACASS-576
	void updateForOutdatedEntityShouldFail() {

		VersionedEntity versionedEntity = new VersionedEntity(42);

		template.insert(versionedEntity);

		assertThatThrownBy(() -> template.update(new VersionedEntity(42, 5, "f")))
				.isInstanceOf(OptimisticLockingFailureException.class);
	}

	@Test // DATACASS-576
	void shouldDeleteVersionedEntity() {

		VersionedEntity versionedEntity = new VersionedEntity(42);

		VersionedEntity saved = template.insert(versionedEntity);

		template.delete(saved);

		VersionedEntity loaded = template.query(VersionedEntity.class).firstValue();

		assertThat(loaded).isNull();
	}

	@Test // DATACASS-576
	void deleteForOutdatedEntityShouldFail() {

		template.insert(new VersionedEntity(42));

		assertThatThrownBy(() -> template.delete(new VersionedEntity(42)))
				.isInstanceOf(OptimisticLockingFailureException.class);

		VersionedEntity loaded = template.query(VersionedEntity.class).firstValue();

		assertThat(loaded).isNotNull();
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
