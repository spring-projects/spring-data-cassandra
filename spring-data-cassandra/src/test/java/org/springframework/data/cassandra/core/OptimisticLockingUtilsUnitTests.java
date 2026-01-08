/*
 * Copyright 2026-present the original author or authors.
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

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyClass;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;

/**
 * Unit tests for {@link OptimisticLockingUtils}.
 *
 * @author Mark Paluch
 */
class OptimisticLockingUtilsUnitTests {

	@ParameterizedTest
	@MethodSource("entities")
	void insertShouldFail(EntityOperations.AdaptibleEntity<?> entity) {
		assertThat(OptimisticLockingUtils.insertFailed(entity)).hasMessageContaining("Failed to insert")
				.hasMessageContaining("entity with id").hasMessageContaining("version '1'");
	}

	@ParameterizedTest
	@MethodSource("entities")
	void updateShouldFail(EntityOperations.AdaptibleEntity<?> entity) {
		assertThat(OptimisticLockingUtils.updateFailed(entity)).hasMessageContaining("Failed to update")
				.hasMessageContaining("entity with id").hasMessageContaining("version '1'");
	}

	@ParameterizedTest
	@MethodSource("entities")
	void deleteShouldFail(EntityOperations.AdaptibleEntity<?> entity) {
		assertThat(OptimisticLockingUtils.deleteFailed(entity)).hasMessageContaining("Failed to delete")
				.hasMessageContaining("entity with id").hasMessageContaining("version '1'");
	}

	static List<EntityOperations.AdaptibleEntity<?>> entities() {

		MappingCassandraConverter converter = new MappingCassandraConverter();
		EntityOperations operations = new EntityOperations(converter);

		List<EntityOperations.AdaptibleEntity<?>> entities = new ArrayList<>();
		entities.add(operations.forEntity(new Simple("foo", 1), converter.getConversionService()));
		entities.add(operations.forEntity(new WithCompositePrimaryKey(new CompositePrimaryKey("primary", "key"), 1),
				converter.getConversionService()));
		entities.add(operations.forEntity(new WithPrimaryKeyColumns("foo", "bar", 1), converter.getConversionService()));
		entities.add(operations.forEntity(new WithoutPrimaryKey(1), converter.getConversionService()));

		return entities;
	}

	static class Simple {

		@Id String id;

		@Version int version;

		public Simple(String id, int version) {
			this.id = id;
			this.version = version;
		}
	}

	static class WithCompositePrimaryKey {

		@Id CompositePrimaryKey id;

		@Version int version;

		public WithCompositePrimaryKey(CompositePrimaryKey id, int version) {
			this.id = id;
			this.version = version;
		}
	}

	@PrimaryKeyClass
	static class CompositePrimaryKey {

		@PrimaryKeyColumn(type = PrimaryKeyType.PARTITIONED) String a;

		@PrimaryKeyColumn String b;

		public CompositePrimaryKey(String a, String b) {
			this.a = a;
			this.b = b;
		}
	}

	static class WithPrimaryKeyColumns {

		@PrimaryKeyColumn(type = PrimaryKeyType.PARTITIONED) String a;

		@PrimaryKeyColumn String b;

		@Version int version;

		public WithPrimaryKeyColumns(String a, String b, int version) {
			this.a = a;
			this.b = b;
			this.version = version;
		}
	}

	static class WithoutPrimaryKey {

		@Version int version;

		public WithoutPrimaryKey(int version) {
			this.version = version;
		}
	}
}
