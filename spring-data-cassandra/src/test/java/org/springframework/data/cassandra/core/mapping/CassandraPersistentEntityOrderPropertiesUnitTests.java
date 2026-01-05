/*
 * Copyright 2016-present the original author or authors.
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
package org.springframework.data.cassandra.core.mapping;

import static org.assertj.core.api.Assertions.*;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.util.ObjectUtils;

/**
 * Unit tests for {@link CassandraMappingContext}.
 *
 * @author David Webb
 * @author Mark Paluch
 */
class CassandraPersistentEntityOrderPropertiesUnitTests {

	private List<CassandraPersistentProperty> expected;
	private CassandraMappingContext mappingContext = new CassandraMappingContext();

	@BeforeEach
	void init() {}

	@Test
	void testCompositeKeyPropertyOrder() {

		CassandraPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(CompositePK.class);

		expected = new LinkedList<>();
		expected.add(entity.getRequiredPersistentProperty("key0"));
		expected.add(entity.getRequiredPersistentProperty("key1"));
		expected.add(entity.getRequiredPersistentProperty("key2"));

		final List<CassandraPersistentProperty> actual = new LinkedList<>();

		entity.doWithProperties((PropertyHandler<CassandraPersistentProperty>) actual::add);

		assertThat(actual).isEqualTo(expected);
	}

	@Test
	void testTablePropertyOrder() {

		CassandraPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(CompositeKeyEntity.class);

		expected = new LinkedList<>();
		expected.add(entity.getRequiredPersistentProperty("key"));
		expected.add(entity.getRequiredPersistentProperty("attribute"));
		expected.add(entity.getRequiredPersistentProperty("text"));

		final List<CassandraPersistentProperty> actual = new LinkedList<>();

		entity.doWithProperties((PropertyHandler<CassandraPersistentProperty>) actual::add);

		assertThat(actual).isEqualTo(expected);

	}

	@Table
	private static class CompositeKeyEntity {

		@PrimaryKey private CompositePK key;

		private String attribute;

		private String text;

	}

	/**
	 * This is intentionally using dumb ordinals
	 */
	@PrimaryKeyClass
	private static class CompositePK implements Serializable {

		@PrimaryKeyColumn(ordinal = 2, type = PrimaryKeyType.PARTITIONED) private String key0;

		@PrimaryKeyColumn(ordinal = 0) private String key1;

		@PrimaryKeyColumn(ordinal = 1) private String key2;

		@Override
		public boolean equals(Object o) {
			if (this == o)
				return true;
			if (o == null || getClass() != o.getClass())
				return false;

			CompositePK that = (CompositePK) o;

			if (!ObjectUtils.nullSafeEquals(key0, that.key0)) {
				return false;
			}
			if (!ObjectUtils.nullSafeEquals(key1, that.key1)) {
				return false;
			}
			return ObjectUtils.nullSafeEquals(key2, that.key2);
		}

		@Override
		public int hashCode() {
			int result = ObjectUtils.nullSafeHashCode(key0);
			result = 31 * result + ObjectUtils.nullSafeHashCode(key1);
			result = 31 * result + ObjectUtils.nullSafeHashCode(key2);
			return result;
		}
	}

	@Table
	private static class SimpleKeyEntity {

		@Id private String id;

	}

}
