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

package org.springframework.data.cassandra.core.mapping;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.springframework.data.util.ClassTypeInformation;

/**
 * Unit tests for {@link BasicCassandraPersistentEntity}.
 *
 * @author Matthew T. Adams
 */
class ForceQuotedEntitiesSimpleUnitTests {

	@Test
	void testImplicitTableNameForceQuoted() {
		BasicCassandraPersistentEntity<ImplicitTableNameForceQuoted> entity = new BasicCassandraPersistentEntity<>(
				ClassTypeInformation.from(ImplicitTableNameForceQuoted.class));

		assertThat(entity.getTableName().asCql(false))
				.isEqualTo("\"" + ImplicitTableNameForceQuoted.class.getSimpleName() + "\"");
		assertThat(entity.getTableName().asInternal()).isEqualTo(ImplicitTableNameForceQuoted.class.getSimpleName());
	}

	@Table(forceQuote = true)
	private static class ImplicitTableNameForceQuoted {}

	private static final String EXPLICIT_TABLE_NAME = "Xx";

	@Test
	void testExplicitTableNameForceQuoted() {
		BasicCassandraPersistentEntity<ExplicitTableNameForceQuoted> entity = new BasicCassandraPersistentEntity<>(
				ClassTypeInformation.from(ExplicitTableNameForceQuoted.class));

		assertThat(entity.getTableName().asCql(true)).isEqualTo("\"" + EXPLICIT_TABLE_NAME + "\"");
		assertThat(entity.getTableName().asInternal()).isEqualTo(EXPLICIT_TABLE_NAME);
	}

	@Table(value = EXPLICIT_TABLE_NAME, forceQuote = true)
	private static class ExplicitTableNameForceQuoted {}

	@Test
	void testDefaultTableNameForceQuoted() {
		BasicCassandraPersistentEntity<DefaultTableNameForceQuoted> entity = new BasicCassandraPersistentEntity<>(
				ClassTypeInformation.from(DefaultTableNameForceQuoted.class));

		assertThat(entity.getTableName().asCql(true))
				.isEqualTo(DefaultTableNameForceQuoted.class.getSimpleName().toLowerCase());
		assertThat(entity.getTableName().asInternal())
				.isEqualTo(DefaultTableNameForceQuoted.class.getSimpleName().toLowerCase());
	}

	@Table
	private static class DefaultTableNameForceQuoted {}
}
