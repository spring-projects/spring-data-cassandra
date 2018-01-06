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

package org.springframework.data.cassandra.core.mapping;

import static org.assertj.core.api.Assertions.*;

import org.junit.Test;
import org.springframework.data.util.ClassTypeInformation;

/**
 * Unit tests for {@link BasicCassandraPersistentEntity}.
 *
 * @author Matthew T. Adams
 */
public class ForceQuotedEntitiesSimpleUnitTests {

	@Test
	public void testImplicitTableNameForceQuoted() {
		BasicCassandraPersistentEntity<ImplicitTableNameForceQuoted> entity = new BasicCassandraPersistentEntity<>(
				ClassTypeInformation.from(ImplicitTableNameForceQuoted.class));

		assertThat(entity.getTableName().toCql())
				.isEqualTo("\"" + ImplicitTableNameForceQuoted.class.getSimpleName() + "\"");
		assertThat(entity.getTableName().getUnquoted()).isEqualTo(ImplicitTableNameForceQuoted.class.getSimpleName());
	}

	@Table(forceQuote = true)
	public static class ImplicitTableNameForceQuoted {}

	public static final String EXPLICIT_TABLE_NAME = "Xx";

	@Test
	public void testExplicitTableNameForceQuoted() {
		BasicCassandraPersistentEntity<ExplicitTableNameForceQuoted> entity = new BasicCassandraPersistentEntity<>(
				ClassTypeInformation.from(ExplicitTableNameForceQuoted.class));

		assertThat(entity.getTableName().toCql()).isEqualTo("\"" + EXPLICIT_TABLE_NAME + "\"");
		assertThat(entity.getTableName().getUnquoted()).isEqualTo(EXPLICIT_TABLE_NAME);
	}

	@Table(value = EXPLICIT_TABLE_NAME, forceQuote = true)
	public static class ExplicitTableNameForceQuoted {}

	@Test
	public void testDefaultTableNameForceQuoted() {
		BasicCassandraPersistentEntity<DefaultTableNameForceQuoted> entity = new BasicCassandraPersistentEntity<>(
				ClassTypeInformation.from(DefaultTableNameForceQuoted.class));

		assertThat(entity.getTableName().toCql())
				.isEqualTo(DefaultTableNameForceQuoted.class.getSimpleName().toLowerCase());
		assertThat(entity.getTableName().getUnquoted())
				.isEqualTo(DefaultTableNameForceQuoted.class.getSimpleName().toLowerCase());
	}

	@Table
	public static class DefaultTableNameForceQuoted {}
}
