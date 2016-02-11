/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.data.cassandra.mapping;

import static org.junit.Assert.*;

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
		BasicCassandraPersistentEntity<ImplicitTableNameForceQuoted> entity = new BasicCassandraPersistentEntity<ImplicitTableNameForceQuoted>(
				ClassTypeInformation.from(ImplicitTableNameForceQuoted.class));

		assertEquals("\"" + ImplicitTableNameForceQuoted.class.getSimpleName() + "\"", entity.getTableName().toCql());
		assertEquals(ImplicitTableNameForceQuoted.class.getSimpleName(), entity.getTableName().getUnquoted());
	}

	@Table(forceQuote = true)
	public static class ImplicitTableNameForceQuoted {}

	public static final String EXPLICIT_TABLE_NAME = "Xx";

	@Test
	public void testExplicitTableNameForceQuoted() {
		BasicCassandraPersistentEntity<ExplicitTableNameForceQuoted> entity = new BasicCassandraPersistentEntity<ExplicitTableNameForceQuoted>(
				ClassTypeInformation.from(ExplicitTableNameForceQuoted.class));

		assertEquals("\"" + EXPLICIT_TABLE_NAME + "\"", entity.getTableName().toCql());
		assertEquals(EXPLICIT_TABLE_NAME, entity.getTableName().getUnquoted());
	}

	@Table(value = EXPLICIT_TABLE_NAME, forceQuote = true)
	public static class ExplicitTableNameForceQuoted {}

	@Test
	public void testDefaultTableNameForceQuoted() {
		BasicCassandraPersistentEntity<DefaultTableNameForceQuoted> entity = new BasicCassandraPersistentEntity<DefaultTableNameForceQuoted>(
				ClassTypeInformation.from(DefaultTableNameForceQuoted.class));

		assertEquals(DefaultTableNameForceQuoted.class.getSimpleName().toLowerCase(), entity.getTableName().toCql());
		assertEquals(DefaultTableNameForceQuoted.class.getSimpleName().toLowerCase(), entity.getTableName().getUnquoted());
	}

	@Table
	public static class DefaultTableNameForceQuoted {}
}
