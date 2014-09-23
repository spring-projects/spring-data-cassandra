/*
 * Copyright 2013-2014 the original author or authors
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.test.integration.mappingcontext;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.mapping.BasicCassandraMappingContext;
import org.springframework.data.cassandra.mapping.PrimaryKey;
import org.springframework.data.cassandra.mapping.Table;
import org.springframework.data.mapping.model.MappingException;

public class MappingContextIntegrationTests {

	public static class Transient {}

	@Table
	public static class X {
		@PrimaryKey
		String key;
	}

	@Table
	public static class Y {
		@PrimaryKey
		String key;
	}

	BasicCassandraMappingContext ctx = new BasicCassandraMappingContext();

	@Test(expected = MappingException.class)
	public void testGetPersistentEntityOfTransientType() {

		CassandraPersistentEntity<?> entity = ctx.getPersistentEntity(Transient.class);

	}

	@Test
	public void testGetExistingPersistentEntityHappyPath() {

		ctx.getPersistentEntity(X.class);

		assertTrue(ctx.contains(X.class));
		assertNotNull(ctx.getExistingPersistentEntity(X.class));
		assertFalse(ctx.contains(Y.class));
	}
}
