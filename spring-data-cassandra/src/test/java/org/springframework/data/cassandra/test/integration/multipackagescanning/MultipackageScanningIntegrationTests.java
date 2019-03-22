/*
 * Copyright 2013-2014 the original author or authors
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   https://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.test.integration.multipackagescanning;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.HashSet;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.cassandra.config.CassandraEntityClassScanner;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.mapping.BasicCassandraMappingContext;
import org.springframework.data.cassandra.test.integration.multipackagescanning.first.First;
import org.springframework.data.cassandra.test.integration.multipackagescanning.second.Second;
import org.springframework.data.cassandra.test.integration.multipackagescanning.third.Third;

public class MultipackageScanningIntegrationTests {

	BasicCassandraMappingContext mapping;
	String pkg = getClass().getPackage().getName();

	@Before
	public void before() throws ClassNotFoundException {

		mapping = new BasicCassandraMappingContext();
		mapping.setInitialEntitySet(CassandraEntityClassScanner.scan(pkg + ".first", pkg + ".second"));

		mapping.initialize();
	}

	@Test
	public void test() {

		Collection<CassandraPersistentEntity<?>> entities = mapping.getPersistentEntities();

		Collection<Class<?>> types = new HashSet<Class<?>>(entities.size());
		for (CassandraPersistentEntity<?> entity : entities) {
			types.add(entity.getType());
		}

		assertTrue(types.contains(First.class));
		assertTrue(types.contains(Second.class));
		assertFalse(types.contains(Third.class));
		assertFalse(types.contains(Top.class));
	}
}
