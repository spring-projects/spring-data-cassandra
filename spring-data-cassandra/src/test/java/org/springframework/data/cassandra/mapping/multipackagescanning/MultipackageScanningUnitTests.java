/*
 * Copyright 2016-2017 the original author or authors.
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
package org.springframework.data.cassandra.mapping.multipackagescanning;

import static org.assertj.core.api.Assertions.*;

import java.util.Collection;
import java.util.HashSet;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.cassandra.config.CassandraEntityClassScanner;
import org.springframework.data.cassandra.mapping.BasicCassandraMappingContext;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.mapping.multipackagescanning.first.First;
import org.springframework.data.cassandra.mapping.multipackagescanning.second.Second;
import org.springframework.data.cassandra.mapping.multipackagescanning.third.Third;

/**
 * Unit tests for {@link BasicCassandraMappingContext}.
 *
 * @author Matthew T. Adams
 */
public class MultipackageScanningUnitTests {

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

		Collection<Class<?>> types = entities.stream().map(CassandraPersistentEntity::getType).collect(Collectors.toSet());

		assertThat(types.contains(First.class)).isTrue();
		assertThat(types.contains(Second.class)).isTrue();
		assertThat(types.contains(Third.class)).isFalse();
		assertThat(types.contains(Top.class)).isFalse();
	}
}
