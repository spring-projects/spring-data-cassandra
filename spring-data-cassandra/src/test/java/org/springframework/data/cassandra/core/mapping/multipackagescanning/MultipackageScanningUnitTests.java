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
package org.springframework.data.cassandra.core.mapping.multipackagescanning;

import static org.assertj.core.api.Assertions.*;

import java.util.Collection;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.cassandra.config.CassandraEntityClassScanner;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty;
import org.springframework.data.cassandra.core.mapping.multipackagescanning.first.First;
import org.springframework.data.cassandra.core.mapping.multipackagescanning.second.Second;
import org.springframework.data.cassandra.core.mapping.multipackagescanning.third.Third;
import org.springframework.data.mapping.context.MappingContext;

/**
 * Unit tests for {@link CassandraMappingContext}.
 *
 * @author Matthew T. Adams
 */
class MultipackageScanningUnitTests {

	private MappingContext<? extends CassandraPersistentEntity<?>, ? extends CassandraPersistentProperty> context;
	private String pkg = getClass().getPackage().getName();

	@BeforeEach
	void before() throws ClassNotFoundException {

		CassandraMappingContext context = new CassandraMappingContext();

		context.setInitialEntitySet(CassandraEntityClassScanner.scan(pkg + ".first", pkg + ".second"));
		context.initialize();
		this.context = context;
	}

	@Test
	void test() {

		Collection<? extends CassandraPersistentEntity<?>> entities = context.getPersistentEntities();

		Collection<Class<?>> types = entities.stream().map(CassandraPersistentEntity::getType).collect(Collectors.toSet());

		assertThat(types.contains(First.class)).isTrue();
		assertThat(types.contains(Second.class)).isTrue();
		assertThat(types.contains(Third.class)).isFalse();
		assertThat(types.contains(Top.class)).isFalse();
	}
}
