/*
 * Copyright 2017-2021 the original author or authors.
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
package org.springframework.data.cassandra.repository;

import static org.assertj.core.api.Assertions.*;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Collections;
import java.util.Set;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.data.cassandra.config.SchemaAction;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.core.mapping.Table;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;
import org.springframework.data.cassandra.repository.support.AbstractSpringDataEmbeddedCassandraIntegrationTest;
import org.springframework.data.cassandra.repository.support.IntegrationTestConfig;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
@SpringJUnitConfig
class IntParamIntegrationTests extends AbstractSpringDataEmbeddedCassandraIntegrationTest {

	@Configuration
	@EnableCassandraRepositories(basePackageClasses = IntThingRepo.class, considerNestedRepositories = true,
			includeFilters = @Filter(pattern = ".*IntThingRepo", type = FilterType.REGEX))
	public static class Config extends IntegrationTestConfig {

		@Override
		protected Set<Class<?>> getInitialEntitySet() {
			return Collections.singleton(IntThing.class);
		}

		@Override
		public SchemaAction getSchemaAction() {
			return SchemaAction.RECREATE_DROP_UNUSED;
		}
	}

	@Autowired IntThingRepo repo;

	@Test
	void testQueryWithIntPrimitiveAndReference() {

		int number = 42;
		IntThing saved = new IntThing(42);
		repo.save(saved);

		IntThing found = repo.findThingByIntPrimitive(number);
		assertThat(found).isNotNull();

		found = repo.findThingByIntReference(new Integer(number));
		assertThat(found).isNotNull();
	}

	@Table
	@Data
	@AllArgsConstructor
	@NoArgsConstructor
	static class IntThing {

		@PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED) private int number;
	}

	interface IntThingRepo extends MapIdCassandraRepository<IntThing> {

		@Query("SELECT * from intthing where number = ?0")
		IntThing findThingByIntPrimitive(int number);

		@Query("SELECT * from intthing where number = ?0")
		IntThing findThingByIntReference(Integer number);
	}
}
