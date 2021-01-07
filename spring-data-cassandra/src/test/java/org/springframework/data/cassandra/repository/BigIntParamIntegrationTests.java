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

import java.math.BigInteger;
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
 * Integration tests for {@link BigInteger} usage in repositories.
 *
 * @author Pete Cable
 * @author Mark Paluch
 */
@SpringJUnitConfig
class BigIntParamIntegrationTests extends AbstractSpringDataEmbeddedCassandraIntegrationTest {

	@Configuration
	@EnableCassandraRepositories(basePackageClasses = BigThingRepo.class, considerNestedRepositories = true,
			includeFilters = @Filter(pattern = ".*BigThingRepo", type = FilterType.REGEX))
	public static class Config extends IntegrationTestConfig {

		@Override
		protected Set<Class<?>> getInitialEntitySet() {
			return Collections.singleton(BigThing.class);
		}

		@Override
		public SchemaAction getSchemaAction() {
			return SchemaAction.RECREATE_DROP_UNUSED;
		}
	}

	@Autowired BigThingRepo repo;

	@Test
	void testQueryWithReference() {
		BigInteger number = new BigInteger("42");
		BigThing saved = new BigThing(number);
		repo.save(saved);

		BigThing found = repo.findThingByBigInteger(new BigInteger("42"));
		assertThat(found).isNotNull();
	}

	/**
	 * @author Pete Cable
	 */
	@Table
	@Data
	@AllArgsConstructor
	@NoArgsConstructor
	static class BigThing {

		@PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED) private BigInteger number;
	}

	/**
	 * @author Pete Cable
	 */
	interface BigThingRepo extends MapIdCassandraRepository<BigThing> {

		@Query("SELECT * from bigthing where number = ?0")
		BigThing findThingByBigInteger(BigInteger number);
	}
}
