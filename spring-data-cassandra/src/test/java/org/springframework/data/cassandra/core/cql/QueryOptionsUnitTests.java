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
package org.springframework.data.cassandra.core.cql;

import static org.assertj.core.api.Assertions.*;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;

/**
 * Unit tests for {@link QueryOptions}.
 *
 * @author Mark Paluch
 * @author Tomasz Lelek
 */
class QueryOptionsUnitTests {

	@Test // DATACASS-202
	void buildQueryOptions() {

		QueryOptions queryOptions = QueryOptions.builder() //
				.consistencyLevel(DefaultConsistencyLevel.ANY) //
				.timeout(Duration.ofSeconds(1)) //
				.pageSize(10) //
				.tracing(true) //
				.keyspace(CqlIdentifier.fromCql("ks1")) //
				.build();

		assertThat(queryOptions.getClass()).isEqualTo(QueryOptions.class);
		assertThat(queryOptions.getConsistencyLevel()).isEqualTo(DefaultConsistencyLevel.ANY);
		assertThat(queryOptions.getTimeout()).isEqualTo(Duration.ofSeconds(1));
		assertThat(queryOptions.getPageSize()).isEqualTo(10);
		assertThat(queryOptions.getTracing()).isTrue();
		assertThat(queryOptions.getKeyspace()).isEqualTo(CqlIdentifier.fromCql("ks1"));
	}

	@Test // DATACASS-56
	void buildQueryOptionsMutate() {

		QueryOptions queryOptions = QueryOptions.builder() //
				.consistencyLevel(DefaultConsistencyLevel.ANY) //
				.timeout(Duration.ofSeconds(1)) //
				.pageSize(10) //
				.tracing(true) //
				.keyspace(CqlIdentifier.fromCql("ks1")) //
				.build();

		QueryOptions mutated = queryOptions.mutate().timeout(Duration.ofSeconds(5)).build();

		assertThat(mutated).isNotNull().isNotSameAs(queryOptions);
		assertThat(mutated.getClass()).isEqualTo(QueryOptions.class);
		assertThat(mutated.getConsistencyLevel()).isEqualTo(DefaultConsistencyLevel.ANY);
		assertThat(mutated.getTimeout()).isEqualTo(Duration.ofSeconds(5));
		assertThat(mutated.getPageSize()).isEqualTo(10);
		assertThat(mutated.getTracing()).isTrue();
		assertThat(mutated.getKeyspace()).isEqualTo(CqlIdentifier.fromCql("ks1"));
	}
}
