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
package org.springframework.data.cassandra.repository.query;

import static org.assertj.core.api.Assertions.*;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import rx.Single;

import java.lang.reflect.Method;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.domain.Person;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;

/**
 * Unit tests for {@link ReactiveCassandraQueryMethod}.
 *
 * @author Mark Paluch
 */
class ReactiveCassandraQueryMethodUnitTests {

	private CassandraMappingContext context;

	@BeforeEach
	void setUp() {
		context = new CassandraMappingContext();
	}

	@Test // DATACASS-335
	void considersMethodAsStreamQuery() throws Exception {

		ReactiveCassandraQueryMethod queryMethod = queryMethod(SampleRepository.class, "method");

		assertThat(queryMethod.isStreamQuery()).isTrue();
	}

	@Test // DATACASS-335
	void considersMethodAsCollectionQuery() throws Exception {

		ReactiveCassandraQueryMethod queryMethod = queryMethod(SampleRepository.class, "method");

		assertThat(queryMethod.isCollectionQuery()).isTrue();
	}

	@Test // DATACASS-335
	void considersMonoMethodAsEntityQuery() throws Exception {

		ReactiveCassandraQueryMethod queryMethod = queryMethod(SampleRepository.class, "mono");

		assertThat(queryMethod.isCollectionQuery()).isFalse();
		assertThat(queryMethod.isQueryForEntity()).isTrue();
	}

	@Test // DATACASS-335
	void considersSingleMethodAsEntityQuery() throws Exception {

		ReactiveCassandraQueryMethod queryMethod = queryMethod(SampleRepository.class, "single");

		assertThat(queryMethod.isCollectionQuery()).isFalse();
		assertThat(queryMethod.isQueryForEntity()).isTrue();
	}

	private ReactiveCassandraQueryMethod queryMethod(Class<?> repository, String name, Class<?>... parameters)
			throws Exception {

		Method method = repository.getMethod(name, parameters);
		ProjectionFactory factory = new SpelAwareProxyProjectionFactory();
		return new ReactiveCassandraQueryMethod(method, new DefaultRepositoryMetadata(repository), factory, context);
	}

	@SuppressWarnings("unused")
	interface SampleRepository extends Repository<Person, Long> {

		Flux<Person> method();

		Single<Person> single();

		Mono<Person> mono();
	}
}
