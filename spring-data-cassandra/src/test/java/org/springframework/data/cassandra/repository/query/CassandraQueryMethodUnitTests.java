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

import java.lang.reflect.Method;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.domain.User;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;

/**
 * Unit tests for {@link CassandraQueryMethod}.
 *
 * @author Mark Paluch
 */
class CassandraQueryMethodUnitTests {

	private CassandraMappingContext context;

	@BeforeEach
	void setUp() {
		context = new CassandraMappingContext();
	}

	@Test // DATACASS-7
	void detectsCollectionFromRepoTypeIfReturnTypeNotAssignable() throws Exception {

		CassandraQueryMethod queryMethod = queryMethod(SampleRepository.class, "method");
		CassandraEntityMetadata<?> metadata = queryMethod.getEntityInformation();

		assertThat(metadata.getJavaType()).isAssignableFrom(User.class);
		assertThat(metadata.getTableName().toString()).isEqualTo("users");
	}

	@Test // DATACASS-7
	void rejectsNullMappingContext() throws Exception {

		Method method = SampleRepository.class.getMethod("method");

		assertThatIllegalArgumentException().isThrownBy(
				() ->
		new CassandraQueryMethod(method, new DefaultRepositoryMetadata(SampleRepository.class),
						new SpelAwareProxyProjectionFactory(), null));
	}

	@Test // DATACASS-7
	void considersMethodAsCollectionQuery() throws Exception {

		CassandraQueryMethod queryMethod = queryMethod(SampleRepository.class, "method");

		assertThat(queryMethod.isCollectionQuery()).isTrue();
	}

	@Test // DATACASS-479
	void considersManagedEntityAsEntityInformation() throws Exception {

		CassandraQueryMethod queryMethod = queryMethod(SampleRepository.class, "findAllBy");

		assertThat(queryMethod.getEntityInformation().getJavaType()).isEqualTo(User.class);
	}

	private CassandraQueryMethod queryMethod(Class<?> repository, String name, Class<?>... parameters) throws Exception {

		Method method = repository.getMethod(name, parameters);
		ProjectionFactory factory = new SpelAwareProxyProjectionFactory();
		return new CassandraQueryMethod(method, new DefaultRepositoryMetadata(repository), factory, context);
	}

	@SuppressWarnings("unused")
	interface SampleRepository extends Repository<User, Long> {

		List<User> method();

		UserProjection findAllBy();

	}

	interface UserProjection {

		String getFirstname();

		String getLastname();
	}
}
