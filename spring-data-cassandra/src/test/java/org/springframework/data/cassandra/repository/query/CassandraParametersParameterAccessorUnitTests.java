/*
 * Copyright 2016-2018 the original author or authors.
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
package org.springframework.data.cassandra.repository.query;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Method;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import org.springframework.data.cassandra.core.cql.QueryOptions;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.cassandra.domain.AllPossibleTypes;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;

import org.threeten.bp.LocalDateTime;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.DataType.Name;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;

/**
 * Unit tests for {@link CassandraParametersParameterAccessor}.
 *
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class CassandraParametersParameterAccessorUnitTests {

	@Mock ProjectionFactory projectionFactory;

	RepositoryMetadata metadata = new DefaultRepositoryMetadata(PossibleRepository.class);
	CassandraMappingContext context = new CassandraMappingContext();

	@Test // DATACASS-296, DATACASS-146
	public void returnsCassandraSimpleType() throws Exception {

		Method method = PossibleRepository.class.getMethod("findByFirstname", String.class);
		CassandraParameterAccessor accessor = new CassandraParametersParameterAccessor(getCassandraQueryMethod(method),
				"firstname");

		assertThat(accessor.getDataType(0)).isEqualTo(DataType.varchar());
		assertThat(accessor.getQueryOptions()).isNull();
	}

	@Test // DATACASS-296
	public void shouldReturnNoTypeForComplexTypes() throws Exception {

		Method method = PossibleRepository.class.getMethod("findByBpLocalDateTime", LocalDateTime.class);
		CassandraParameterAccessor accessor = new CassandraParametersParameterAccessor(getCassandraQueryMethod(method),
				LocalDateTime.of(2000, 10, 11, 12, 13, 14));

		assertThat(accessor.getDataType(0)).isNull();
	}

	@Test // DATACASS-296
	public void returnTypeForAnnotatedParameter() throws Exception {

		Method method = PossibleRepository.class.getMethod("findByAnnotatedBpLocalDateTime", LocalDateTime.class);
		CassandraParameterAccessor accessor = new CassandraParametersParameterAccessor(getCassandraQueryMethod(method),
				LocalDateTime.of(2000, 10, 11, 12, 13, 14));

		assertThat(accessor.getDataType(0)).isEqualTo(DataType.date());
	}

	@Test // DATACASS-296
	public void returnTypeForAnnotatedParameterWhenUsingStringValue() throws Exception {

		Method method = PossibleRepository.class.getMethod("findByAnnotatedObject", Object.class);
		CassandraParameterAccessor accessor = new CassandraParametersParameterAccessor(getCassandraQueryMethod(method), "");

		assertThat(accessor.getDataType(0)).isEqualTo(DataType.date());
	}

	@Test // DATACASS-296
	public void returnTypeForAnnotatedParameterWhenUsingNullValue() throws Exception {

		Method method = PossibleRepository.class.getMethod("findByAnnotatedObject", Object.class);
		CassandraParameterAccessor accessor = new CassandraParametersParameterAccessor(getCassandraQueryMethod(method), "");

		assertThat(accessor.getDataType(0)).isEqualTo(DataType.date());
	}

	@Test // DATACASS-146
	public void shouldProvideQueryOptions() throws Exception {

		QueryOptions options = QueryOptions.builder().retryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE).build();

		Method method = PossibleRepository.class.getMethod("findByFirstname", QueryOptions.class, String.class);
		CassandraParameterAccessor accessor = new CassandraParametersParameterAccessor(getCassandraQueryMethod(method),
				options, "firstname");

		assertThat(accessor.getQueryOptions()).isEqualTo(options);
	}

	private CassandraQueryMethod getCassandraQueryMethod(Method method) {
		return new CassandraQueryMethod(method, metadata, projectionFactory, context);
	}

	interface PossibleRepository extends Repository<AllPossibleTypes, Long> {

		List<AllPossibleTypes> findByFirstname(String firstname);

		List<AllPossibleTypes> findByBpLocalDateTime(LocalDateTime dateTime);

		List<AllPossibleTypes> findByAnnotatedBpLocalDateTime(@CassandraType(type = Name.DATE) LocalDateTime dateTime);

		List<AllPossibleTypes> findByAnnotatedObject(@CassandraType(type = Name.DATE) Object dateTime);

		List<AllPossibleTypes> findByFirstname(QueryOptions queryOptions, String firstname);
	}
}
