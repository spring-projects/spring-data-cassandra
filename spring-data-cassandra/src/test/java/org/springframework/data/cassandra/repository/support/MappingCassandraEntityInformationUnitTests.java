/*
 * Copyright 2017 the original author or authors.
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
package org.springframework.data.cassandra.repository.support;

import static org.assertj.core.api.Assertions.*;

import lombok.Data;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.domain.AllPossibleTypes;
import org.springframework.data.cassandra.domain.CompositeKey;
import org.springframework.data.cassandra.domain.TypeWithKeyClass;
import org.springframework.data.cassandra.domain.TypeWithMapId;
import org.springframework.data.cassandra.mapping.BasicCassandraMappingContext;
import org.springframework.data.cassandra.mapping.UserTypeResolver;

/**
 * Unit tests for {@link MappingCassandraEntityInformation}.
 *
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings("unchecked")
public class MappingCassandraEntityInformationUnitTests {

	BasicCassandraMappingContext context = new BasicCassandraMappingContext();

	CassandraConverter converter = new MappingCassandraConverter(context);

	@Mock UserTypeResolver userTypeResolver;

	@Before
	public void before() {
		context.setUserTypeResolver(userTypeResolver);
	}

	@Test // DATACASS-420
	public void shouldConsiderSimpleIdEntityAsPrimaryKeyOnly() {

		MappingCassandraEntityInformation information = new MappingCassandraEntityInformation(
				context.getPersistentEntity(PrimaryKeyOnly.class), converter);

		assertThat(information.isPrimaryKeyEntity()).isTrue();
	}

	@Test // DATACASS-420
	public void shouldConsiderCompositeIdEntityAsPrimaryKeyOnly() {

		MappingCassandraEntityInformation information = new MappingCassandraEntityInformation(
				context.getPersistentEntity(CompositeKey.class), converter);

		assertThat(information.isPrimaryKeyEntity()).isTrue();
	}

	@Test // DATACASS-420
	public void shouldConsiderCompositeKeyClassEntityAsPrimaryKeyOnly() {

		MappingCassandraEntityInformation information = new MappingCassandraEntityInformation(
				context.getPersistentEntity(TypeWithKeyClass.class), converter);

		assertThat(information.isPrimaryKeyEntity()).isTrue();
	}

	@Test // DATACASS-420
	public void shouldConsiderMapIdClassEntityAsPrimaryKeyOnly() {

		MappingCassandraEntityInformation information = new MappingCassandraEntityInformation(
				context.getPersistentEntity(TypeWithMapId.class), converter);

		assertThat(information.isPrimaryKeyEntity()).isTrue();
	}

	@Test // DATACASS-420
	public void shouldComplexEntityNotAsPrimaryKeyOnly() {

		MappingCassandraEntityInformation information = new MappingCassandraEntityInformation(
				context.getPersistentEntity(AllPossibleTypes.class), converter);

		assertThat(information.isPrimaryKeyEntity()).isFalse();
	}

	@Data
	static class PrimaryKeyOnly {
		@Id String id;
	}
}
