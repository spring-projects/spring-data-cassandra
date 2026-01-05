/*
 * Copyright 2025-present the original author or authors.
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
package org.springframework.data.cassandra.support;

import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.domain.User;
import org.springframework.data.cassandra.repository.query.CassandraEntityInformation;
import org.springframework.data.cassandra.repository.support.CassandraRepositoryFragmentsContributor;
import org.springframework.data.cassandra.repository.support.MappingCassandraEntityInformation;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.AbstractRepositoryMetadata;
import org.springframework.data.repository.core.support.RepositoryComposition.RepositoryFragments;
import org.springframework.data.repository.core.support.RepositoryFragment;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link CassandraRepositoryFragmentsContributor}.
 *
 * @author Chris Bono
 */
class CassandraRepositoryFragmentsContributorUnitTests {

	@Test // GH-3279
	void composedContributorShouldCreateFragments() {

		var mappingContext = new CassandraMappingContext();
		var converter = new MappingCassandraConverter(mappingContext);
		CassandraOperations operations = mock();
		when(operations.getConverter()).thenReturn(converter);

		var contributor = CassandraRepositoryFragmentsContributor.DEFAULT
				.andThen(MyCassandraRepositoryFragmentsContributor.INSTANCE)
				.andThen(MyOtherCassandraRepositoryFragmentsContributor.INSTANCE);

		var fragments = contributor.contribute(
				AbstractRepositoryMetadata.getMetadata(MyUserRepo.class),
				new MappingCassandraEntityInformation<>(mappingContext.getPersistentEntity(User.class), converter),
				operations);

		assertThat(fragments.stream())
				.extracting(RepositoryFragment::getImplementationClass)
				.containsExactly(Optional.of(MyFragment.class), Optional.of(MyOtherFragment.class));
	}

	enum MyCassandraRepositoryFragmentsContributor implements CassandraRepositoryFragmentsContributor {

		INSTANCE;

		@Override
		public RepositoryFragments contribute(RepositoryMetadata metadata,
				CassandraEntityInformation<?, ?> entityInformation, CassandraOperations operations) {
			return RepositoryFragments.just(new MyFragment());
		}

		@Override
		public RepositoryFragments describe(RepositoryMetadata metadata) {
			return RepositoryFragments.just(new MyFragment());
		}
	}

	enum MyOtherCassandraRepositoryFragmentsContributor implements CassandraRepositoryFragmentsContributor {

		INSTANCE;

		@Override
		public RepositoryFragments contribute(RepositoryMetadata metadata,
				CassandraEntityInformation<?, ?> entityInformation, CassandraOperations operations) {
			return RepositoryFragments.just(new MyOtherFragment());
		}

		@Override
		public RepositoryFragments describe(RepositoryMetadata metadata) {
			return RepositoryFragments.just(new MyOtherFragment());
		}
	}

	static class MyFragment {

	}

	static class MyOtherFragment {

	}

	interface MyUserRepo extends Repository<User, Long> {}

}
