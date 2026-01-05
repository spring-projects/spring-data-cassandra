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
import org.springframework.data.cassandra.core.ReactiveCassandraOperations;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.domain.User;
import org.springframework.data.cassandra.repository.query.CassandraEntityInformation;
import org.springframework.data.cassandra.repository.support.MappingCassandraEntityInformation;
import org.springframework.data.cassandra.repository.support.ReactiveCassandraRepositoryFragmentsContributor;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.AbstractRepositoryMetadata;
import org.springframework.data.repository.core.support.RepositoryComposition.RepositoryFragments;
import org.springframework.data.repository.core.support.RepositoryFragment;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link ReactiveCassandraRepositoryFragmentsContributor}.
 *
 * @author Chris Bono
 */
class ReactiveCassandraRepositoryFragmentsContributorUnitTests {

	@Test // GH-3279
	void composedContributorShouldCreateFragments() {

		var mappingContext = new CassandraMappingContext();
		var converter = new MappingCassandraConverter(mappingContext);
		ReactiveCassandraOperations operations = mock();
		when(operations.getConverter()).thenReturn(converter);

		var contributor = ReactiveCassandraRepositoryFragmentsContributor.DEFAULT
				.andThen(MyReactiveCassandraRepositoryFragmentsContributor.INSTANCE)
				.andThen(MyOtherReactiveCassandraRepositoryFragmentsContributor.INSTANCE);

		var fragments = contributor.contribute(
				AbstractRepositoryMetadata.getMetadata(MyUserRepo.class),
				new MappingCassandraEntityInformation<>(mappingContext.getPersistentEntity(User.class), converter),
				operations);

		assertThat(fragments.stream())
				.extracting(RepositoryFragment::getImplementationClass)
				.containsExactly(Optional.of(MyReactiveFragment.class), Optional.of(MyOtherReactiveFragment.class));
	}

	enum MyReactiveCassandraRepositoryFragmentsContributor implements ReactiveCassandraRepositoryFragmentsContributor {

		INSTANCE;

		@Override
		public RepositoryFragments contribute(RepositoryMetadata metadata,
				CassandraEntityInformation<?, ?> entityInformation, ReactiveCassandraOperations operations) {
			return RepositoryFragments.just(new MyReactiveFragment());
		}

		@Override
		public RepositoryFragments describe(RepositoryMetadata metadata) {
			return RepositoryFragments.just(new MyReactiveFragment());
		}
	}

	enum MyOtherReactiveCassandraRepositoryFragmentsContributor implements ReactiveCassandraRepositoryFragmentsContributor {

		INSTANCE;

		@Override
		public RepositoryFragments contribute(RepositoryMetadata metadata,
				CassandraEntityInformation<?, ?> entityInformation, ReactiveCassandraOperations operations) {
			return RepositoryFragments.just(new MyOtherReactiveFragment());
		}

		@Override
		public RepositoryFragments describe(RepositoryMetadata metadata) {
			return RepositoryFragments.just(new MyOtherReactiveFragment());
		}
	}

	static class MyReactiveFragment {

	}

	static class MyOtherReactiveFragment {

	}

	interface MyUserRepo extends Repository<User, Long> {}

}
