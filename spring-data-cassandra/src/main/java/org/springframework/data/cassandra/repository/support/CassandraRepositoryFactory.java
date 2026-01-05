/*
 * Copyright 2013-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.repository.support;

import java.lang.reflect.Method;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty;
import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.data.cassandra.repository.query.CassandraEntityInformation;
import org.springframework.data.cassandra.repository.query.CassandraQueryMethod;
import org.springframework.data.cassandra.repository.query.PartTreeCassandraQuery;
import org.springframework.data.cassandra.repository.query.StringBasedCassandraQuery;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.RepositoryComposition.RepositoryFragments;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.data.repository.query.CachingValueExpressionDelegate;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.data.repository.query.QueryLookupStrategy.Key;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.ValueExpressionDelegate;
import org.springframework.util.Assert;

/**
 * Factory to create {@link CassandraRepository} instances.
 *
 * @author Alex Shvid
 * @author Matthew T. Adams
 * @author Thomas Darimont
 * @author Mark Paluch
 * @author John Blum
 * @author Chris Bono
 */
public class CassandraRepositoryFactory extends RepositoryFactorySupport {

	private final MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> mappingContext;

	private final CassandraOperations operations;

	private CassandraRepositoryFragmentsContributor fragmentsContributor = CassandraRepositoryFragmentsContributor.DEFAULT;

	/**
	 * Create a new {@link CassandraRepositoryFactory} with the given {@link CassandraOperations}.
	 *
	 * @param operations must not be {@literal null}
	 */
	public CassandraRepositoryFactory(CassandraOperations operations) {

		Assert.notNull(operations, "CassandraOperations must not be null");

		this.operations = operations;
		this.mappingContext = operations.getConverter().getMappingContext();
	}

	/**
	 * Configures the {@link CassandraRepositoryFragmentsContributor} to be used. Defaults to
	 * {@link CassandraRepositoryFragmentsContributor#DEFAULT}.
	 *
	 * @param fragmentsContributor
	 * @since 5.0
	 */
	public void setFragmentsContributor(CassandraRepositoryFragmentsContributor fragmentsContributor) {
		this.fragmentsContributor = fragmentsContributor;
	}

	@Override
	protected ProjectionFactory getProjectionFactory(@Nullable ClassLoader classLoader,
			@Nullable BeanFactory beanFactory) {
		return this.operations.getConverter().getProjectionFactory();
	}

	@Override
	protected Class<?> getRepositoryBaseClass(RepositoryMetadata metadata) {
		return SimpleCassandraRepository.class;
	}

	@Override
	protected Object getTargetRepository(RepositoryInformation information) {

		CassandraEntityInformation<?, ?> entityInformation = getEntityInformation(information);

		return getTargetRepositoryViaReflection(information, entityInformation, operations);
	}

	@Override
	public CassandraEntityInformation<?, ?> getEntityInformation(RepositoryMetadata metadata) {

		CassandraPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(metadata.getDomainType());
		return new MappingCassandraEntityInformation<>(entity, operations.getConverter());
	}

	@Override
	protected Optional<QueryLookupStrategy> getQueryLookupStrategy(@Nullable Key key,
			ValueExpressionDelegate valueExpressionDelegate) {
		return Optional.of(new CassandraQueryLookupStrategy(operations,
				new CachingValueExpressionDelegate(valueExpressionDelegate), mappingContext));
	}

	@Override
	protected RepositoryFragments getRepositoryFragments(RepositoryMetadata metadata) {
		return getRepositoryFragments(metadata, operations);
	}

	/**
	 * Creates {@link RepositoryFragments} based on {@link RepositoryMetadata} to add Cassandra-specific extensions.
	 * Built-in fragment contribution can be customized by configuring {@link CassandraRepositoryFragmentsContributor}.
	 *
	 * @param metadata repository metadata.
	 * @param operations the Cassandra operations manager.
	 * @return {@link RepositoryFragments} to be added to the repository.
	 * @since 5.0
	 */
	protected RepositoryFragments getRepositoryFragments(RepositoryMetadata metadata, CassandraOperations operations) {
		return fragmentsContributor.contribute(metadata, getEntityInformation(metadata), operations);
	}

	private record CassandraQueryLookupStrategy(CassandraOperations operations,
			ValueExpressionDelegate valueExpressionDelegate,
			MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> mappingContext)
			implements
				QueryLookupStrategy {

		@Override
		public RepositoryQuery resolveQuery(Method method, RepositoryMetadata metadata, ProjectionFactory factory,
				NamedQueries namedQueries) {

			CassandraQueryMethod queryMethod = new CassandraQueryMethod(method, metadata, factory, mappingContext);
			String namedQueryName = queryMethod.getNamedQueryName();

			if (namedQueries.hasQuery(namedQueryName)) {
				String namedQuery = namedQueries.getQuery(namedQueryName);
				return new StringBasedCassandraQuery(namedQuery, queryMethod, operations, valueExpressionDelegate);
			} else if (queryMethod.hasAnnotatedQuery()) {
				return new StringBasedCassandraQuery(queryMethod, operations, valueExpressionDelegate);
			} else {
				return new PartTreeCassandraQuery(queryMethod, operations);
			}
		}

	}

}
