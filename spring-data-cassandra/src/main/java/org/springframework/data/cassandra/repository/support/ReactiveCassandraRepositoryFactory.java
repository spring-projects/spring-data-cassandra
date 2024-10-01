/*
 * Copyright 2016-2024 the original author or authors.
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
package org.springframework.data.cassandra.repository.support;

import java.lang.reflect.Method;
import java.util.Optional;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.data.cassandra.core.ReactiveCassandraOperations;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty;
import org.springframework.data.cassandra.repository.query.CassandraEntityInformation;
import org.springframework.data.cassandra.repository.query.ReactiveCassandraQueryMethod;
import org.springframework.data.cassandra.repository.query.ReactivePartTreeCassandraQuery;
import org.springframework.data.cassandra.repository.query.ReactiveStringBasedCassandraQuery;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.ReactiveRepositoryFactorySupport;
import org.springframework.data.repository.query.CachingValueExpressionDelegate;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.data.repository.query.QueryLookupStrategy.Key;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.ValueExpressionDelegate;
import org.springframework.util.Assert;

/**
 * Factory to create {@link org.springframework.data.cassandra.repository.ReactiveCassandraRepository} instances.
 *
 * @author Mark Paluch
 * @author Marcin Grzejszczak
 * @since 2.0
 */
public class ReactiveCassandraRepositoryFactory extends ReactiveRepositoryFactorySupport {

	private final ReactiveCassandraOperations operations;

	private final MappingContext<? extends CassandraPersistentEntity<?>, ? extends CassandraPersistentProperty> mappingContext;

	/**
	 * Create a new {@link ReactiveCassandraRepositoryFactory} with the given {@link ReactiveCassandraOperations}.
	 *
	 * @param cassandraOperations must not be {@literal null}.
	 */
	public ReactiveCassandraRepositoryFactory(ReactiveCassandraOperations cassandraOperations) {

		Assert.notNull(cassandraOperations, "ReactiveCassandraOperations must not be null");

		this.operations = cassandraOperations;
		this.mappingContext = cassandraOperations.getConverter().getMappingContext();
	}

	@Override
	protected ProjectionFactory getProjectionFactory(ClassLoader classLoader, BeanFactory beanFactory) {
		return this.operations.getConverter().getProjectionFactory();
	}

	@Override
	protected Class<?> getRepositoryBaseClass(RepositoryMetadata metadata) {
		return SimpleReactiveCassandraRepository.class;
	}

	@Override
	protected Object getTargetRepository(RepositoryInformation information) {

		CassandraEntityInformation<?, Object> entityInformation = getEntityInformation(information.getDomainType());

		return getTargetRepositoryViaReflection(information, entityInformation, operations);
	}

	@Override
	protected Optional<QueryLookupStrategy> getQueryLookupStrategy(Key key,
			ValueExpressionDelegate valueExpressionDelegate) {
		return Optional.of(new CassandraQueryLookupStrategy(operations,
				new CachingValueExpressionDelegate(valueExpressionDelegate), mappingContext));
	}

	@SuppressWarnings("unchecked")
	public <T, ID> CassandraEntityInformation<T, ID> getEntityInformation(Class<T> domainClass) {

		CassandraPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(domainClass);

		return new MappingCassandraEntityInformation<>((CassandraPersistentEntity<T>) entity, operations.getConverter());
	}

	/**
	 * {@link QueryLookupStrategy} to create
	 * {@link org.springframework.data.cassandra.repository.query.PartTreeCassandraQuery} instances.
	 *
	 * @author Mark Paluch
	 */
	private record CassandraQueryLookupStrategy(ReactiveCassandraOperations operations, ValueExpressionDelegate delegate,
			MappingContext<? extends CassandraPersistentEntity<?>, ? extends CassandraPersistentProperty> mappingContext)
			implements
				QueryLookupStrategy {

		@Override
		public RepositoryQuery resolveQuery(Method method, RepositoryMetadata metadata, ProjectionFactory factory,
				NamedQueries namedQueries) {

			ReactiveCassandraQueryMethod queryMethod = new ReactiveCassandraQueryMethod(method, metadata, factory,
					mappingContext);

			String namedQueryName = queryMethod.getNamedQueryName();

			if (namedQueries.hasQuery(namedQueryName)) {
				String namedQuery = namedQueries.getQuery(namedQueryName);

				return new ReactiveStringBasedCassandraQuery(namedQuery, queryMethod, operations, delegate);
			} else if (queryMethod.hasAnnotatedQuery()) {
				return new ReactiveStringBasedCassandraQuery(queryMethod, operations, delegate);
			} else {
				return new ReactivePartTreeCassandraQuery(queryMethod, operations);
			}
		}
	}
}
