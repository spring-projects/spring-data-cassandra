/*
 * Copyright 2013-2021 the original author or authors.
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
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.data.repository.query.QueryLookupStrategy.Key;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Factory to create {@link CassandraRepository} instances.
 *
 * @author Alex Shvid
 * @author Matthew T. Adams
 * @author Thomas Darimont
 * @author Mark Paluch
 * @author John Blum
 */
public class CassandraRepositoryFactory extends RepositoryFactorySupport {

	private static final SpelExpressionParser EXPRESSION_PARSER = new SpelExpressionParser();

	private final MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> mappingContext;

	private final CassandraOperations operations;

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

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.core.support.RepositoryFactorySupport#getRepositoryBaseClass(org.springframework.data.repository.core.RepositoryMetadata)
	 */
	@Override
	protected Class<?> getRepositoryBaseClass(RepositoryMetadata metadata) {
		return SimpleCassandraRepository.class;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.core.support.RepositoryFactorySupport#getTargetRepository(org.springframework.data.repository.core.RepositoryInformation)
	 */
	@Override
	protected Object getTargetRepository(RepositoryInformation information) {

		CassandraEntityInformation<?, Object> entityInformation = getEntityInformation(information.getDomainType());

		return getTargetRepositoryViaReflection(information, entityInformation, operations);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.core.support.RepositoryFactorySupport#getEntityInformation(java.lang.Class)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public <T, ID> CassandraEntityInformation<T, ID> getEntityInformation(Class<T> domainClass) {

		CassandraPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(domainClass);

		return new MappingCassandraEntityInformation<>((CassandraPersistentEntity<T>) entity, operations.getConverter());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.core.support.RepositoryFactorySupport#getQueryLookupStrategy(Key, EvaluationContextProvider)
	 */
	@Override
	protected Optional<QueryLookupStrategy> getQueryLookupStrategy(@Nullable Key key,
			QueryMethodEvaluationContextProvider evaluationContextProvider) {

		return Optional.of(new CassandraQueryLookupStrategy(operations, evaluationContextProvider, mappingContext));
	}

	private static class CassandraQueryLookupStrategy implements QueryLookupStrategy {

		private final QueryMethodEvaluationContextProvider evaluationContextProvider;

		private final MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> mappingContext;

		private final CassandraOperations operations;

		private final ExpressionParser expressionParser = new CachingExpressionParser(EXPRESSION_PARSER);

		CassandraQueryLookupStrategy(CassandraOperations operations,
				QueryMethodEvaluationContextProvider evaluationContextProvider,
				MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> mappingContext) {

			this.operations = operations;
			this.evaluationContextProvider = evaluationContextProvider;
			this.mappingContext = mappingContext;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.repository.query.QueryLookupStrategy#resolveQuery(java.lang.reflect.Method, org.springframework.data.repository.core.RepositoryMetadata, org.springframework.data.projection.ProjectionFactory, org.springframework.data.repository.core.NamedQueries)
		 */
		@Override
		public RepositoryQuery resolveQuery(Method method, RepositoryMetadata metadata, ProjectionFactory factory,
				NamedQueries namedQueries) {

			CassandraQueryMethod queryMethod = new CassandraQueryMethod(method, metadata, factory, mappingContext);
			String namedQueryName = queryMethod.getNamedQueryName();

			if (namedQueries.hasQuery(namedQueryName)) {
				String namedQuery = namedQueries.getQuery(namedQueryName);
				return new StringBasedCassandraQuery(namedQuery, queryMethod, operations, expressionParser,
						evaluationContextProvider);
			} else if (queryMethod.hasAnnotatedQuery()) {
				return new StringBasedCassandraQuery(queryMethod, operations, expressionParser, evaluationContextProvider);
			} else {
				return new PartTreeCassandraQuery(queryMethod, operations);
			}
		}
	}
}

