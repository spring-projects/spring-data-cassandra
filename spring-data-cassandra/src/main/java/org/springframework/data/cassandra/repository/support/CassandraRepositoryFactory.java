/*
 * Copyright 2013-2015 the original author or authors
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.repository.support;

import java.io.Serializable;
import java.lang.reflect.Method;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.repository.TypedIdCassandraRepository;
import org.springframework.data.cassandra.repository.query.CassandraEntityInformation;
import org.springframework.data.cassandra.repository.query.CassandraQueryMethod;
import org.springframework.data.cassandra.repository.query.StringBasedCassandraQuery;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.data.repository.query.QueryLookupStrategy.Key;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.util.Assert;

/**
 * Factory to create {@link TypedIdCassandraRepository} instances.
 * 
 * @author Alex Shvid
 * @author Matthew T. Adams
 * @author Thomas Darimont
 */

public class CassandraRepositoryFactory extends RepositoryFactorySupport {

	private final CassandraOperations cassandraOperations;
	private final CassandraMappingContext mappingContext;

	/**
	 * Creates a new {@link CassandraRepositoryFactory} with the given {@link CassandraOperations}.
	 * 
	 * @param cassandraOperations must not be {@literal null}
	 */
	public CassandraRepositoryFactory(CassandraOperations cassandraOperations) {

		Assert.notNull(cassandraOperations);

		this.cassandraOperations = cassandraOperations;
		this.mappingContext = cassandraOperations.getConverter().getMappingContext();

		// TODO: remove when supporting declarative query methods
		setQueryLookupStrategyKey(QueryLookupStrategy.Key.USE_DECLARED_QUERY);
	}

	@Override
	protected Class<?> getRepositoryBaseClass(RepositoryMetadata metadata) {
		return SimpleCassandraRepository.class;
	}

	@Override
	protected Object getTargetRepository(RepositoryInformation information) {

		CassandraEntityInformation<?, Serializable> entityInformation = getEntityInformation(information.getDomainType());
		return getTargetRepositoryViaReflection(information, entityInformation, cassandraOperations);
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T, ID extends Serializable> CassandraEntityInformation<T, ID> getEntityInformation(Class<T> domainClass) {

		CassandraPersistentEntity<?> entity = mappingContext.getPersistentEntity(domainClass);

		if (entity == null) {
			throw new MappingException(String.format("Could not lookup mapping metadata for domain class %s!",
					domainClass.getName()));
		}

		return new MappingCassandraEntityInformation<T, ID>((CassandraPersistentEntity<T>) entity,
				cassandraOperations.getConverter());
	}

	@Override
	protected QueryLookupStrategy getQueryLookupStrategy(Key key) {
		return new CassandraQueryLookupStrategy();
	}

	private class CassandraQueryLookupStrategy implements QueryLookupStrategy {

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.data.repository.query.QueryLookupStrategy#resolveQuery(java.lang.reflect.Method, org.springframework.data.repository.core.RepositoryMetadata, org.springframework.data.projection.ProjectionFactory, org.springframework.data.repository.core.NamedQueries)
		 */
		@Override
		public RepositoryQuery resolveQuery(Method method, RepositoryMetadata metadata, ProjectionFactory factory,
				NamedQueries namedQueries) {

			CassandraQueryMethod queryMethod = new CassandraQueryMethod(method, metadata, factory, mappingContext);
			String namedQueryName = queryMethod.getNamedQueryName();

			if (namedQueries.hasQuery(namedQueryName)) {
				String namedQuery = namedQueries.getQuery(namedQueryName);
				return new StringBasedCassandraQuery(namedQuery, queryMethod, cassandraOperations);
			} else if (queryMethod.hasAnnotatedQuery()) {
				return new StringBasedCassandraQuery(queryMethod, cassandraOperations);
			} else {
				throw new InvalidDataAccessApiUsageException("declarative query methods are a todo");
			}
		}
	}
}
