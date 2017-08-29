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
package org.springframework.data.cassandra.repository.query;

import lombok.RequiredArgsConstructor;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.cassandra.core.StatementFactory;
import org.springframework.data.cassandra.core.cql.QueryOptions;
import org.springframework.data.cassandra.core.cql.QueryOptionsUtil;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.repository.query.QueryCreationException;
import org.springframework.data.repository.query.parser.PartTree;

import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;

/**
 * @author Mark Paluch
 * @since 2.0
 */
@RequiredArgsConstructor
class QueryMethodStatementFactory {

	private static final Logger LOG = LoggerFactory.getLogger(QueryMethodStatementFactory.class);

	private final CassandraQueryMethod queryMethod;

	Statement select(StatementFactory statementFactory, PartTree tree,
			MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> mappingContext,
			CassandraParameterAccessor parameterAccessor) {

		CassandraQueryCreator queryCreator = new CassandraQueryCreator(tree, parameterAccessor, mappingContext);

		Query query = queryCreator.createQuery();

		try {

			if (tree.isLimiting()) {
				query = query.limit(tree.getMaxResults());
			}

			if (queryMethod.getQueryAnnotation().map(org.springframework.data.cassandra.repository.Query::allowFiltering)
					.orElse(false)) {

				query = query.withAllowFiltering();
			}

			Optional<QueryOptions> queryOptions = Optional.ofNullable(parameterAccessor.getQueryOptions());

			if (queryOptions.isPresent()) {
				query = Optional.ofNullable(parameterAccessor.getQueryOptions()).map(query::queryOptions).orElse(query);
			} else if (queryMethod.hasConsistencyLevel()) {
				query = query.queryOptions(
						QueryOptions.builder().consistencyLevel(queryMethod.getRequiredAnnotatedConsistencyLevel()).build());
			}

			CassandraPersistentEntity<?> persistentEntity = mappingContext
					.getRequiredPersistentEntity(queryMethod.getDomainClass());

			RegularStatement statement = statementFactory.select(query, persistentEntity);

			if (LOG.isDebugEnabled()) {
				LOG.debug(String.format("Created query [%s].", statement));
			}

			return statement;

		} catch (RuntimeException e) {
			throw QueryCreationException.create(queryMethod, e);
		}
	}

	SimpleStatement select(StringBasedQuery stringBasedQuery, CassandraParameterAccessor parameterAccessor) {

		try {

			SimpleStatement boundQuery = stringBasedQuery.bindQuery(parameterAccessor, queryMethod);

			Optional<QueryOptions> queryOptions = Optional.ofNullable(parameterAccessor.getQueryOptions());

			SimpleStatement queryToUse = boundQuery;

			if (queryOptions.isPresent()) {
				queryToUse = Optional.ofNullable(parameterAccessor.getQueryOptions())
						.map(it -> QueryOptionsUtil.addQueryOptions(boundQuery, it)).orElse(boundQuery);
			} else if (queryMethod.hasConsistencyLevel()) {
				queryToUse.setConsistencyLevel(queryMethod.getRequiredAnnotatedConsistencyLevel());
			}

			if (LOG.isDebugEnabled()) {
				LOG.debug(String.format("Created query [%s].", queryToUse));
			}

			return queryToUse;
		} catch (RuntimeException e) {
			throw QueryCreationException.create(queryMethod, e);
		}
	}
}
