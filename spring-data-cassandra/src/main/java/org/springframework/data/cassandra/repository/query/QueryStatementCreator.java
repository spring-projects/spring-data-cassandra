/*
 * Copyright 2017-2018 the original author or authors.
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

import java.util.Optional;
import java.util.function.Function;

import lombok.RequiredArgsConstructor;

import org.springframework.data.cassandra.core.StatementFactory;
import org.springframework.data.cassandra.core.cql.QueryOptions;
import org.springframework.data.cassandra.core.cql.QueryOptionsUtil;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.repository.query.QueryCreationException;
import org.springframework.data.repository.query.parser.PartTree;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;

/**
 * Creates {@link Statement}s for {@link CassandraQueryMethod query methods} based on {@link PartTree} and
 * {@link StringBasedQuery}.
 *
 * @author Mark Paluch
 * @since 2.0
 */
@RequiredArgsConstructor
class QueryStatementCreator {

	private static final Logger LOG = LoggerFactory.getLogger(QueryStatementCreator.class);

	private final CassandraQueryMethod queryMethod;

	private final MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> mappingContext;

	private CassandraPersistentEntity<?> requirePersistentEntity() {
		return this.mappingContext.getRequiredPersistentEntity(this.queryMethod.getDomainClass());
	}

	/**
	 * Create a {@literal SELECT} {@link Statement} from a {@link PartTree} and apply query options.
	 *
	 * @param statementFactory must not be {@literal null}.
	 * @param tree must not be {@literal null}.
	 * @param parameterAccessor must not be {@literal null}.
	 * @return the {@literal SELECT} {@link Statement}.
	 */
	Statement select(StatementFactory statementFactory, PartTree tree,
			CassandraParameterAccessor parameterAccessor) {

		Function<Query, Statement> function = query -> {

			RegularStatement statement = statementFactory.select(query, requirePersistentEntity());

			if (LOG.isDebugEnabled()) {
				LOG.debug(String.format("Created query [%s].", statement));
			}

			return statement;
		};

		return doWithQuery(parameterAccessor, tree, function);
	}

	/**
	 * Create a {@literal COUNT} {@link Statement} from a {@link PartTree} and apply query options.
	 *
	 * @param statementFactory must not be {@literal null}.
	 * @param tree must not be {@literal null}.
	 * @param parameterAccessor must not be {@literal null}.
	 * @return the {@literal SELECT} {@link Statement}.
	 * @since 2.1
	 */
	Statement count(StatementFactory statementFactory, PartTree tree, CassandraParameterAccessor parameterAccessor) {

		Function<Query, Statement> function = query -> {

			RegularStatement statement = statementFactory.count(query, requirePersistentEntity());

			if (LOG.isDebugEnabled()) {
				LOG.debug(String.format("Created query [%s].", statement));
			}

			return statement;
		};

		return doWithQuery(parameterAccessor, tree, function);
	}

	/**
	 * Create a {@literal SELECT} {@link Statement} from a {@link PartTree} and apply query options for exists query
	 * execution. Limit results to a single row.
	 *
	 * @param statementFactory must not be {@literal null}.
	 * @param tree must not be {@literal null}.
	 * @param parameterAccessor must not be {@literal null}.
	 * @return the {@literal SELECT} {@link Statement}.
	 * @since 2.1
	 */
	Statement exists(StatementFactory statementFactory, PartTree tree, CassandraParameterAccessor parameterAccessor) {

		Function<Query, Statement> function = query -> {

			RegularStatement statement = statementFactory.select(query.limit(1), requirePersistentEntity());

			if (LOG.isDebugEnabled()) {
				LOG.debug(String.format("Created query [%s].", statement));
			}

			return statement;
		};

		return doWithQuery(parameterAccessor, tree, function);
	}

	/**
	 * A {@link Function} to {@link Query} derived from a {@link PartTree} and apply query options.
	 *
	 * @param parameterAccessor must not be {@literal null}.
	 * @param tree must not be {@literal null}.
	 * @param function callback function must not be {@literal null}.
	 * @return the {@literal SELECT} {@link Statement}.
	 */
	<T> T doWithQuery(CassandraParameterAccessor parameterAccessor, PartTree tree,
			Function<Query, ? extends T> function) {

		CassandraQueryCreator queryCreator =
				new CassandraQueryCreator(tree, parameterAccessor, this.mappingContext);

		Query query = queryCreator.createQuery();

		try {

			if (tree.isLimiting()) {
				query = query.limit(tree.getMaxResults());
			}

			if (allowsFiltering()) {
				query = query.withAllowFiltering();
			}

			Optional<QueryOptions> queryOptions = Optional.ofNullable(parameterAccessor.getQueryOptions());

			if (queryOptions.isPresent()) {
				query = Optional.ofNullable(parameterAccessor.getQueryOptions()).map(query::queryOptions).orElse(query);
			} else if (this.queryMethod.hasConsistencyLevel()) {
				query = query.queryOptions(QueryOptions.builder()
						.consistencyLevel(this.queryMethod.getRequiredAnnotatedConsistencyLevel())
						.build());
			}

			return function.apply(query);
		} catch (RuntimeException cause) {
			throw QueryCreationException.create(this.queryMethod, cause);
		}
	}

	private boolean allowsFiltering() {

		return this.queryMethod.getQueryAnnotation()
			.map(org.springframework.data.cassandra.repository.Query::allowFiltering)
			.orElse(false);
	}

	/**
	 * Create a {@link Statement} from a {@link StringBasedQuery} and apply query options.
	 *
	 * @param stringBasedQuery must not be {@literal null}.
	 * @param parameterAccessor must not be {@literal null}.
	 * @return the {@link Statement}.
	 */
	SimpleStatement select(StringBasedQuery stringBasedQuery, CassandraParameterAccessor parameterAccessor) {

		try {

			SimpleStatement boundQuery = stringBasedQuery.bindQuery(parameterAccessor, this.queryMethod);

			Optional<QueryOptions> queryOptions = Optional.ofNullable(parameterAccessor.getQueryOptions());

			SimpleStatement queryToUse = boundQuery;

			if (queryOptions.isPresent()) {
				queryToUse = Optional.ofNullable(parameterAccessor.getQueryOptions())
						.map(it -> QueryOptionsUtil.addQueryOptions(boundQuery, it))
						.orElse(boundQuery);
			} else if (this.queryMethod.hasConsistencyLevel()) {
				queryToUse.setConsistencyLevel(this.queryMethod.getRequiredAnnotatedConsistencyLevel());
			}

			if (LOG.isDebugEnabled()) {
				LOG.debug(String.format("Created query [%s].", queryToUse));
			}

			return queryToUse;
		} catch (RuntimeException cause) {
			throw QueryCreationException.create(this.queryMethod, cause);
		}
	}
}
