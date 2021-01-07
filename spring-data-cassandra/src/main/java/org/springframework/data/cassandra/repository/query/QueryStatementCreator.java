/*
 * Copyright 2017-2021 the original author or authors.
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
package org.springframework.data.cassandra.repository.query;


import java.util.Optional;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.data.cassandra.core.StatementFactory;
import org.springframework.data.cassandra.core.cql.QueryExtractorDelegate;
import org.springframework.data.cassandra.core.cql.QueryOptions;
import org.springframework.data.cassandra.core.cql.QueryOptionsUtil;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty;
import org.springframework.data.cassandra.core.query.Columns;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.cassandra.repository.Query.Idempotency;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.SpELExpressionEvaluator;
import org.springframework.data.repository.query.QueryCreationException;
import org.springframework.data.repository.query.ResultProcessor;
import org.springframework.data.repository.query.ReturnedType;
import org.springframework.data.repository.query.parser.PartTree;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;

/**
 * Creates {@link Statement}s for {@link CassandraQueryMethod query methods} based on {@link PartTree} and
 * {@link StringBasedQuery}.
 *
 * @author Mark Paluch
 * @since 2.0
 */
class QueryStatementCreator {

	private static final Logger LOG = LoggerFactory.getLogger(QueryStatementCreator.class);

	private final CassandraQueryMethod queryMethod;

	private final MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> mappingContext;

	QueryStatementCreator(CassandraQueryMethod queryMethod,
			MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> mappingContext) {
		this.queryMethod = queryMethod;
		this.mappingContext = mappingContext;
	}

	private CassandraPersistentEntity<?> getPersistentEntity() {
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
	SimpleStatement select(StatementFactory statementFactory, PartTree tree, CassandraParameterAccessor parameterAccessor,
			ResultProcessor processor) {

		Function<Query, SimpleStatement> function = query -> {

			ReturnedType returnedType = processor.withDynamicProjection(parameterAccessor).getReturnedType();

			if (returnedType.needsCustomConstruction()) {

				Columns columns = Columns.from(returnedType.getInputProperties().toArray(new String[0]));
				query = query.columns(columns);
			}

			SimpleStatement statement = statementFactory.select(query, getPersistentEntity()).build();

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
	SimpleStatement count(StatementFactory statementFactory, PartTree tree,
			CassandraParameterAccessor parameterAccessor) {

		Function<Query, SimpleStatement> function = query -> {

			SimpleStatement statement = statementFactory.count(query, getPersistentEntity()).build();

			if (LOG.isDebugEnabled()) {
				LOG.debug(String.format("Created query [%s].", QueryExtractorDelegate.getCql(statement)));
			}

			return statement;
		};

		return doWithQuery(parameterAccessor, tree, function);
	}

	/**
	 * Create a {@literal DELETE} {@link Statement} from a {@link PartTree} and apply query options for delete query
	 * execution.
	 *
	 * @param statementFactory must not be {@literal null}.
	 * @param tree must not be {@literal null}.
	 * @param parameterAccessor must not be {@literal null}.
	 * @return the {@literal DELETE} {@link Statement}.
	 * @since 2.2
	 */
	SimpleStatement delete(StatementFactory statementFactory, PartTree tree,
			CassandraParameterAccessor parameterAccessor) {

		Function<Query, SimpleStatement> function = query -> {

			SimpleStatement statement = statementFactory.delete(query, getPersistentEntity()).build();

			if (LOG.isDebugEnabled()) {
				LOG.debug(String.format("Created query [%s].", QueryExtractorDelegate.getCql(statement)));
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
	SimpleStatement exists(StatementFactory statementFactory, PartTree tree,
			CassandraParameterAccessor parameterAccessor) {

		Function<Query, SimpleStatement> function = query -> {

			SimpleStatement statement = statementFactory.select(query.limit(1), getPersistentEntity()).build();

			if (LOG.isDebugEnabled()) {
				LOG.debug(String.format("Created query [%s].", QueryExtractorDelegate.getCql(statement)));
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

		CassandraQueryCreator queryCreator = new CassandraQueryCreator(tree, parameterAccessor, this.mappingContext);

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
				query = query.queryOptions(
						QueryOptions.builder().consistencyLevel(this.queryMethod.getRequiredAnnotatedConsistencyLevel()).build());
			}

			return function.apply(query);
		} catch (RuntimeException cause) {
			throw QueryCreationException.create(this.queryMethod, cause);
		}
	}

	private boolean allowsFiltering() {
		return this.queryMethod.getQueryAnnotation()
				.map(org.springframework.data.cassandra.repository.Query::allowFiltering).orElse(false);
	}

	/**
	 * Create a {@link Statement} from a {@link StringBasedQuery} and apply query options.
	 *
	 * @param stringBasedQuery must not be {@literal null}.
	 * @param parameterAccessor must not be {@literal null}.
	 * @return the {@link Statement}.
	 */
	SimpleStatement select(StringBasedQuery stringBasedQuery, CassandraParameterAccessor parameterAccessor,
			SpELExpressionEvaluator evaluator) {

		try {

			SimpleStatement boundQuery = stringBasedQuery.bindQuery(parameterAccessor, evaluator);

			Optional<QueryOptions> queryOptions = Optional.ofNullable(parameterAccessor.getQueryOptions());

			SimpleStatement queryToUse = boundQuery;

			if (queryOptions.isPresent()) {
				queryToUse = Optional.ofNullable(parameterAccessor.getQueryOptions())
						.map(it -> QueryOptionsUtil.addQueryOptions(boundQuery, it)).orElse(boundQuery);
			} else if (this.queryMethod.hasConsistencyLevel()) {
				queryToUse = queryToUse.setConsistencyLevel(this.queryMethod.getRequiredAnnotatedConsistencyLevel());
			}

			Idempotency idempotency = this.queryMethod.getIdempotency();
			if (idempotency != Idempotency.UNDEFINED) {
				queryToUse = queryToUse.setIdempotent(idempotency == Idempotency.IDEMPOTENT);
			}

			if (LOG.isDebugEnabled()) {
				LOG.debug(String.format("Created query [%s].", QueryExtractorDelegate.getCql(queryToUse)));
			}

			return queryToUse;
		} catch (RuntimeException cause) {
			throw QueryCreationException.create(this.queryMethod, cause);
		}
	}
}
