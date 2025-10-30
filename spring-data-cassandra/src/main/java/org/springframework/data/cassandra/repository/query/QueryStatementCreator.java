/*
 * Copyright 2017-2025 the original author or authors.
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jspecify.annotations.Nullable;

import org.springframework.data.cassandra.core.StatementFactory;
import org.springframework.data.cassandra.core.cql.QueryExtractorDelegate;
import org.springframework.data.cassandra.core.cql.QueryOptions;
import org.springframework.data.cassandra.core.cql.QueryOptions.QueryOptionsBuilder;
import org.springframework.data.cassandra.core.cql.QueryOptionsUtil;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty;
import org.springframework.data.cassandra.core.mapping.SimilarityFunction;
import org.springframework.data.cassandra.core.query.Columns;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.cassandra.repository.Query.Idempotency;
import org.springframework.data.core.PropertyPath;
import org.springframework.data.domain.Limit;
import org.springframework.data.domain.ScoringFunction;
import org.springframework.data.domain.Vector;
import org.springframework.data.domain.VectorScoringFunctions;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.ValueExpressionEvaluator;
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

	private static final Map<ScoringFunction, SimilarityFunction> SIMILARITY_FUNCTIONS = Map.of(
			VectorScoringFunctions.COSINE, SimilarityFunction.COSINE, //
			VectorScoringFunctions.EUCLIDEAN, SimilarityFunction.EUCLIDEAN, //
			VectorScoringFunctions.DOT_PRODUCT, SimilarityFunction.DOT_PRODUCT);

	private static final Log LOG = LogFactory.getLog(QueryStatementCreator.class);

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

			Columns columns = null;
			if (returnedType.needsCustomConstruction()) {
				columns = Columns.from(returnedType.getInputProperties().toArray(new String[0]));
			} else if (queryMethod.isSearchQuery()) {
				columns = getColumns(returnedType.getReturnedType());
			}

			if (columns != null) {

				if (queryMethod.isSearchQuery()) {

					CassandraQueryCreator queryCreator = new CassandraQueryCreator(tree, parameterAccessor, this.mappingContext);

					PropertyPath vectorProperty = queryCreator.getVectorProperty();
					Vector vector = parameterAccessor.getVector();
					SimilarityFunction similarityFunction = getSimilarityFunction(parameterAccessor.getScoringFunction());

					columns = columns.select(vectorProperty.toDotPath(),
							selectorBuilder -> selectorBuilder.similarity(vector).using(similarityFunction).as("\"__score__\""));
				}


				query = query.columns(columns);
			}

			SimpleStatement statement = statementFactory.select(query, getPersistentEntity()).build();

			if (LOG.isDebugEnabled()) {
				LOG.debug(String.format("Created query [%s]", statement));
			}

			return statement;
		};

		return doWithQuery(parameterAccessor, tree, function);
	}

	private Columns getColumns(Class<?> returnedType) {

		CassandraPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(returnedType);
		List<String> names = new ArrayList<>();
		for (CassandraPersistentProperty property : entity) {
			names.add(property.getName());
		}

		return Columns.from(names.toArray(new String[0]));
	}

	private SimilarityFunction getSimilarityFunction(@Nullable ScoringFunction function) {

		if (function == null) {
			throw new IllegalStateException(
					"Cannot determine ScoringFunction. No ScoringFunction, Score/Similarity or bounded Score Range parameters provided.");
		}

		SimilarityFunction similarityFunction = SIMILARITY_FUNCTIONS.get(function);

		if (similarityFunction == null) {
			throw new IllegalArgumentException(
					"Cannot determine SimilarityFunction from ScoreFunction '%s'".formatted(function));
		}

		return similarityFunction;
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
				LOG.debug(String.format("Created query [%s]", QueryExtractorDelegate.getCql(statement)));
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
				LOG.debug(String.format("Created query [%s]", QueryExtractorDelegate.getCql(statement)));
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
				LOG.debug(String.format("Created query [%s]", QueryExtractorDelegate.getCql(statement)));
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
	@SuppressWarnings("NullAway")
	<T> T doWithQuery(CassandraParameterAccessor parameterAccessor, PartTree tree,
			Function<Query, ? extends T> function) {

		CassandraQueryCreator queryCreator = new CassandraQueryCreator(tree, parameterAccessor, this.mappingContext);

		Query query = queryCreator.createQuery();

		try {

			Limit limit = parameterAccessor.getLimit();

			if (!queryMethod.isScrollQuery()) {

				if (limit.isLimited()) {
					query = query.limit(limit);
				}
				if (tree.isLimiting()) {
					query = query.limit(tree.getMaxResults());
				}
			}

			if (allowsFiltering()) {
				query = query.withAllowFiltering();
			}

			Optional<QueryOptions> queryOptions = Optional.ofNullable(parameterAccessor.getQueryOptions());
			QueryOptionsBuilder optionsBuilder = queryOptions.orElseGet(QueryOptions::empty).mutate();

			if (queryMethod.isScrollQuery()) {

				if (limit.isLimited()) {
					optionsBuilder.pageSize(limit.max());
				} else if (tree.isLimiting()) {
					optionsBuilder.pageSize(tree.getMaxResults());
				}
			}

			if (this.queryMethod.hasConsistencyLevel()) {
				optionsBuilder.consistencyLevel(this.queryMethod.getRequiredAnnotatedConsistencyLevel());
			}

			query = query.queryOptions(optionsBuilder.build());

			return function.apply(query);
		} catch (RuntimeException cause) {
			throw QueryCreationException.create(this.queryMethod, cause);
		}
	}

	private boolean allowsFiltering() {

		org.springframework.data.cassandra.repository.Query query = this.queryMethod.getQueryAnnotation();
		return query != null && query.allowFiltering();
	}

	/**
	 * Create a {@link Statement} from a {@link StringBasedQuery} and apply query options.
	 *
	 * @param stringBasedQuery must not be {@literal null}.
	 * @param parameterAccessor must not be {@literal null}.
	 * @return the {@link Statement}.
	 */
	SimpleStatement select(StringBasedQuery stringBasedQuery, CassandraParameterAccessor parameterAccessor,
			ValueExpressionEvaluator evaluator) {

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
				LOG.debug(String.format("Created query [%s]", QueryExtractorDelegate.getCql(queryToUse)));
			}

			return queryToUse;
		} catch (RuntimeException cause) {
			throw QueryCreationException.create(this.queryMethod, cause);
		}
	}
}
