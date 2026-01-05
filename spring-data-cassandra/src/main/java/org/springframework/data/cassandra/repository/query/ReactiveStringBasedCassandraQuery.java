/*
 * Copyright 2016-present the original author or authors.
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

import reactor.core.publisher.Mono;

import org.springframework.data.cassandra.core.ReactiveCassandraOperations;
import org.springframework.data.cassandra.repository.Query;
import org.springframework.data.expression.ReactiveValueEvaluationContextProvider;
import org.springframework.data.expression.ValueEvaluationContextProvider;
import org.springframework.data.mapping.model.ValueExpressionEvaluator;
import org.springframework.data.repository.query.ValueExpressionDelegate;
import org.springframework.data.spel.ExpressionDependencies;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;

/**
 * String-based {@link AbstractReactiveCassandraQuery} implementation.
 * <p>
 * A {@link ReactiveStringBasedCassandraQuery} expects a query method to be annotated with
 * {@link org.springframework.data.cassandra.repository.Query} with a CQL query. String-based queries support named,
 * index-based and expression parameters that are resolved during query execution.
 *
 * @author Mark Paluch
 * @author Marcin Grzejszczak
 * @see org.springframework.data.cassandra.repository.Query
 * @see org.springframework.data.cassandra.repository.query.AbstractReactiveCassandraQuery
 * @since 2.0
 */
public class ReactiveStringBasedCassandraQuery extends AbstractReactiveCassandraQuery {

	private static final String COUNT_AND_EXISTS = "Manually defined query for %s cannot be a count and exists query at the same time";

	private final StringBasedQuery stringBasedQuery;

	private final boolean isCountQuery;

	private final boolean isExistsQuery;

	private final ValueExpressionDelegate delegate;

	private final ReactiveValueEvaluationContextProvider valueEvaluationContextProvider;

	/**
	 * Create a new {@link ReactiveStringBasedCassandraQuery} for the given {@link CassandraQueryMethod},
	 * {@link ReactiveCassandraOperations}, {@link ValueExpressionDelegate}
	 *
	 * @param queryMethod {@link ReactiveCassandraQueryMethod} on which this query is based.
	 * @param operations {@link ReactiveCassandraOperations} used to perform data access in Cassandra.
	 * @param delegate {@link ValueExpressionDelegate} used to parse expressions in the query.
	 * @see org.springframework.data.cassandra.repository.query.ReactiveCassandraQueryMethod
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations
	 * @since 4.4
	 */
	public ReactiveStringBasedCassandraQuery(ReactiveCassandraQueryMethod queryMethod,
			ReactiveCassandraOperations operations, ValueExpressionDelegate delegate) {

		this(queryMethod.getRequiredAnnotatedQuery(), queryMethod, operations, delegate);
	}

	/**
	 * Create a new {@link ReactiveStringBasedCassandraQuery} for the given {@code query}, {@link CassandraQueryMethod},
	 * {@link ReactiveCassandraOperations}, {@link ValueExpressionDelegate}
	 *
	 * @param method {@link ReactiveCassandraQueryMethod} on which this query is based.
	 * @param operations {@link ReactiveCassandraOperations} used to perform data access in Cassandra.
	 * @param delegate {@link SpelExpressionParser} used to parse expressions in the query.
	 * @see org.springframework.data.cassandra.repository.query.ReactiveCassandraQueryMethod
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations
	 * @since 4.4
	 */
	public ReactiveStringBasedCassandraQuery(String query, ReactiveCassandraQueryMethod method,
			ReactiveCassandraOperations operations, ValueExpressionDelegate delegate) {

		super(method, operations);

		Assert.hasText(query, "Query must not be empty");

		this.delegate = delegate;

		this.stringBasedQuery = new StringBasedQuery(query, method.getParameters(), delegate);

		ValueEvaluationContextProvider valueContextProvider = delegate.createValueContextProvider(method.getParameters());
		Assert.isInstanceOf(ReactiveValueEvaluationContextProvider.class, valueContextProvider,
				"ValueEvaluationContextProvider must be reactive");
		this.valueEvaluationContextProvider = (ReactiveValueEvaluationContextProvider) valueContextProvider;

		if (method.hasAnnotatedQuery()) {

			Query queryAnnotation = method.getRequiredQueryAnnotation();

			this.isCountQuery = queryAnnotation.count();
			this.isExistsQuery = queryAnnotation.exists();

			if (ProjectionUtil.hasAmbiguousProjectionFlags(this.isCountQuery, this.isExistsQuery)) {
				throw new IllegalArgumentException(String.format(COUNT_AND_EXISTS, method));
			}
		} else {
			this.isCountQuery = false;
			this.isExistsQuery = false;
		}
	}

	StringBasedQuery getStringBasedQuery() {
		return this.stringBasedQuery;
	}

	@Override
	public Mono<SimpleStatement> createQuery(CassandraParameterAccessor parameterAccessor) {

		StringBasedQuery query = getStringBasedQuery();
		ConvertingParameterAccessor parameterAccessorToUse = new ConvertingParameterAccessor(
				getReactiveCassandraOperations().getConverter(), parameterAccessor);

		return getValueExpressionEvaluatorLater(query.getExpressionDependencies(), parameterAccessor)
				.map(it -> getQueryStatementCreator().select(query, parameterAccessorToUse, it));
	}

	@Override
	protected boolean isCountQuery() {
		return this.isCountQuery;
	}

	@Override
	protected boolean isExistsQuery() {
		return this.isExistsQuery;
	}

	@Override
	protected boolean isLimiting() {
		return false;
	}

	@Override
	protected boolean isModifyingQuery() {
		return false;
	}

	private Mono<ValueExpressionEvaluator> getValueExpressionEvaluatorLater(ExpressionDependencies dependencies,
			CassandraParameterAccessor accessor) {
		return valueEvaluationContextProvider.getEvaluationContextLater(accessor.getValues(), dependencies)
				.map(evaluationContext -> new ContextualValueExpressionEvaluator(delegate, evaluationContext));
	}
}
