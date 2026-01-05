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

import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.repository.Query;
import org.springframework.data.expression.ValueEvaluationContext;
import org.springframework.data.repository.query.ValueExpressionDelegate;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;

/**
 * String-based {@link AbstractCassandraQuery} implementation.
 * <p>
 * A {@link StringBasedCassandraQuery} expects a query method to be annotated with
 * {@link org.springframework.data.cassandra.repository.Query} with a CQL query. String-based queries support named,
 * index-based and expression parameters that are resolved during query execution.
 *
 * @author Matthew Adams
 * @author Mark Paluch
 * @author Marcin Grzejszczak
 * @see org.springframework.data.cassandra.repository.Query
 * @see org.springframework.data.cassandra.repository.query.AbstractCassandraQuery
 */
public class StringBasedCassandraQuery extends AbstractCassandraQuery {

	private static final String COUNT_AND_EXISTS = "Manually defined query for %s cannot be a count and exists query at the same time";

	private final StringBasedQuery stringBasedQuery;

	private final boolean isCountQuery;

	private final boolean isExistsQuery;

	private final ValueExpressionDelegate valueExpressionDelegate;

	/**
	 * Create a new {@link StringBasedCassandraQuery} for the given {@link CassandraQueryMethod},
	 * {@link CassandraOperations}, {@link ValueExpressionDelegate}.
	 *
	 * @param queryMethod {@link CassandraQueryMethod} on which this query is based.
	 * @param operations {@link CassandraOperations} used to perform data access in Cassandra.
	 * @param valueExpressionDelegate {@link ValueExpressionDelegate} used to parse expressions in the query.
	 * @see org.springframework.data.cassandra.repository.query.CassandraQueryMethod
	 * @see org.springframework.data.cassandra.core.CassandraOperations
	 * @since 4.4
	 */
	public StringBasedCassandraQuery(CassandraQueryMethod queryMethod, CassandraOperations operations,
			ValueExpressionDelegate valueExpressionDelegate) {

		this(queryMethod.getRequiredAnnotatedQuery(), queryMethod, operations, valueExpressionDelegate);
	}

	/**
	 * Create a new {@link StringBasedCassandraQuery} for the given {@code query}, {@link CassandraQueryMethod},
	 * {@link CassandraOperations}, {@link ValueExpressionDelegate}.
	 *
	 * @param query {@link String} containing the Apache Cassandra CQL query to execute.
	 * @param method {@link CassandraQueryMethod} on which this query is based.
	 * @param operations {@link CassandraOperations} used to perform data access in Cassandra.
	 * @param valueExpressionDelegate {@link ValueExpressionDelegate} used to parse expressions in the query.
	 * @see org.springframework.data.cassandra.repository.query.CassandraQueryMethod
	 * @see org.springframework.data.cassandra.core.CassandraOperations
	 * @since 4.4
	 */
	public StringBasedCassandraQuery(String query, CassandraQueryMethod method, CassandraOperations operations,
			ValueExpressionDelegate valueExpressionDelegate) {

		super(method, operations);

		this.valueExpressionDelegate = valueExpressionDelegate;

		this.stringBasedQuery = new StringBasedQuery(query, method.getParameters(), valueExpressionDelegate);

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
	public SimpleStatement createQuery(CassandraParameterAccessor parameterAccessor) {

		StringBasedQuery query = getStringBasedQuery();
		ConvertingParameterAccessor parameterAccessorToUse = new ConvertingParameterAccessor(getOperations().getConverter(),
				parameterAccessor);
		ValueEvaluationContext evaluationContext = valueExpressionDelegate
				.createValueContextProvider(getQueryMethod().getParameters())
				.getEvaluationContext(parameterAccessorToUse.getValues(), query.getExpressionDependencies());

		return getQueryStatementCreator().select(query, parameterAccessorToUse,
				new ContextualValueExpressionEvaluator(valueExpressionDelegate, evaluationContext));
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

}
