/*
 * Copyright 2016-2018 the original author or authors.
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

import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.repository.Query;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import com.datastax.driver.core.SimpleStatement;

/**
 * String-based {@link AbstractCassandraQuery} implementation.
 * <p>
 * A {@link StringBasedCassandraQuery} expects a query method to be annotated with
 * {@link org.springframework.data.cassandra.repository.Query} with a CQL query. String-based queries support named,
 * index-based and expression parameters that are resolved during query execution.
 *
 * @author Matthew Adams
 * @author Mark Paluch
 * @see org.springframework.data.cassandra.repository.Query
 * @see org.springframework.data.cassandra.repository.query.AbstractCassandraQuery
 */
public class StringBasedCassandraQuery extends AbstractCassandraQuery {

	private static final String COUNT_AND_EXISTS = "Manually defined query for %s cannot be a count and exists query at the same time!";

	private final StringBasedQuery stringBasedQuery;

	private final boolean isCountQuery;

	private final boolean isExistsQuery;

	/**
	 * Create a new {@link StringBasedCassandraQuery} for the given {@link CassandraQueryMethod},
	 * {@link CassandraOperations}, {@link SpelExpressionParser}, and {@link QueryMethodEvaluationContextProvider}.
	 *
	 * @param queryMethod {@link CassandraQueryMethod} on which this query is based.
	 * @param operations {@link CassandraOperations} used to perform data access in Cassandra.
	 * @param expressionParser {@link SpelExpressionParser} used to parse expressions in the query.
	 * @param evaluationContextProvider {@link QueryMethodEvaluationContextProvider} used to access
	 * the potentially shared {@link org.springframework.expression.spel.support.StandardEvaluationContext}.
	 * @see org.springframework.data.cassandra.repository.query.CassandraQueryMethod
	 * @see org.springframework.data.cassandra.core.CassandraOperations
	 */
	public StringBasedCassandraQuery(CassandraQueryMethod queryMethod, CassandraOperations operations,
			SpelExpressionParser expressionParser, QueryMethodEvaluationContextProvider evaluationContextProvider) {

		this(queryMethod.getRequiredAnnotatedQuery(), queryMethod, operations, expressionParser,
				evaluationContextProvider);
	}

	/**
	 * Create a new {@link StringBasedCassandraQuery} for the given {@code query}, {@link CassandraQueryMethod},
	 * {@link CassandraOperations}, {@link SpelExpressionParser}, and {@link QueryMethodEvaluationContextProvider}.
	 *
	 * @param query {@link String} containing the Apache Cassandra CQL query to execute.
	 * @param method {@link CassandraQueryMethod} on which this query is based.
	 * @param operations {@link CassandraOperations} used to perform data access in Cassandra.
	 * @param expressionParser {@link SpelExpressionParser} used to parse expressions in the query.
	 * @param evaluationContextProvider {@link QueryMethodEvaluationContextProvider} used to access
	 * the potentially shared {@link org.springframework.expression.spel.support.StandardEvaluationContext}.
	 * @see org.springframework.data.cassandra.repository.query.CassandraQueryMethod
	 * @see org.springframework.data.cassandra.core.CassandraOperations
	 */
	public StringBasedCassandraQuery(String query, CassandraQueryMethod method, CassandraOperations operations,
			SpelExpressionParser expressionParser, QueryMethodEvaluationContextProvider evaluationContextProvider) {

		super(method, operations);

		this.stringBasedQuery = new StringBasedQuery(query,
				new ExpressionEvaluatingParameterBinder(expressionParser, evaluationContextProvider));

		if (method.hasAnnotatedQuery()) {

			Query queryAnnotation = method.getQueryAnnotation().orElse(null);

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

	protected StringBasedQuery getStringBasedQuery() {
		return this.stringBasedQuery;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.repository.query.AbstractCassandraQuery#createQuery(org.springframework.data.cassandra.repository.query.CassandraParameterAccessor)
	 */
	@Override
	public SimpleStatement createQuery(CassandraParameterAccessor parameterAccessor) {
		return getQueryStatementCreator().select(getStringBasedQuery(), parameterAccessor);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.repository.query.AbstractCassandraQuery#isCountQuery()
	 */
	@Override
	protected boolean isCountQuery() {
		return this.isCountQuery;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.repository.query.AbstractCassandraQuery#isExistsQuery()
	 */
	@Override
	protected boolean isExistsQuery() {
		return this.isExistsQuery;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.repository.query.AbstractCassandraQuery#isLimiting()
	 */
	@Override
	protected boolean isLimiting() {
		return false;
	}
}
