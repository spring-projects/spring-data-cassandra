/*
 * Copyright 2016-2017 the original author or authors.
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
import org.springframework.data.repository.query.EvaluationContextProvider;
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
 */
public class StringBasedCassandraQuery extends AbstractCassandraQuery {

	private final StringBasedQuery stringBasedQuery;

	/**
	 * Create a new {@link StringBasedCassandraQuery} for the given {@link CassandraQueryMethod},
	 * {@link CassandraOperations}, {@link SpelExpressionParser}, and {@link EvaluationContextProvider}.
	 *
	 * @param queryMethod {@link CassandraQueryMethod} on which this query is based.
	 * @param operations {@link CassandraOperations} used to perform data access in Cassandra.
	 * @param expressionParser {@link SpelExpressionParser} used to parse expressions in the query.
	 * @param evaluationContextProvider {@link EvaluationContextProvider} used to access the potentially shared
	 *          {@link org.springframework.expression.spel.support.StandardEvaluationContext}.
	 */
	public StringBasedCassandraQuery(CassandraQueryMethod queryMethod, CassandraOperations operations,
			SpelExpressionParser expressionParser, EvaluationContextProvider evaluationContextProvider) {

		this(queryMethod.getRequiredAnnotatedQuery(), queryMethod, operations, expressionParser, evaluationContextProvider);
	}

	/**
	 * Create a new {@link StringBasedCassandraQuery} for the given {@code query}, {@link CassandraQueryMethod},
	 * {@link CassandraOperations}, {@link SpelExpressionParser}, and {@link EvaluationContextProvider}.
	 *
	 * @param query
	 * @param queryMethod {@link CassandraQueryMethod} on which this query is based.
	 * @param operations {@link CassandraOperations} used to perform data access in Cassandra.
	 * @param expressionParser {@link SpelExpressionParser} used to parse expressions in the query.
	 * @param evaluationContextProvider {@link EvaluationContextProvider} used to access the potentially shared
	 *          {@link org.springframework.expression.spel.support.StandardEvaluationContext}.
	 */
	public StringBasedCassandraQuery(String query, CassandraQueryMethod queryMethod, CassandraOperations operations,
			SpelExpressionParser expressionParser, EvaluationContextProvider evaluationContextProvider) {

		super(queryMethod, operations);

		this.stringBasedQuery = new StringBasedQuery(query,
				new ExpressionEvaluatingParameterBinder(expressionParser, evaluationContextProvider));
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
}
