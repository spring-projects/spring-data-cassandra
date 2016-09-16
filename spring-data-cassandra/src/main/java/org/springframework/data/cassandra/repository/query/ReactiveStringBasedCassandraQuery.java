/*
 * Copyright 2016 the original author or authors.
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cassandra.core.ReactiveSessionCallback;
import org.springframework.data.cassandra.core.ReactiveCassandraOperations;
import org.springframework.data.repository.query.EvaluationContextProvider;
import org.springframework.data.repository.query.QueryCreationException;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.util.Assert;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;

import reactor.core.publisher.Flux;

/**
 * String-based {@link AbstractCassandraQuery} implementation.
 * <p>
 * A {@link ReactiveStringBasedCassandraQuery} expects a query method to be annotated with
 * {@link org.springframework.data.cassandra.repository.Query} with a CQL query. String-based queries support named,
 * index-based and expression parameters that are resolved during query execution.
 *
 * @author Mark Paluch
 * @since 2.0
 * @see org.springframework.data.cassandra.repository.Query
 */
public class ReactiveStringBasedCassandraQuery extends AbstractReactiveCassandraQuery {

	private static final Logger LOG = LoggerFactory.getLogger(ReactiveStringBasedCassandraQuery.class);

	private final StringBasedQuery stringBasedQuery;

	/**
	 * Creates a new {@link ReactiveStringBasedCassandraQuery} for the given {@link CassandraQueryMethod},
	 * {@link ReactiveCassandraOperations}, {@link SpelExpressionParser}, and {@link EvaluationContextProvider}.
	 *
	 * @param queryMethod {@link CassandraQueryMethod} on which this query is based.
	 * @param operations {@link ReactiveCassandraOperations} used to perform data access in Cassandra.
	 * @param expressionParser {@link SpelExpressionParser} used to parse expressions in the query.
	 * @param evaluationContextProvider {@link EvaluationContextProvider} used to access the potentially shared
	 *          {@link org.springframework.expression.spel.support.StandardEvaluationContext}.
	 */
	public ReactiveStringBasedCassandraQuery(CassandraQueryMethod queryMethod, ReactiveCassandraOperations operations,
			SpelExpressionParser expressionParser, EvaluationContextProvider evaluationContextProvider) {
		this(queryMethod.getAnnotatedQuery(), queryMethod, operations, expressionParser, evaluationContextProvider);
	}

	/**
	 * Creates a new {@link ReactiveStringBasedCassandraQuery} for the given {@code query}, {@link CassandraQueryMethod},
	 * {@link ReactiveCassandraOperations}, {@link SpelExpressionParser}, and {@link EvaluationContextProvider}.
	 *
	 * @param queryMethod {@link CassandraQueryMethod} on which this query is based.
	 * @param operations {@link ReactiveCassandraOperations} used to perform data access in Cassandra.
	 * @param expressionParser {@link SpelExpressionParser} used to parse expressions in the query.
	 * @param evaluationContextProvider {@link EvaluationContextProvider} used to access the potentially shared
	 *          {@link org.springframework.expression.spel.support.StandardEvaluationContext}.
	 */
	public ReactiveStringBasedCassandraQuery(String query, CassandraQueryMethod queryMethod,
			ReactiveCassandraOperations operations, SpelExpressionParser expressionParser,
			EvaluationContextProvider evaluationContextProvider) {

		super(queryMethod, operations);

		Assert.hasText(query, "Query must not be empty");

		// this blocking operation is to retrieve the underlying Cluster and does not include any I/O here.
		Cluster cluster = operations.getReactiveCqlOperations()
				.execute((ReactiveSessionCallback<Cluster>) session -> Flux.just(session.getCluster())).blockFirst();

		CodecRegistry codecRegistry = cluster.getConfiguration().getCodecRegistry();

		this.stringBasedQuery = new StringBasedQuery(query,
				new ExpressionEvaluatingParameterBinder(expressionParser, evaluationContextProvider), codecRegistry);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.repository.query.AbstractCassandraQuery#createQuery(org.springframework.data.cassandra.repository.query.CassandraParameterAccessor)
	 */
	@Override
	public String createQuery(CassandraParameterAccessor parameterAccessor) {

		try {
			String boundQuery = stringBasedQuery.bindQuery(parameterAccessor, getQueryMethod());

			if (LOG.isDebugEnabled()) {
				LOG.debug(String.format("Created query [%s].", boundQuery));
			}

			return boundQuery;
		} catch (RuntimeException e) {
			throw QueryCreationException.create(getQueryMethod(), e);
		}
	}
}
