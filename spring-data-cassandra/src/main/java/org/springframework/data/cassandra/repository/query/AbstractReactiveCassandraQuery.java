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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.reactivestreams.Publisher;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.ReactiveCassandraOperations;
import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.repository.query.ReactiveCassandraQueryExecution.CollectionExecution;
import org.springframework.data.cassandra.repository.query.ReactiveCassandraQueryExecution.ExistsExecution;
import org.springframework.data.cassandra.repository.query.ReactiveCassandraQueryExecution.ResultProcessingConverter;
import org.springframework.data.cassandra.repository.query.ReactiveCassandraQueryExecution.ResultProcessingExecution;
import org.springframework.data.cassandra.repository.query.ReactiveCassandraQueryExecution.SingleEntityExecution;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.ResultProcessor;
import org.springframework.util.Assert;

import com.datastax.driver.core.Statement;

/**
 * Base class for reactive {@link RepositoryQuery} implementations for Cassandra.
 *
 * @author Mark Paluch
 * @see org.springframework.data.cassandra.repository.query.CassandraRepositoryQuerySupport
 * @since 2.0
 */
public abstract class AbstractReactiveCassandraQuery extends CassandraRepositoryQuerySupport {

	private final ReactiveCassandraOperations operations;

	private static CassandraConverter toConverter(ReactiveCassandraOperations operations) {

		Assert.notNull(operations, "ReactiveCassandraOperations must not be null");

		return operations.getConverter();
	}

	private static CassandraMappingContext toMappingContext(ReactiveCassandraOperations operations) {
		return toConverter(operations).getMappingContext();
	}

	/**
	 * Create a new {@link AbstractReactiveCassandraQuery} from the given {@link CassandraQueryMethod} and
	 * {@link CassandraOperations}.
	 *
	 * @param method must not be {@literal null}.
	 * @param operations must not be {@literal null}.
	 */
	public AbstractReactiveCassandraQuery(ReactiveCassandraQueryMethod method, ReactiveCassandraOperations operations) {

		super(method, toMappingContext(operations));

		this.operations = operations;
	}

	protected ReactiveCassandraOperations getReactiveCassandraOperations() {
		return this.operations;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.RepositoryQuery#getQueryMethod()
	 */
	@Override
	public ReactiveCassandraQueryMethod getQueryMethod() {
		return (ReactiveCassandraQueryMethod) super.getQueryMethod();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.RepositoryQuery#execute(java.lang.Object[])
	 */
	@Override
	public Object execute(Object[] parameters) {

		return getQueryMethod().hasReactiveWrapperParameter()
				? executeDeferred(parameters)
				: executeNow(parameters);
	}

	@SuppressWarnings("unchecked")
	private Object executeDeferred(Object[] parameters) {

		return getQueryMethod().isCollectionQuery()
				? Flux.defer(() -> (Publisher<Object>) execute(parameters))
				: Mono.defer(() -> (Mono<Object>) execute(parameters));
	}

	private Object executeNow(Object[] parameters) {

		ReactiveCassandraParameterAccessor parameterAccessor =
				new ReactiveCassandraParameterAccessor(getQueryMethod(), parameters);

		CassandraParameterAccessor convertingParameterAccessor = new ConvertingParameterAccessor(
				toConverter(getReactiveCassandraOperations()), parameterAccessor);

		Statement statement = createQuery(convertingParameterAccessor);

		ResultProcessor resultProcessor = getQueryMethod().getResultProcessor()
				.withDynamicProjection(convertingParameterAccessor);

		ReactiveCassandraQueryExecution queryExecution = getExecution(new ResultProcessingConverter(resultProcessor,
				toMappingContext(getReactiveCassandraOperations()), getEntityInstantiators()));

		Class<?> resultType = resolveResultType(resultProcessor);

		return queryExecution.execute(statement, resultType);
	}

	private Class<?> resolveResultType(ResultProcessor resultProcessor) {

		CassandraReturnedType returnedType = new CassandraReturnedType(resultProcessor.getReturnedType(),
				toConverter(getReactiveCassandraOperations()).getCustomConversions());

		return (returnedType.isProjecting() ? returnedType.getDomainType() : returnedType.getReturnedType());
	}

	/**
	 * Creates a string query using the given {@link ParameterAccessor}
	 *
	 * @param accessor must not be {@literal null}.
	 */
	protected abstract Statement createQuery(CassandraParameterAccessor accessor);

	/**
	 * Returns the execution instance to use.
	 *
	 * @param resultProcessing must not be {@literal null}. @return
	 */
	private ReactiveCassandraQueryExecution getExecution(Converter<Object, Object> resultProcessing) {
		return new ResultProcessingExecution(getExecutionToWrap(), resultProcessing);
	}

	private ReactiveCassandraQueryExecution getExecutionToWrap() {

		if (getQueryMethod().isCollectionQuery()) {
			return new CollectionExecution(getReactiveCassandraOperations());
		} else if (isCountQuery()) {
			return ((statement, type) ->
			new SingleEntityExecution(getReactiveCassandraOperations(), false).execute(statement, Long.class));
		} else if (isExistsQuery()) {
			return new ExistsExecution(getReactiveCassandraOperations());
		} else {
			return new SingleEntityExecution(getReactiveCassandraOperations(), isLimiting());
		}
	}

	/**
	 * Returns whether the query should get a count projection applied.
	 *
	 * @return a boolean value indicating whether the query is a count projection.
	 * @since 2.1
	 */
	protected abstract boolean isCountQuery();

	/**
	 * Returns whether the query should get an exists projection applied.
	 *
	 * @return a boolean value indicating whether the query is an exists projection.
	 * @since 2.1
	 */
	protected abstract boolean isExistsQuery();

	/**
	 * Return whether the query has an explicit limit set.
	 *
	 * @return a boolean value indicating whether the query has an explicit limit set.
	 * @since 2.0.4
	 */
	protected abstract boolean isLimiting();
}
