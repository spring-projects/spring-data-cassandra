/*
 * Copyright 2016-2021 the original author or authors.
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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.reactivestreams.Publisher;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.cassandra.ReactiveResultSet;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.ReactiveCassandraOperations;
import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.repository.query.ReactiveCassandraQueryExecution.CollectionExecution;
import org.springframework.data.cassandra.repository.query.ReactiveCassandraQueryExecution.ExistsExecution;
import org.springframework.data.cassandra.repository.query.ReactiveCassandraQueryExecution.ResultProcessingConverter;
import org.springframework.data.cassandra.repository.query.ReactiveCassandraQueryExecution.ResultProcessingExecution;
import org.springframework.data.cassandra.repository.query.ReactiveCassandraQueryExecution.SingleEntityExecution;
import org.springframework.data.cassandra.repository.query.ReactiveCassandraQueryExecution.SlicedExecution;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.ResultProcessor;
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;

/**
 * Base class for reactive {@link RepositoryQuery} implementations for Cassandra.
 *
 * @author Mark Paluch
 * @author Hleb Albau
 * @see org.springframework.data.cassandra.repository.query.CassandraRepositoryQuerySupport
 * @since 2.0
 */
public abstract class AbstractReactiveCassandraQuery extends CassandraRepositoryQuerySupport {

	private final ReactiveCassandraOperations operations;

	/**
	 * Create a new {@link AbstractReactiveCassandraQuery} from the given {@link CassandraQueryMethod} and
	 * {@link CassandraOperations}.
	 *
	 * @param method must not be {@literal null}.
	 * @param operations must not be {@literal null}.
	 */
	public AbstractReactiveCassandraQuery(ReactiveCassandraQueryMethod method, ReactiveCassandraOperations operations) {

		super(method, getRequiredMappingContext(operations));

		this.operations = operations;
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
		return Flux.defer(() -> executeLater(parameters));
	}

	private Publisher<Object> executeLater(Object[] parameters) {

		ReactiveCassandraParameterAccessor parameterAccessor = new ReactiveCassandraParameterAccessor(getQueryMethod(),
				parameters);

		CassandraParameterAccessor convertingParameterAccessor = new ConvertingParameterAccessor(
				getRequiredConverter(getReactiveCassandraOperations()), parameterAccessor);

		Mono<SimpleStatement> statement = createQuery(convertingParameterAccessor);

		ResultProcessor resultProcessor = getQueryMethod().getResultProcessor()
				.withDynamicProjection(convertingParameterAccessor);

		ReactiveCassandraQueryExecution queryExecution = getExecution(parameterAccessor, new ResultProcessingConverter(
				resultProcessor, getRequiredMappingContext(getReactiveCassandraOperations()), getEntityInstantiators()));

		Class<?> resultType = resolveResultType(resultProcessor);

		return statement.flatMapMany(it -> queryExecution.execute(it, resultType));
	}

	private Class<?> resolveResultType(ResultProcessor resultProcessor) {

		CassandraReturnedType returnedType = new CassandraReturnedType(resultProcessor.getReturnedType(),
				getRequiredConverter(getReactiveCassandraOperations()).getCustomConversions());

		return (returnedType.isProjecting() ? returnedType.getDomainType() : returnedType.getReturnedType());
	}

	/**
	 * Creates a string query using the given {@link ParameterAccessor}
	 *
	 * @param accessor must not be {@literal null}.
	 */
	protected abstract Mono<SimpleStatement> createQuery(CassandraParameterAccessor accessor);

	protected ReactiveCassandraOperations getReactiveCassandraOperations() {
		return this.operations;
	}

	/**
	 * Returns the execution instance to use.
	 *
	 * @param parameterAccessor must not be {@literal null}.
	 * @param resultProcessing must not be {@literal null}.
	 */
	private ReactiveCassandraQueryExecution getExecution(ReactiveCassandraParameterAccessor parameterAccessor,
			Converter<Object, Object> resultProcessing) {
		return new ResultProcessingExecution(getExecutionToWrap(parameterAccessor), resultProcessing);
	}

	private ReactiveCassandraQueryExecution getExecutionToWrap(CassandraParameterAccessor parameterAccessor) {

		if (getQueryMethod().isSliceQuery()) {
			return new SlicedExecution(getReactiveCassandraOperations(), parameterAccessor.getPageable());
		} else if (getQueryMethod().isCollectionQuery()) {
			return new CollectionExecution(getReactiveCassandraOperations());
		} else if (isCountQuery()) {
			return ((statement, type) -> new SingleEntityExecution(getReactiveCassandraOperations(), false).execute(statement,
					Long.class));
		} else if (isExistsQuery()) {
			return new ExistsExecution(getReactiveCassandraOperations());
		} else if (isModifyingQuery()) {

			return (statement, type) -> getReactiveCassandraOperations().execute(statement)
					.map(ReactiveResultSet::wasApplied);
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

	/**
	 * Returns whether the query is a modifying query.
	 *
	 * @return a boolean value indicating whether the query is a modifying query.
	 * @since 2.2
	 */
	protected abstract boolean isModifyingQuery();

	private static CassandraConverter getRequiredConverter(ReactiveCassandraOperations operations) {

		Assert.notNull(operations, "ReactiveCassandraOperations must not be null");

		return operations.getConverter();
	}

	private static CassandraMappingContext getRequiredMappingContext(ReactiveCassandraOperations operations) {
		return getRequiredConverter(operations).getMappingContext();
	}
}
