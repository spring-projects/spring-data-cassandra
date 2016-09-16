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

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.ReactiveCassandraOperations;
import org.springframework.data.cassandra.repository.query.ReactiveCassandraQueryExecution.CollectionExecution;
import org.springframework.data.cassandra.repository.query.ReactiveCassandraQueryExecution.ResultProcessingConverter;
import org.springframework.data.cassandra.repository.query.ReactiveCassandraQueryExecution.ResultProcessingExecution;
import org.springframework.data.cassandra.repository.query.ReactiveCassandraQueryExecution.SingleEntityExecution;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.ReactiveWrapperConverters;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.ResultProcessor;
import org.springframework.util.Assert;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Base class for reactive {@link RepositoryQuery} implementations for Cassandra.
 *
 * @author Mark Paluch
 * @since 2.0
 */
public abstract class AbstractReactiveCassandraQuery implements RepositoryQuery {

	protected static Logger log = LoggerFactory.getLogger(AbstractReactiveCassandraQuery.class);

	private final CassandraQueryMethod method;
	private final ReactiveCassandraOperations operations;

	/**
	 * Creates a new {@link AbstractReactiveCassandraQuery} from the given {@link CassandraQueryMethod} and
	 * {@link CassandraOperations}.
	 *
	 * @param method must not be {@literal null}.
	 * @param operations must not be {@literal null}.
	 */
	public AbstractReactiveCassandraQuery(CassandraQueryMethod method, ReactiveCassandraOperations operations) {

		Assert.notNull(method, "CassandraQueryMethod must not be null");
		Assert.notNull(operations, "ReactiveCassandraOperations must not be null");

		this.method = method;
		this.operations = operations;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.RepositoryQuery#getQueryMethod()
	 */
	@Override
	public CassandraQueryMethod getQueryMethod() {
		return method;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.RepositoryQuery#execute(java.lang.Object[])
	 */
	@Override
	public Object execute(Object[] parameters) {

		if (hasReactiveWrapperParameter()) {
			return executeDeferred(parameters);
		}

		return execute(new ReactiveCassandraParameterAccessor(method, parameters));
	}

	@SuppressWarnings("unchecked")
	private Object executeDeferred(Object[] parameters) {

		ReactiveCassandraParameterAccessor accessor = new ReactiveCassandraParameterAccessor(method, parameters);

		if (getQueryMethod().isCollectionQuery()) {
			return Flux.defer(() -> (Publisher<Object>) execute(accessor));
		}

		return Mono.defer(() -> (Mono<Object>) execute(accessor));
	}

	private Object execute(CassandraParameterAccessor parameterAccessor) {

		CassandraParameterAccessor convertingParameterAccessor = new ConvertingParameterAccessor(operations.getConverter(),
				parameterAccessor);

		String query = createQuery(convertingParameterAccessor);

		ResultProcessor resultProcessor = method.getResultProcessor().withDynamicProjection(convertingParameterAccessor);

		ReactiveCassandraQueryExecution queryExecution = getExecution(query, convertingParameterAccessor,
				new ResultProcessingConverter(resultProcessor));

		CassandraReturnedType returnedType = new CassandraReturnedType(resultProcessor.getReturnedType(),
				operations.getConverter().getCustomConversions());

		Class<?> resultType = (returnedType.isProjecting() ? returnedType.getDomainType() : returnedType.getReturnedType());

		return queryExecution.execute(query, resultType);
	}

	/**
	 * Returns the execution instance to use.
	 *
	 * @param query must not be {@literal null}.
	 * @param accessor must not be {@literal null}.
	 * @param resultProcessing must not be {@literal null}. @return
	 */
	private ReactiveCassandraQueryExecution getExecution(String query, CassandraParameterAccessor accessor,
			Converter<Object, Object> resultProcessing) {

		return new ResultProcessingExecution(getExecutionToWrap(accessor, resultProcessing), resultProcessing);
	}

	private ReactiveCassandraQueryExecution getExecutionToWrap(CassandraParameterAccessor accessor,
			Converter<Object, Object> resultProcessing) {

		if (method.isCollectionQuery()) {
			return new CollectionExecution(operations);
		} else {
			return new SingleEntityExecution(operations);
		}
	}

	private boolean hasReactiveWrapperParameter() {

		for (CassandraParameters.CassandraParameter cassandraParameter : method.getParameters()) {
			if (ReactiveWrapperConverters.supports(cassandraParameter.getType())) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Creates a string query using the given {@link ParameterAccessor}
	 *
	 * @param accessor must not be {@literal null}.
	 */
	protected abstract String createQuery(CassandraParameterAccessor accessor);
}
