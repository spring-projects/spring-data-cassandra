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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.reactivestreams.Publisher;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.ReactiveCassandraOperations;
import org.springframework.data.cassandra.repository.query.ReactiveCassandraQueryExecution.CollectionExecution;
import org.springframework.data.cassandra.repository.query.ReactiveCassandraQueryExecution.ResultProcessingConverter;
import org.springframework.data.cassandra.repository.query.ReactiveCassandraQueryExecution.ResultProcessingExecution;
import org.springframework.data.cassandra.repository.query.ReactiveCassandraQueryExecution.SingleEntityExecution;
import org.springframework.data.convert.EntityInstantiators;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.ResultProcessor;
import org.springframework.util.Assert;

import com.datastax.driver.core.Statement;

/**
 * Base class for reactive {@link RepositoryQuery} implementations for Cassandra.
 *
 * @author Mark Paluch
 * @since 2.0
 */
public abstract class AbstractReactiveCassandraQuery implements RepositoryQuery {

	private final ReactiveCassandraQueryMethod method;

	private final ReactiveCassandraOperations operations;

	private final EntityInstantiators instantiators;

	QueryMethodStatementFactory queryMethodStatementFactory;

	/**
	 * Create a new {@link AbstractReactiveCassandraQuery} from the given {@link CassandraQueryMethod} and
	 * {@link CassandraOperations}.
	 *
	 * @param method must not be {@literal null}.
	 * @param operations must not be {@literal null}.
	 */
	public AbstractReactiveCassandraQuery(ReactiveCassandraQueryMethod method, ReactiveCassandraOperations operations) {

		Assert.notNull(method, "ReactiveCassandraQueryMethod must not be null");
		Assert.notNull(operations, "ReactiveCassandraOperations must not be null");

		this.method = method;
		this.operations = operations;
		this.instantiators = new EntityInstantiators();
		this.queryMethodStatementFactory = new QueryMethodStatementFactory(method);
	}

	/* (non-Javadoc) */
	protected EntityInstantiators getEntityInstantiators() {
		return this.instantiators;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.RepositoryQuery#getQueryMethod()
	 */
	@Override
	public ReactiveCassandraQueryMethod getQueryMethod() {
		return this.method;
	}

	/* (non-Javadoc) */
	protected ReactiveCassandraOperations getReactiveCassandraOperations() {
		return this.operations;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.RepositoryQuery#execute(java.lang.Object[])
	 */
	@Override
	public Object execute(Object[] parameters) {

		return (getQueryMethod().hasReactiveWrapperParameter() ? executeDeferred(parameters)
				: execute(new ReactiveCassandraParameterAccessor(getQueryMethod(), parameters)));
	}

	@SuppressWarnings("unchecked")
	private Object executeDeferred(Object[] parameters) {

		ReactiveCassandraParameterAccessor accessor = new ReactiveCassandraParameterAccessor(getQueryMethod(), parameters);

		return (getQueryMethod().isCollectionQuery() ? Flux.defer(() -> (Publisher<Object>) execute(accessor))
				: Mono.defer(() -> (Mono<Object>) execute(accessor)));
	}

	private Object execute(CassandraParameterAccessor parameterAccessor) {

		CassandraParameterAccessor convertingParameterAccessor = new ConvertingParameterAccessor(
				getReactiveCassandraOperations().getConverter(), parameterAccessor);

		Statement statement = createQuery(convertingParameterAccessor);

		ResultProcessor resultProcessor = getQueryMethod().getResultProcessor()
				.withDynamicProjection(convertingParameterAccessor);

		ReactiveCassandraQueryExecution queryExecution = getExecution(new ResultProcessingConverter(resultProcessor,
				getReactiveCassandraOperations().getConverter().getMappingContext(), getEntityInstantiators()));

		CassandraReturnedType returnedType = new CassandraReturnedType(resultProcessor.getReturnedType(),
				getReactiveCassandraOperations().getConverter().getCustomConversions());

		Class<?> resultType = (returnedType.isProjecting() ? returnedType.getDomainType() : returnedType.getReturnedType());

		return queryExecution.execute(statement, resultType);
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

	/* (non-Javadoc) */
	private ReactiveCassandraQueryExecution getExecutionToWrap() {
		return (getQueryMethod().isCollectionQuery() ? new CollectionExecution(getReactiveCassandraOperations())
				: new SingleEntityExecution(getReactiveCassandraOperations()));
	}
}
