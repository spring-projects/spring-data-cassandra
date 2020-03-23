/*
 * Copyright 2010-2020 the original author or authors.
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

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.repository.query.CassandraQueryExecution.CollectionExecution;
import org.springframework.data.cassandra.repository.query.CassandraQueryExecution.ExistsExecution;
import org.springframework.data.cassandra.repository.query.CassandraQueryExecution.ResultProcessingConverter;
import org.springframework.data.cassandra.repository.query.CassandraQueryExecution.ResultProcessingExecution;
import org.springframework.data.cassandra.repository.query.CassandraQueryExecution.ResultSetQuery;
import org.springframework.data.cassandra.repository.query.CassandraQueryExecution.SingleEntityExecution;
import org.springframework.data.cassandra.repository.query.CassandraQueryExecution.SlicedExecution;
import org.springframework.data.cassandra.repository.query.CassandraQueryExecution.StreamExecution;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.ResultProcessor;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;

/**
 * Base class for {@link RepositoryQuery} implementations for Cassandra.
 *
 * @author Mark Paluch
 * @author John Blum
 * @see org.springframework.data.cassandra.repository.query.CassandraRepositoryQuerySupport
 */
public abstract class AbstractCassandraQuery extends CassandraRepositoryQuerySupport {

	private final CassandraOperations operations;


	private static CassandraConverter toConverter(CassandraOperations operations) {

		Assert.notNull(operations, "CassandraOperations must not be null");

		return operations.getConverter();
	}

	private static CassandraMappingContext toMappingContext(CassandraOperations operations) {
		return toConverter(operations).getMappingContext();
	}

	/**
	 * Create a new {@link AbstractCassandraQuery} from the given {@link CassandraQueryMethod} and
	 * {@link CassandraOperations}.
	 *
	 * @param queryMethod must not be {@literal null}.
	 * @param operations must not be {@literal null}.
	 */
	public AbstractCassandraQuery(CassandraQueryMethod queryMethod, CassandraOperations operations) {

		super(queryMethod, toMappingContext(operations));

		this.operations = operations;
	}

	/**
	 * Return a reference to the {@link CassandraOperations} used to execute this Cassandra query.
	 *
	 * @return a reference to the {@link CassandraOperations} used to execute this Cassandra query.
	 * @see org.springframework.data.cassandra.core.CassandraOperations
	 */
	protected CassandraOperations getOperations() {
		return this.operations;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.RepositoryQuery#execute(java.lang.Object[])
	 */
	@Nullable
	@Override
	public Object execute(Object[] parameters) {

		CassandraParameterAccessor parameterAccessor = new ConvertingParameterAccessor(toConverter(getOperations()),
				new CassandraParametersParameterAccessor(getQueryMethod(), parameters));

		ResultProcessor resultProcessor = getQueryMethod().getResultProcessor().withDynamicProjection(parameterAccessor);

		Statement<?> statement = createQuery(parameterAccessor);

		CassandraQueryExecution queryExecution = getExecution(parameterAccessor,
				new ResultProcessingConverter(resultProcessor, toMappingContext(getOperations()), getEntityInstantiators()));

		Class<?> resultType = resolveResultType(resultProcessor);

		return queryExecution.execute(statement, resultType);
	}

	private Class<?> resolveResultType(ResultProcessor resultProcessor) {

		CassandraReturnedType returnedType = new CassandraReturnedType(resultProcessor.getReturnedType(),
				getOperations().getConverter().getCustomConversions());

		return returnedType.isProjecting() ? returnedType.getDomainType() : returnedType.getReturnedType();
	}

	/**
	 * Creates a {@link Statement} using the given {@link ParameterAccessor}
	 *
	 * @param accessor must not be {@literal null}.
	 */
	protected abstract SimpleStatement createQuery(CassandraParameterAccessor accessor);

	/**
	 * Returns the execution instance to use.
	 *
	 * @param parameterAccessor must not be {@literal null}.
	 * @param resultProcessing must not be {@literal null}.
	 * @return a wrapped {@link CassandraQueryExecution} to execute this query method.
	 */
	private CassandraQueryExecution getExecution(CassandraParameterAccessor parameterAccessor,
			Converter<Object, Object> resultProcessing) {

		return new ResultProcessingExecution(getExecutionToWrap(parameterAccessor, resultProcessing), resultProcessing);
	}

	private CassandraQueryExecution getExecutionToWrap(CassandraParameterAccessor parameterAccessor,
			Converter<Object, Object> resultProcessing) {

		if (getQueryMethod().isSliceQuery()) {
			return new SlicedExecution(getOperations(), parameterAccessor.getPageable());
		} else if (getQueryMethod().isCollectionQuery()) {
			return new CollectionExecution(getOperations());
		} else if (getQueryMethod().isResultSetQuery()) {
			return new ResultSetQuery(getOperations());
		} else if (getQueryMethod().isStreamQuery()) {
			return new StreamExecution(getOperations(), resultProcessing);
		} else if (isCountQuery()) {
			return ((statement, type) -> new SingleEntityExecution(getOperations(), false).execute(statement, Long.class));
		} else if (isExistsQuery()) {
			return new ExistsExecution(getOperations());
		} else if (isModifyingQuery()) {
			return ((statement, type) -> getOperations().getCqlOperations().queryForResultSet(statement).wasApplied());
		} else {
			return new SingleEntityExecution(getOperations(), isLimiting());
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
}
