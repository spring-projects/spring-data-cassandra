/*
 * Copyright 2010-2017 the original author or authors.
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

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.repository.query.CassandraQueryExecution.CollectionExecution;
import org.springframework.data.cassandra.repository.query.CassandraQueryExecution.ResultProcessingConverter;
import org.springframework.data.cassandra.repository.query.CassandraQueryExecution.ResultProcessingExecution;
import org.springframework.data.cassandra.repository.query.CassandraQueryExecution.ResultSetQuery;
import org.springframework.data.cassandra.repository.query.CassandraQueryExecution.SingleEntityExecution;
import org.springframework.data.cassandra.repository.query.CassandraQueryExecution.StreamExecution;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.ResultProcessor;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.datastax.driver.core.Statement;

/**
 * Base class for {@link RepositoryQuery} implementations for Cassandra.
 *
 * @author Mark Paluch
 * @author John Blum
 */
public abstract class AbstractCassandraQuery extends CassandraRepositoryQuerySupport {

	private final CassandraOperations operations;

	/**
	 * Create a new {@link AbstractCassandraQuery} from the given {@link CassandraQueryMethod} and
	 * {@link CassandraOperations}.
	 *
	 * @param queryMethod must not be {@literal null}.
	 * @param operations must not be {@literal null}.
	 */
	public AbstractCassandraQuery(CassandraQueryMethod queryMethod, CassandraOperations operations) {

		super(queryMethod);

		Assert.notNull(operations, "CassandraOperations must not be null");

		this.operations = operations;
	}

	protected CassandraOperations getOperations() {
		return this.operations;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.RepositoryQuery#execute(java.lang.Object[])
	 */
	@Nullable
	@Override
	public Object execute(Object[] parameters) {

		CassandraParameterAccessor parameterAccessor = new ConvertingParameterAccessor(getOperations().getConverter(),
				new CassandraParametersParameterAccessor(getQueryMethod(), parameters));

		ResultProcessor resultProcessor = getQueryMethod().getResultProcessor().withDynamicProjection(parameterAccessor);

		Statement statement = createQuery(parameterAccessor);

		CassandraQueryExecution queryExecution = getExecution(new ResultProcessingConverter(resultProcessor,
				getOperations().getConverter().getMappingContext(), getEntityInstantiators()));

		CassandraReturnedType returnedType = new CassandraReturnedType(resultProcessor.getReturnedType(),
				getOperations().getConverter().getCustomConversions());

		Class<?> resultType = (returnedType.isProjecting() ? returnedType.getDomainType() : returnedType.getReturnedType());

		return queryExecution.execute(statement, resultType);
	}

	/**
	 * Creates a {@link Statement} using the given {@link ParameterAccessor}
	 *
	 * @param accessor must not be {@literal null}.
	 */
	protected abstract Statement createQuery(CassandraParameterAccessor accessor);

	/**
	 * Returns the execution instance to use.
	 *
	 * @param resultProcessing must not be {@literal null}. @return
	 */
	private CassandraQueryExecution getExecution(Converter<Object, Object> resultProcessing) {
		return new ResultProcessingExecution(getExecutionToWrap(resultProcessing), resultProcessing);
	}

	private CassandraQueryExecution getExecutionToWrap(Converter<Object, Object> resultProcessing) {

		if (getQueryMethod().isCollectionQuery()) {
			return new CollectionExecution(getOperations());
		} else if (getQueryMethod().isResultSetQuery()) {
			return new ResultSetQuery(getOperations());
		} else if (getQueryMethod().isStreamQuery()) {
			return new StreamExecution(getOperations(), resultProcessing);
		} else {
			return new SingleEntityExecution(getOperations());
		}
	}
}
