/*
 * Copyright 2010-2016 the original author or authors.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.cassandra.convert.CassandraConverter;
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
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

/**
 * Base class for {@link RepositoryQuery} implementations for Cassandra.
 *
 * @author Mark Paluch
 */
public abstract class AbstractCassandraQuery implements RepositoryQuery {

	protected static Logger log = LoggerFactory.getLogger(AbstractCassandraQuery.class);

	private final CassandraQueryMethod method;
	private final CassandraOperations template;

	/**
	 * Creates a new {@link AbstractCassandraQuery} from the given {@link CassandraQueryMethod} and
	 * {@link CassandraOperations}.
	 *
	 * @param method must not be {@literal null}.
	 * @param operations must not be {@literal null}.
	 */
	public AbstractCassandraQuery(CassandraQueryMethod method, CassandraOperations operations) {

		Assert.notNull(operations);
		Assert.notNull(method);

		this.method = method;
		this.template = operations;
	}

	@Override
	public CassandraQueryMethod getQueryMethod() {
		return method;
	}

	@Override
	public Object execute(Object[] parameters) {

		CassandraParameterAccessor accessor = new ConvertingParameterAccessor(template.getConverter(), new CassandraParametersParameterAccessor(method, parameters));
		String query = createQuery(accessor);

		ResultProcessor processor = method.getResultProcessor().withDynamicProjection(accessor);

		CassandraQueryExecution cassandraQueryExecution = getExecution(query, accessor,
				new ResultProcessingConverter(processor));

		return cassandraQueryExecution.execute(query, processor.getReturnedType().getReturnedType());
	}

	/**
	 * Returns the execution instance to use.
	 *
	 * @param query must not be {@literal null}.
	 * @param accessor must not be {@literal null}.
	 * @param resultProcessing must not be {@literal null}. @return
	 */
	private CassandraQueryExecution getExecution(String query, CassandraParameterAccessor accessor,
			Converter<Object, Object> resultProcessing) {

		return new ResultProcessingExecution(getExecutionToWrap(accessor, resultProcessing), resultProcessing);
	}

	private CassandraQueryExecution getExecutionToWrap(CassandraParameterAccessor accessor,
			Converter<Object, Object> resultProcessing) {

		if (method.isCollectionQuery()) {
			return new CollectionExecution(template);
		} else if (method.isResultSetQuery()) {
			return new ResultSetQuery(template);
		} else if (method.isStreamQuery()) {
			return new StreamExecution(template, resultProcessing);
		} else {
			return new SingleEntityExecution(template);
		}
	}

	/**
	 * @param resultSet
	 * @param declaredReturnType
	 * @param returnedUnwrappedObjectType
	 * @return
	 * @deprecated {@link org.springframework.data.cassandra.mapping.CassandraMappingContext} handles type conversion.
	 */
	@Deprecated
	public Object getCollectionOfEntity(ResultSet resultSet, Class<?> declaredReturnType,
			Class<?> returnedUnwrappedObjectType) {

		Collection<Object> results = null;

		if (ClassUtils.isAssignable(SortedSet.class, declaredReturnType)) {
			results = new TreeSet<Object>();
		} else if (ClassUtils.isAssignable(Set.class, declaredReturnType)) {
			results = new HashSet<Object>();
		} else { // List.class, Collection.class, or array
			results = new ArrayList<Object>();
		}

		CassandraConverter converter = template.getConverter();
		for (Row row : resultSet) {
			results.add(converter.read(returnedUnwrappedObjectType, row));
		}

		return results;
	}

	/**
	 * @param resultSet
	 * @param type
	 * @return
	 * @deprecated {@link org.springframework.data.cassandra.mapping.CassandraMappingContext} handles type conversion.
	 */
	@Deprecated
	public Object getSingleEntity(ResultSet resultSet, Class<?> type) {
		if (resultSet.isExhausted()) {
			return null;
		}

		Iterator<Row> iterator = resultSet.iterator();
		Object object = template.getConverter().read(type, iterator.next());

		warnIfMoreResults(iterator);

		return object;
	}

	protected void warnIfMoreResults(Iterator<Row> iterator) {
		if (log.isWarnEnabled() && iterator.hasNext()) {

			int i = 0;
			while (iterator.hasNext()) {
				iterator.next();
				i++;
			}

			log.warn("ignoring extra {} row{}", i, i == 1 ? "" : "s");
		}
	}

	public ConversionService getConversionService() {
		return template.getConverter().getConversionService();
	}

	/**
	 * @param conversionService
	 * @deprecated {@link org.springframework.data.cassandra.mapping.CassandraMappingContext} handles type conversion.
	 */
	@Deprecated
	public void setConversionService(ConversionService conversionService) {
		throw new UnsupportedOperationException("setConversionService(ConversionService) is not supported anymore. "
				+ "Please use CassandraMappingContext instead");
	}

	/**
	 * Creates a string query using the given {@link ParameterAccessor}
	 *
	 * @param accessor must not be {@literal null}.
	 */
	protected abstract String createQuery(CassandraParameterAccessor accessor);
}
