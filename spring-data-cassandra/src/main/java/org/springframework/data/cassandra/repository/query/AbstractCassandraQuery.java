/*
 * Copyright 2010-2014 the original author or authors.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cassandra.core.converter.ResultSetToBigDecimalConverter;
import org.springframework.cassandra.core.converter.ResultSetToBigIntegerConverter;
import org.springframework.cassandra.core.converter.ResultSetToBooleanConverter;
import org.springframework.cassandra.core.converter.ResultSetToByteBufferConverter;
import org.springframework.cassandra.core.converter.ResultSetToDateConverter;
import org.springframework.cassandra.core.converter.ResultSetToDoubleConverter;
import org.springframework.cassandra.core.converter.ResultSetToFloatConverter;
import org.springframework.cassandra.core.converter.ResultSetToInetAddressConverter;
import org.springframework.cassandra.core.converter.ResultSetToIntegerConverter;
import org.springframework.cassandra.core.converter.ResultSetToListConverter;
import org.springframework.cassandra.core.converter.ResultSetToLongConverter;
import org.springframework.cassandra.core.converter.ResultSetToStringConverter;
import org.springframework.cassandra.core.converter.ResultSetToUuidConverter;
import org.springframework.cassandra.core.converter.RowToMapConverter;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.support.ConfigurableConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

/**
 * Base class for {@link RepositoryQuery} implementations for Cassandra.
 */
public abstract class AbstractCassandraQuery implements RepositoryQuery {

	protected static final Converter<?, ?>[] DEFAULT_CONVERTERS = new Converter<?, ?>[] { new ResultSetToListConverter(),
			new ResultSetToStringConverter(), new RowToMapConverter(), new ResultSetToBigDecimalConverter(),
			new ResultSetToBigIntegerConverter(), new ResultSetToBooleanConverter(), new ResultSetToByteBufferConverter(),
			new ResultSetToDateConverter(), new ResultSetToDoubleConverter(), new ResultSetToFloatConverter(),
			new ResultSetToInetAddressConverter(), new ResultSetToIntegerConverter(), new ResultSetToLongConverter(),
			new ResultSetToUuidConverter() };

	protected static Logger log = LoggerFactory.getLogger(AbstractCassandraQuery.class);

	private ConversionService conversionService;

	Converter<ResultSet, List<Map<String, Object>>> resultSetToListConverter = new ResultSetToListConverter();

	private final CassandraQueryMethod method;
	private final CassandraOperations template;

	protected RowToMapConverter rowToMapConverter = new RowToMapConverter();

	/**
	 * Creates a new {@link AbstractCassandraQuery} from the given {@link CassandraQueryMethod} and
	 * {@link CassandraOperations}.
	 * 
	 * @param method must not be {@literal null}.
	 * @param template must not be {@literal null}.
	 */
	public AbstractCassandraQuery(CassandraQueryMethod method, CassandraOperations operations) {

		Assert.notNull(operations);
		Assert.notNull(method);

		this.method = method;
		this.template = operations;

		this.conversionService = createDefaultConversionService();
	}

	protected ConfigurableConversionService createDefaultConversionService() {

		ConfigurableConversionService conversionService = new DefaultConversionService();

		for (Converter<?, ?> converter : DEFAULT_CONVERTERS) {
			conversionService.addConverter(converter);
		}

		return conversionService;
	}

	@Override
	public CassandraQueryMethod getQueryMethod() {
		return method;
	}

	@Override
	public Object execute(Object[] parameters) {

		CassandraParameterAccessor accessor = new CassandraParametersParameterAccessor(method, parameters);
		String query = createQuery(accessor);

		ResultSet resultSet = template.query(query);

		// return raw result set if requested
		if (method.isResultSetQuery()) {
			return resultSet;
		}

		Class<?> declaredReturnType = method.getReturnType().getType();
		Class<?> returnedUnwrappedObjectType = method.getReturnedObjectType();

		if (method.isSingleEntityQuery()) {
			return getSingleEntity(resultSet, returnedUnwrappedObjectType);
		}

		Object retval = resultSet;

		if (method.isCollectionOfEntityQuery()) {
			retval = getCollectionOfEntity(resultSet, declaredReturnType, returnedUnwrappedObjectType);
		}

		// TODO: support Page & Slice queries

		// if we get this far, let the configured conversion service try to convert the result set
		return conversionService.convert(retval, TypeDescriptor.forObject(retval),
				TypeDescriptor.valueOf(declaredReturnType));
	}

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
		return conversionService;
	}

	public void setConversionService(ConversionService conversionService) {

		Assert.notNull(conversionService);
		this.conversionService = conversionService;
	}

	/**
	 * Creates a string query using the given {@link ParameterAccessor}
	 * 
	 * @param accessor must not be {@literal null}.
	 */
	protected abstract String createQuery(CassandraParameterAccessor accessor);
}
