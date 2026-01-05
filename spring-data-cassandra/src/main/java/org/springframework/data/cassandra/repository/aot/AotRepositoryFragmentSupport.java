/*
 * Copyright 2025-present the original author or authors.
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
package org.springframework.data.cassandra.repository.aot;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Optional;
import java.util.stream.Stream;

import org.jspecify.annotations.Nullable;

import org.springframework.core.CollectionFactory;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.ConfigurableConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.repository.query.CassandraParameters;
import org.springframework.data.domain.Limit;
import org.springframework.data.domain.Slice;
import org.springframework.data.expression.ValueEvaluationContextProvider;
import org.springframework.data.expression.ValueExpression;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.RepositoryFactoryBeanSupport;
import org.springframework.data.repository.query.ParametersSource;
import org.springframework.data.repository.query.ValueExpressionDelegate;
import org.springframework.data.util.Lazy;
import org.springframework.util.ConcurrentLruCache;

/**
 * Support class for Cassandra AOT repository fragments.
 *
 * @author Chris Bono
 * @since 5.0
 */
public class AotRepositoryFragmentSupport {

	private static final ConversionService CONVERSION_SERVICE;

	static {

		ConfigurableConversionService conversionService = new DefaultConversionService();

		conversionService.removeConvertible(Collection.class, Object.class);
		conversionService.removeConvertible(Object.class, Optional.class);

		CONVERSION_SERVICE = conversionService;
	}

	private final RepositoryMetadata repositoryMetadata;
	private final CassandraOperations cassandraOperations;
	private final CassandraConverter converter;
	private final ProjectionFactory projectionFactory;

	private final Lazy<ConcurrentLruCache<String, ValueExpression>> expressions;

	private final Lazy<ConcurrentLruCache<Method, ValueEvaluationContextProvider>> contextProviders;

	protected AotRepositoryFragmentSupport(CassandraOperations cassandraOperations,
			RepositoryFactoryBeanSupport.FragmentCreationContext context) {
		this(cassandraOperations, context.getRepositoryMetadata(), context.getValueExpressionDelegate(),
				context.getProjectionFactory());
	}

	protected AotRepositoryFragmentSupport(CassandraOperations cassandraOperations, RepositoryMetadata repositoryMetadata,
			ValueExpressionDelegate valueExpressions, ProjectionFactory projectionFactory) {

		this.cassandraOperations = cassandraOperations;
		this.converter = cassandraOperations.getConverter();
		this.repositoryMetadata = repositoryMetadata;
		this.projectionFactory = projectionFactory;

		this.expressions = Lazy.of(() -> new ConcurrentLruCache<>(32, valueExpressions::parse));
		this.contextProviders = Lazy.of(() -> new ConcurrentLruCache<>(32, it -> valueExpressions
				.createValueContextProvider(new CassandraParameters(ParametersSource.of(repositoryMetadata, it)))));
	}

	protected @Nullable Object potentiallyConvertBindingValue(@Nullable Object bindableValue) {

		if (bindableValue == null) {
			return null;
		}

		if (bindableValue instanceof Limit limit) {
			return limit.max();
		}

		return this.converter.convertToColumnType(bindableValue, converter.getColumnTypeResolver().resolve(bindableValue));
	}

	/**
	 * Evaluate a Value Expression.
	 *
	 * @param method
	 * @param expressionString
	 * @param args
	 * @return
	 */
	protected @Nullable Object evaluateExpression(Method method, String expressionString, Object... args) {

		ValueExpression expression = this.expressions.get().get(expressionString);
		ValueEvaluationContextProvider contextProvider = this.contextProviders.get().get(method);

		return potentiallyConvertBindingValue(
				expression.evaluate(contextProvider.getEvaluationContext(args, expression.getExpressionDependencies())));
	}

	protected <T> @Nullable T convertOne(@Nullable Object result, Class<T> projection) {

		if (result == null) {
			return null;
		}

		if (projection.isInstance(result)) {
			return projection.cast(result);
		}

		if (CONVERSION_SERVICE.canConvert(result.getClass(), projection)) {
			return CONVERSION_SERVICE.convert(result, projection);
		}

		return projectionFactory.createProjection(projection, result);
	}

	protected @Nullable Object convertMany(@Nullable Object result, Class<?> projection) {

		if (result == null) {
			return null;
		}

		if (projection.isInstance(result)) {
			return result;
		}

		if (result instanceof Stream<?> stream) {
			return stream.map(it -> convertOne(it, projection));
		}

		if (result instanceof Slice<?> slice) {
			return slice.map(it -> convertOne(it, projection));
		}

		if (result instanceof Collection<?> collection) {

			Collection<@Nullable Object> target = CollectionFactory.createCollection(collection.getClass(),
					collection.size());
			for (Object o : collection) {
				target.add(convertOne(o, projection));
			}

			return target;
		}

		throw new UnsupportedOperationException("Cannot create projection for %s".formatted(result));
	}

}
