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

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import org.springframework.core.MethodParameter;
import org.springframework.core.ResolvableType;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.data.cassandra.core.cql.QueryOptions;
import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.cassandra.repository.query.CassandraParameters.CassandraParameter;
import org.springframework.data.repository.query.Parameter;
import org.springframework.data.repository.query.Parameters;
import org.springframework.data.repository.util.QueryExecutionConverters;
import org.springframework.data.repository.util.ReactiveWrapperConverters;
import org.springframework.data.repository.util.ReactiveWrappers;
import org.springframework.lang.Nullable;

/**
 * Custom extension of {@link Parameters} discovering additional properties of query method parameters.
 *
 * @author Matthew Adams
 * @author Mark Paluch
 */
public class CassandraParameters extends Parameters<CassandraParameters, CassandraParameter> {

	private final @Nullable Integer queryOptionsIndex;

	/**
	 * Create a new {@link CassandraParameters} instance from the given {@link Method}
	 *
	 * @param method must not be {@literal null}.
	 */
	public CassandraParameters(Method method) {

		super(method);

		this.queryOptionsIndex = Arrays.asList(method.getParameterTypes()).indexOf(QueryOptions.class);
	}

	private CassandraParameters(List<CassandraParameter> originals, @Nullable Integer queryOptionsIndex) {

		super(originals);

		this.queryOptionsIndex = queryOptionsIndex;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.Parameters#createParameter(org.springframework.core.MethodParameter)
	 */
	@Override
	protected CassandraParameter createParameter(MethodParameter parameter) {
		return new CassandraParameter(parameter);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.Parameters#createFrom(java.util.List)
	 */
	@Override
	protected CassandraParameters createFrom(List<CassandraParameter> parameters) {
		return new CassandraParameters(parameters, queryOptionsIndex);
	}

	/**
	 * Returns the index of the {@link QueryOptions} parameter to be applied to queries.
	 *
	 * @return
	 * @since 2.0
	 */
	public int getQueryOptionsIndex() {
		return (queryOptionsIndex != null ? queryOptionsIndex : -1);
	}

	/**
	 * Custom {@link Parameter} implementation adding {@link CassandraType} support.
	 *
	 * @author Mark Paluch
	 */
	static class CassandraParameter extends Parameter {

		private final @Nullable CassandraType cassandraType;
		private final Class<?> parameterType;

		CassandraParameter(MethodParameter parameter) {

			super(parameter);

			AnnotatedParameter annotatedParameter = new AnnotatedParameter(parameter);

			if (AnnotatedElementUtils.hasAnnotation(annotatedParameter, CassandraType.class)) {
				this.cassandraType = AnnotatedElementUtils.findMergedAnnotation(annotatedParameter, CassandraType.class);
			} else {
				this.cassandraType = null;
			}

			parameterType = potentiallyUnwrapParameterType(parameter);
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.repository.query.Parameter#isSpecialParameter()
		 */
		@Override
		public boolean isSpecialParameter() {
			return super.isSpecialParameter() || QueryOptions.class.isAssignableFrom(getType());
		}

		/**
		 * Returns the {@link CassandraType} for the declared parameter if specified using
		 * {@link org.springframework.data.cassandra.core.mapping.CassandraType}.
		 *
		 * @return the {@link CassandraType} or {@literal null}.
		 */
		@Nullable
		public CassandraType getCassandraType() {
			return this.cassandraType;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.repository.query.Parameter#getType()
		 */
		@Override
		public Class<?> getType() {
			return this.parameterType;
		}

		/**
		 * Returns the component type if the given {@link MethodParameter} is a wrapper type and the wrapper should be
		 * unwrapped.
		 *
		 * @param parameter must not be {@literal null}.
		 */
		private static Class<?> potentiallyUnwrapParameterType(MethodParameter parameter) {

			Class<?> originalType = parameter.getParameterType();

			if (isWrapped(parameter) && shouldUnwrap(parameter)) {
				return ResolvableType.forMethodParameter(parameter).getGeneric(0).getRawClass();
			}

			return originalType;
		}

		/**
		 * Returns whether the {@link MethodParameter} is wrapped in a wrapper type.
		 *
		 * @param parameter must not be {@literal null}.
		 * @see QueryExecutionConverters
		 */
		private static boolean isWrapped(MethodParameter parameter) {
			return QueryExecutionConverters.supports(parameter.getParameterType())
					|| ReactiveWrapperConverters.supports(parameter.getParameterType());
		}

		/**
		 * Returns whether the {@link MethodParameter} should be unwrapped.
		 *
		 * @param parameter must not be {@literal null}.
		 * @see QueryExecutionConverters
		 */
		private static boolean shouldUnwrap(MethodParameter parameter) {
			return QueryExecutionConverters.supportsUnwrapping(parameter.getParameterType())
					|| ReactiveWrappers.supports(parameter.getParameterType());
		}
	}

	/**
	 * {@link AnnotatedElement} implementation as annotation source for {@link AnnotatedElementUtils}.
	 *
	 * @author Mark Paluch
	 */
	static class AnnotatedParameter implements AnnotatedElement {

		private final MethodParameter methodParameter;

		AnnotatedParameter(MethodParameter methodParameter) {
			this.methodParameter = methodParameter;
		}

		/**
		 * @inheritDoc
		 */
		@Override
		@Nullable
		public <T extends Annotation> T getAnnotation(Class<T> annotationClass) {
			return methodParameter.getParameterAnnotation(annotationClass);
		}

		/**
		 * @inheritDoc
		 */
		@Override
		public Annotation[] getAnnotations() {
			return methodParameter.getParameterAnnotations();
		}

		/**
		 * @inheritDoc
		 */
		@Override
		public Annotation[] getDeclaredAnnotations() {
			return methodParameter.getParameterAnnotations();
		}
	}
}
