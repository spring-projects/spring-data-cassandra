/*
 * Copyright 2016-2025 the original author or authors.
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

import org.jspecify.annotations.Nullable;

import org.springframework.core.MethodParameter;
import org.springframework.core.ResolvableType;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.data.cassandra.core.cql.QueryOptions;
import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.cassandra.repository.query.CassandraParameters.CassandraParameter;
import org.springframework.data.core.ReactiveWrappers;
import org.springframework.data.core.TypeInformation;
import org.springframework.data.domain.Range;
import org.springframework.data.domain.Score;
import org.springframework.data.domain.ScoringFunction;
import org.springframework.data.repository.query.Parameter;
import org.springframework.data.repository.query.Parameters;
import org.springframework.data.repository.query.ParametersSource;
import org.springframework.data.repository.util.QueryExecutionConverters;
import org.springframework.data.repository.util.ReactiveWrapperConverters;

/**
 * Custom extension of {@link Parameters} discovering additional properties of query method parameters.
 *
 * @author Matthew Adams
 * @author Mark Paluch
 */
public class CassandraParameters extends Parameters<CassandraParameters, CassandraParameter> {

	private final @Nullable Integer queryOptionsIndex;
	private final @Nullable Integer scoringFunctionIndex;

	/**
	 * Create a new {@link CassandraParameters} instance from the given {@link Method}.
	 *
	 * @param parametersSource must not be {@literal null}.
	 */
	public CassandraParameters(ParametersSource parametersSource) {
		super(parametersSource,
				methodParameter -> new CassandraParameter(methodParameter, parametersSource.getDomainTypeInformation()));

		this.queryOptionsIndex = Arrays.asList(parametersSource.getMethod().getParameterTypes())
				.indexOf(QueryOptions.class);

		this.scoringFunctionIndex = Arrays.asList(parametersSource.getMethod().getParameterTypes())
				.indexOf(ScoringFunction.class);
	}

	private CassandraParameters(List<CassandraParameter> originals, @Nullable Integer queryOptionsIndex,
			@Nullable Integer scoringFunctionIndex) {

		super(originals);

		this.queryOptionsIndex = queryOptionsIndex;
		this.scoringFunctionIndex = scoringFunctionIndex;
	}

	@Override
	protected CassandraParameters createFrom(List<CassandraParameter> parameters) {
		return new CassandraParameters(parameters, queryOptionsIndex, scoringFunctionIndex);
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
	 * Returns the index of the {@link ScoringFunction} parameter to be applied to queries.
	 *
	 * @return
	 * @since 5.0
	 */
	public int getScoringFunctionIndex() {
		return (scoringFunctionIndex != null ? scoringFunctionIndex : -1);
	}

	/**
	 * Custom {@link Parameter} implementation adding {@link CassandraType} support.
	 *
	 * @author Mark Paluch
	 */
	public static class CassandraParameter extends Parameter {

		private final @Nullable CassandraType cassandraType;
		private final Class<?> parameterType;
		private final boolean isScoreRange;
		private final boolean isScoringFunction;

		CassandraParameter(MethodParameter parameter, TypeInformation<?> domainType) {

			super(parameter, domainType);

			AnnotatedParameter annotatedParameter = new AnnotatedParameter(parameter);

			if (AnnotatedElementUtils.hasAnnotation(annotatedParameter, CassandraType.class)) {
				this.cassandraType = AnnotatedElementUtils.findMergedAnnotation(annotatedParameter, CassandraType.class);
			} else {
				this.cassandraType = null;
			}

			this.parameterType = potentiallyUnwrapParameterType(parameter);

			ResolvableType type = ResolvableType.forMethodParameter(parameter);
			this.isScoreRange = Range.class.isAssignableFrom(getType()) && type.getGeneric(0).isAssignableFrom(Score.class);
			this.isScoringFunction = ScoringFunction.class.isAssignableFrom(getType());
		}

		@Override
		public boolean isSpecialParameter() {
			return super.isSpecialParameter() || isScoreRange || isScoringFunction || Score.class.isAssignableFrom(getType())
					|| QueryOptions.class.isAssignableFrom(getType());
		}

		/**
		 * Returns the {@link CassandraType} for the declared parameter if specified using
		 * {@link org.springframework.data.cassandra.core.mapping.CassandraType}.
		 *
		 * @return the {@link CassandraType} or {@literal null}.
		 */
		public @Nullable CassandraType getCassandraType() {
			return this.cassandraType;
		}

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

				Class<?> rawClass = ResolvableType.forMethodParameter(parameter).getGeneric(0).getRawClass();
				return rawClass == null ? Object.class : rawClass;
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
		public <T extends Annotation> @Nullable T getAnnotation(Class<T> annotationClass) {
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
