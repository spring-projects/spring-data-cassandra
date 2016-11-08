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

import java.lang.reflect.Method;
import java.util.List;

import org.springframework.core.MethodParameter;
import org.springframework.core.ResolvableType;
import org.springframework.data.cassandra.mapping.CassandraType;
import org.springframework.data.cassandra.repository.query.CassandraParameters.CassandraParameter;
import org.springframework.data.repository.query.Parameter;
import org.springframework.data.repository.query.Parameters;
import org.springframework.data.repository.util.QueryExecutionConverters;
import org.springframework.data.repository.util.ReactiveWrappers;
import org.springframework.util.Assert;

/**
 * Custom extension of {@link Parameters} discovering additional properties of query method parameters.
 *
 * @author Matthew Adams
 * @author Mark Paluch
 */
public class CassandraParameters extends Parameters<CassandraParameters, CassandraParameter> {

	/**
	 * Creates a new {@link CassandraParameters} instance from the given {@link Method}
	 *
	 * @param method must not be {@literal null}.
	 */
	public CassandraParameters(Method method) {
		super(method);
	}

	private CassandraParameters(List<CassandraParameter> originals) {
		super(originals);
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
		return new CassandraParameters(parameters);
	}

	/**
	 * Custom {@link Parameter} implementation adding {@link CassandraType} support.
	 *
	 * @author Mark Paluch
	 */
	static class CassandraParameter extends Parameter {

		private final CassandraType cassandraType;
		private final Class<?> parameterType;

		protected CassandraParameter(MethodParameter parameter) {

			super(parameter);

			if (parameter.hasParameterAnnotation(CassandraType.class)) {
				CassandraType cassandraType = parameter.getParameterAnnotation(CassandraType.class);

				Assert.notNull(cassandraType.type(),
						String.format("You must specify the type() when annotating method parameters with @%s",
								CassandraType.class.getSimpleName()));

				this.cassandraType = cassandraType;
			} else {
				this.cassandraType = null;
			}

			parameterType = potentiallyUnwrapParameterType(parameter);
		}

		/**
		 * Returns the {@link CassandraType} for the declared parameter if specified using
		 * {@link org.springframework.data.cassandra.mapping.CassandraType}.
		 *
		 * @return the {@link CassandraType} or {@literal null}.
		 */
		public CassandraType getCassandraType() {
			return cassandraType;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.repository.query.Parameter#getType()
		 */
		@Override
		public Class<?> getType() {
			return parameterType;
		}

		/**
		 * Returns the component type if the given {@link MethodParameter} is a wrapper type and the wrapper should be
		 * unwrapped.
		 *
		 * @param parameter must not be {@literal null}.
		 * @return
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
		 * @return
		 * @see QueryExecutionConverters
		 */
		private static boolean isWrapped(MethodParameter parameter) {
			return QueryExecutionConverters.supports(parameter.getParameterType());
		}

		/**
		 * Returns whether the {@link MethodParameter} should be unwrapped.
		 *
		 * @param parameter must not be {@literal null}.
		 * @return
		 * @see QueryExecutionConverters
		 */
		private static boolean shouldUnwrap(MethodParameter parameter) {
			return QueryExecutionConverters.supportsUnwrapping(parameter.getParameterType())
					|| ReactiveWrappers.supports(parameter.getParameterType());
		}
	}
}
