/*
 * Copyright 2020-2021 the original author or authors.
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
import java.util.Collections;
import java.util.List;

import org.springframework.data.mapping.model.SpELExpressionEvaluator;
import org.springframework.data.repository.query.Parameter;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Value object capturing the binding context to provide {@link #getBindingValues() binding values} for queries.
 *
 * @author Mark Paluch
 * @since 1.5
 */
class BindingContext {

	private final CassandraParameters parameters;

	private final ParameterAccessor parameterAccessor;

	private final List<ParameterBinding> bindings;

	private final SpELExpressionEvaluator evaluator;

	/**
	 * Create new {@link BindingContext}.
	 */
	public BindingContext(CassandraParameters parameters, ParameterAccessor parameterAccessor,
			List<ParameterBinding> bindings, SpELExpressionEvaluator evaluator) {

		this.parameters = parameters;
		this.parameterAccessor = parameterAccessor;
		this.bindings = bindings;
		this.evaluator = evaluator;
	}

	/**
	 * @return {@literal true} when list of bindings is not empty.
	 */
	private boolean hasBindings() {
		return !bindings.isEmpty();
	}

	/**
	 * Bind values provided by {@link CassandraParameterAccessor} to placeholders in {@link BindingContext} while
	 * considering potential conversions and parameter types.
	 *
	 * @return {@literal null} if given {@code raw} value is empty.
	 */
	public List<Object> getBindingValues() {

		if (!hasBindings()) {
			return Collections.emptyList();
		}

		List<Object> parameters = new ArrayList<>(bindings.size());

		for (ParameterBinding binding : bindings) {
			Object parameterValueForBinding = getParameterValueForBinding(binding);
			parameters.add(parameterValueForBinding);
		}

		return parameters;
	}

	/**
	 * Return the value to be used for the given {@link ParameterBinding}.
	 *
	 * @param binding must not be {@literal null}.
	 * @return the value used for the given {@link ParameterBinding}.
	 */
	@Nullable
	private Object getParameterValueForBinding(ParameterBinding binding) {

		if (binding.isExpression()) {
			return evaluator.evaluate(binding.getRequiredExpression());
		}

		return binding.isNamed()
				? parameterAccessor.getBindableValue(getParameterIndex(parameters, binding.getRequiredParameterName()))
				: parameterAccessor.getBindableValue(binding.getParameterIndex());
	}

	private int getParameterIndex(CassandraParameters parameters, String parameterName) {

		return parameters.stream() //
				.filter(cassandraParameter -> cassandraParameter //
						.getName().filter(s -> s.equals(parameterName)) //
						.isPresent()) //
				.mapToInt(Parameter::getIndex) //
				.findFirst() //
				.orElseThrow(() -> new IllegalArgumentException(
						String.format("Invalid parameter name; Cannot resolve parameter [%s]", parameterName)));
	}

	/**
	 * A generic parameter binding with name or position information.
	 *
	 * @author Mark Paluch
	 */
	static class ParameterBinding {

		private final int parameterIndex;
		private final @Nullable String expression;
		private final @Nullable String parameterName;

		private ParameterBinding(int parameterIndex, @Nullable String expression, @Nullable String parameterName) {

			this.parameterIndex = parameterIndex;
			this.expression = expression;
			this.parameterName = parameterName;
		}

		static ParameterBinding expression(String expression, boolean quoted) {
			return new ParameterBinding(-1, expression, null);
		}

		static ParameterBinding indexed(int parameterIndex) {
			return new ParameterBinding(parameterIndex, null, null);
		}

		static ParameterBinding named(String name) {
			return new ParameterBinding(-1, null, name);
		}

		boolean isNamed() {
			return (parameterName != null);
		}

		int getParameterIndex() {
			return parameterIndex;
		}

		String getParameter() {
			return ("?" + (isExpression() ? "expr" : "") + parameterIndex);
		}

		String getRequiredExpression() {

			Assert.state(expression != null, "ParameterBinding is not an expression");
			return expression;
		}

		boolean isExpression() {
			return (this.expression != null);
		}

		String getRequiredParameterName() {

			Assert.state(parameterName != null, "ParameterBinding is not named");

			return parameterName;
		}
	}
}
