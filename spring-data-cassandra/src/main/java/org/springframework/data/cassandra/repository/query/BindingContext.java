/*
 * Copyright 2020-2025 the original author or authors.
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

import org.jspecify.annotations.Nullable;

import org.springframework.data.mapping.model.ValueExpressionEvaluator;

/**
 * Value object capturing the binding context to provide {@link #getBindingValues() binding values} for queries.
 *
 * @author Mark Paluch
 * @since 1.5
 */
class BindingContext {

	private final CassandraParameters parameters;

	private final CassandraParameterAccessor parameterAccessor;

	private final List<ParameterBinding> bindings;

	private final ValueExpressionEvaluator evaluator;

	/**
	 * Create new {@link BindingContext}.
	 */
	BindingContext(CassandraParameters parameters, CassandraParameterAccessor parameterAccessor,
			List<ParameterBinding> bindings, ValueExpressionEvaluator evaluator) {

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
			parameters.add(binding.prepareValue(parameterValueForBinding));
		}

		return parameters;
	}

	/**
	 * Return the value to be used for the given {@link ParameterBinding}.
	 *
	 * @param binding must not be {@literal null}.
	 * @return the value used for the given {@link ParameterBinding}.
	 */
	private @Nullable Object getParameterValueForBinding(ParameterBinding binding) {

		ParameterBinding.ParameterOrigin origin = binding.getOrigin();

		if (origin.isExpression() && origin instanceof ParameterBinding.Expression expression) {
			return evaluator.evaluate(expression.expression().getExpressionString());
		}

		if (origin instanceof ParameterBinding.MethodInvocationArgument invocationArgument) {

			ParameterBinding.BindingIdentifier argument = invocationArgument.identifier();

			return argument.hasName() ? parameterAccessor.getValue(getParameterIndex(parameters, argument.getName()))
					: parameterAccessor.getBindableValue(argument.getPosition());
		}

		throw new UnsupportedOperationException("Unsupported parameter origin '%s'".formatted(origin));
	}

	private int getParameterIndex(CassandraParameters parameters, String parameterName) {

		for (CassandraParameters.CassandraParameter parameter : parameters) {
			if (parameter.getName().filter(s -> s.equals(parameterName)).isPresent()) {
				return parameter.getIndex();
			}
		}

		throw new IllegalArgumentException(
				String.format("Invalid parameter name; Cannot resolve parameter [%s]", parameterName));
	}

}
