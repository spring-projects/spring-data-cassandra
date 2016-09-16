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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.springframework.data.repository.query.EvaluationContextProvider;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

/**
 * {@link ExpressionEvaluatingParameterBinder} allows to evaluate, convert and bind parameters to placeholders within a
 * {@link String}.
 *
 * @author Mark Paluch
 * @since 1.5
 */
class ExpressionEvaluatingParameterBinder {

	private final SpelExpressionParser expressionParser;
	private final EvaluationContextProvider evaluationContextProvider;

	/**
	 * Creates new {@link ExpressionEvaluatingParameterBinder}
	 *
	 * @param expressionParser must not be {@literal null}.
	 * @param evaluationContextProvider must not be {@literal null}.
	 */
	public ExpressionEvaluatingParameterBinder(SpelExpressionParser expressionParser,
			EvaluationContextProvider evaluationContextProvider) {

		Assert.notNull(expressionParser, "ExpressionParser must not be null");
		Assert.notNull(evaluationContextProvider, "EvaluationContextProvider must not be null");

		this.expressionParser = expressionParser;
		this.evaluationContextProvider = evaluationContextProvider;
	}

	/**
	 * Bind values provided by {@link CassandraParameterAccessor} to placeholders in {@link BindingContext} while
	 * considering potential conversions and parameter types.
	 *
	 * @param parameterAccessor must not be {@literal null}.
	 * @param bindingContext must not be {@literal null}.
	 * @return {@literal null} if given {@code raw} value is empty.
	 */
	public List<Object> bind(CassandraParameterAccessor parameterAccessor, BindingContext bindingContext) {

		if (!bindingContext.hasBindings()) {
			return Collections.emptyList();
		}

		List<Object> parameters = new ArrayList<Object>(bindingContext.getBindings().size());

		for (ParameterBinding binding : bindingContext.getBindings()) {
			parameters.add(getParameterValueForBinding(parameterAccessor, bindingContext.getParameters(), binding));
		}

		return parameters;
	}

	/**
	 * Returns the value to be used for the given {@link ParameterBinding}.
	 *
	 * @param parameterAccessor must not be {@literal null}.
	 * @param parameters must not be {@literal null}.
	 * @param binding must not be {@literal null}.
	 * @return the value used for the given {@link ParameterBinding}.
	 */
	private Object getParameterValueForBinding(CassandraParameterAccessor parameterAccessor,
			CassandraParameters parameters, ParameterBinding binding) {

		if (binding.isExpression()) {
			return evaluateExpression(binding.getExpression(), parameters, parameterAccessor.getValues());
		}

		return binding.isNamed()
				? parameterAccessor.getBindableValue(getParameterIndex(parameters, binding.getParameterName()))
				: parameterAccessor.getBindableValue(binding.getParameterIndex());
	}

	private int getParameterIndex(CassandraParameters parameters, String parameterName) {

		for (CassandraParameters.CassandraParameter parameter : parameters) {
			if (parameterName.equals(parameter.getName())) {
				return parameter.getIndex();
			}
		}

		throw new IllegalArgumentException(
				String.format("Invalid parameter name; Cannot resolve parameter [%s]", parameterName));
	}

	/**
	 * Evaluates the given {@code expressionString}.
	 *
	 * @param expressionString must not be {@literal null} or empty.
	 * @param parameters must not be {@literal null}.
	 * @param parameterValues must not be {@literal null}.
	 * @return the value of the {@code expressionString} evaluation.
	 */
	private Object evaluateExpression(String expressionString, CassandraParameters parameters,
			Object[] parameterValues) {

		EvaluationContext evaluationContext = evaluationContextProvider.getEvaluationContext(parameters, parameterValues);
		Expression expression = expressionParser.parseExpression(expressionString);

		return expression.getValue(evaluationContext, Object.class);
	}

	/**
	 * @author Mark Paluch
	 * @since 1.5
	 */
	static class BindingContext {

		final CassandraQueryMethod queryMethod;
		final List<ParameterBinding> bindings;

		/**
		 * Creates new {@link BindingContext}.
		 *
		 * @param queryMethod {@link CassandraQueryMethod} on which the parameters are evaluated.
		 * @param bindings {@link List} of {@link ParameterBinding} containing name or position (index)
		 * information pertaining to the parameter in the referenced {@code queryMethod}.
		 */
		public BindingContext(CassandraQueryMethod queryMethod, List<ParameterBinding> bindings) {

			this.queryMethod = queryMethod;
			this.bindings = bindings;
		}

		/**
		 * @return {@literal true} when list of bindings is not empty.
		 */
		boolean hasBindings() {
			return !CollectionUtils.isEmpty(bindings);
		}

		/**
		 * Get unmodifiable list of {@link ParameterBinding}s.
		 *
		 * @return never {@literal null}.
		 */
		public List<ParameterBinding> getBindings() {
			return Collections.unmodifiableList(bindings);
		}

		/**
		 * Get the associated {@link CassandraParameters}.
		 *
		 * @return the {@link CassandraParameters} associated with the {@link CassandraQueryMethod}.
		 */
		public CassandraParameters getParameters() {
			return queryMethod.getParameters();
		}

		/**
		 * Get the {@link CassandraQueryMethod}.
		 *
		 * @return the {@link CassandraQueryMethod} used in the expression evaluation context.
		 */
		public CassandraQueryMethod getQueryMethod() {
			return queryMethod;
		}
	}

	/**
	 * A generic parameter binding with name or position information.
	 *
	 * @author Mark Paluch
	 */
	static class ParameterBinding {

		private final boolean quoted;
		private final int parameterIndex;
		private final String expression;
		private final String parameterName;

		private ParameterBinding(int parameterIndex, boolean quoted, String expression, String parameterName) {
			this.parameterIndex = parameterIndex;
			this.quoted = quoted;
			this.expression = expression;
			this.parameterName = parameterName;
		}

		public static ParameterBinding expression(String expression, boolean quoted) {
			return new ParameterBinding(-1, quoted, expression, null);
		}

		public static ParameterBinding indexed(int parameterIndex) {
			return new ParameterBinding(parameterIndex, false, null, null);
		}

		public static ParameterBinding named(String name) {
			return new ParameterBinding(-1, false, null, name);
		}

		public boolean isNamed() {
			return (parameterName != null);
		}

		public int getParameterIndex() {
			return parameterIndex;
		}

		public String getParameter() {
			return ("?" + (isExpression() ? "expr" : "") + parameterIndex);
		}

		public String getExpression() {
			return expression;
		}

		public boolean isExpression() {
			return (this.expression != null);
		}

		public String getParameterName() {
			return parameterName;
		}
	}
}
