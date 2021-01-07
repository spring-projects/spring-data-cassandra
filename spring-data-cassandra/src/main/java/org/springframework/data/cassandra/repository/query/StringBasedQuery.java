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

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.data.cassandra.repository.query.BindingContext.ParameterBinding;
import org.springframework.data.mapping.model.SpELExpressionEvaluator;
import org.springframework.data.spel.ExpressionDependencies;
import org.springframework.expression.ExpressionParser;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;

/**
 * String-based Query abstracting a CQL query with parameter bindings.
 *
 * @author Mark Paluch
 * @since 2.0
 */
class StringBasedQuery {

	private final String query;

	private final CassandraParameters parameters;

	private final ExpressionParser expressionParser;

	private final List<ParameterBinding> queryParameterBindings = new ArrayList<>();

	private final ExpressionDependencies expressionDependencies;

	/**
	 * Create a new {@link StringBasedQuery} given {@code query}, {@link CassandraParameters} and
	 * {@link ExpressionParser}.
	 *
	 * @param query must not be empty.
	 * @param parameters must not be {@literal null}.
	 * @param expressionParser must not be {@literal null}.
	 */
	StringBasedQuery(String query, CassandraParameters parameters, ExpressionParser expressionParser) {

		this.query = ParameterBindingParser.INSTANCE.parseAndCollectParameterBindingsFromQueryIntoBindings(query,
				this.queryParameterBindings);
		this.parameters = parameters;
		this.expressionParser = expressionParser;
		this.expressionDependencies = createExpressionDependencies();
	}

	private ExpressionDependencies createExpressionDependencies() {

		if (queryParameterBindings.isEmpty()) {
			return ExpressionDependencies.none();
		}

		List<ExpressionDependencies> dependencies = new ArrayList<>();

		for (ParameterBinding binding : queryParameterBindings) {
			if (binding.isExpression()) {
				dependencies
						.add(ExpressionDependencies.discover(expressionParser.parseExpression(binding.getRequiredExpression())));
			}
		}

		return ExpressionDependencies.merged(dependencies);
	}

	/**
	 * Obtain {@link ExpressionDependencies} from the parsed query.
	 *
	 * @return the {@link ExpressionDependencies} from the parsed query.
	 */
	public ExpressionDependencies getExpressionDependencies() {
		return expressionDependencies;
	}

	/**
	 * Bind the query to actual parameters using {@link CassandraParameterAccessor},
	 *
	 * @param parameterAccessor must not be {@literal null}.
	 * @param evaluator must not be {@literal null}.
	 * @return the bound String query containing formatted parameters.
	 */
	public SimpleStatement bindQuery(CassandraParameterAccessor parameterAccessor, SpELExpressionEvaluator evaluator) {

		Assert.notNull(parameterAccessor, "CassandraParameterAccessor must not be null");
		Assert.notNull(evaluator, "SpELExpressionEvaluator must not be null");

		BindingContext bindingContext = new BindingContext(this.parameters, parameterAccessor, this.queryParameterBindings,
				evaluator);

		List<Object> arguments = bindingContext.getBindingValues();

		return ParameterBinder.INSTANCE.bind(this.query, arguments);
	}

	/**
	 * A parser that extracts the parameter bindings from a given query string.
	 *
	 * @author Mark Paluch
	 */
	enum ParameterBinder {

		INSTANCE;

		private static final String ARGUMENT_PLACEHOLDER = "?_param_?";
		private static final Pattern ARGUMENT_PLACEHOLDER_PATTERN = Pattern.compile(Pattern.quote(ARGUMENT_PLACEHOLDER));

		public SimpleStatement bind(String input, List<Object> parameters) {

			if (parameters.isEmpty()) {
				return SimpleStatement.newInstance(input);
			}

			StringBuilder result = new StringBuilder();

			int startIndex = 0;
			int currentPosition = 0;
			int parameterIndex = 0;

			Matcher matcher = ARGUMENT_PLACEHOLDER_PATTERN.matcher(input);

			while (currentPosition < input.length()) {

				if (!matcher.find()) {
					break;
				}

				int exprStart = matcher.start();

				result.append(input.subSequence(startIndex, exprStart)).append("?");

				parameterIndex++;
				currentPosition = matcher.end();
				startIndex = currentPosition;
			}

			String bindableStatement = result.append(input.subSequence(currentPosition, input.length())).toString();

			return SimpleStatement.newInstance(bindableStatement, parameters.subList(0, parameterIndex).toArray());
		}
	}

	/**
	 * A parser that extracts the parameter bindings from a given query string.
	 *
	 * @author Mark Paluch
	 */
	enum ParameterBindingParser {

		INSTANCE;

		private static final char CURRLY_BRACE_OPEN = '{';
		private static final char CURRLY_BRACE_CLOSE = '}';

		private static final Pattern INDEX_PARAMETER_BINDING_PATTERN = Pattern.compile("\\?(\\d+)");
		private static final Pattern NAMED_PARAMETER_BINDING_PATTERN = Pattern.compile("\\:(\\w+)");
		private static final Pattern INDEX_BASED_EXPRESSION_PATTERN = Pattern.compile("\\?\\#\\{");
		private static final Pattern NAME_BASED_EXPRESSION_PATTERN = Pattern.compile("\\:\\#\\{");

		private static final String ARGUMENT_PLACEHOLDER = "?_param_?";

		/**
		 * Returns a list of {@link ParameterBinding}s found in the given {@code input}.
		 *
		 * @param input can be {@literal null} or empty.
		 * @param bindings must not be {@literal null}.
		 * @return a list of {@link ParameterBinding}s found in the given {@code input}.
		 */
		public String parseAndCollectParameterBindingsFromQueryIntoBindings(String input, List<ParameterBinding> bindings) {

			if (!StringUtils.hasText(input)) {
				return input;
			}

			Assert.notNull(bindings, "Parameter bindings must not be null");

			return transformQueryAndCollectExpressionParametersIntoBindings(input, bindings);
		}

		private static String transformQueryAndCollectExpressionParametersIntoBindings(String input,
				List<ParameterBinding> bindings) {

			StringBuilder result = new StringBuilder();

			int startIndex = 0;
			int currentPosition = 0;

			while (currentPosition < input.length()) {

				Matcher matcher = findNextBindingOrExpression(input, currentPosition);

				// no expression parameter found
				if (matcher == null) {
					break;
				}

				int exprStart = matcher.start();
				currentPosition = exprStart;

				if (matcher.pattern() == NAME_BASED_EXPRESSION_PATTERN || matcher.pattern() == INDEX_BASED_EXPRESSION_PATTERN) {
					// eat parameter expression
					int curlyBraceOpenCount = 1;
					currentPosition += 3;

					while (curlyBraceOpenCount > 0 && currentPosition < input.length()) {
						switch (input.charAt(currentPosition++)) {
							case CURRLY_BRACE_OPEN:
								curlyBraceOpenCount++;
								break;
							case CURRLY_BRACE_CLOSE:
								curlyBraceOpenCount--;
								break;
							default:
						}
					}

					result.append(input.subSequence(startIndex, exprStart));
				} else {
					result.append(input.subSequence(startIndex, exprStart));
				}

				result.append(ARGUMENT_PLACEHOLDER);

				if (matcher.pattern() == NAME_BASED_EXPRESSION_PATTERN || matcher.pattern() == INDEX_BASED_EXPRESSION_PATTERN) {
					bindings.add(
							BindingContext.ParameterBinding
							.expression(input.substring(exprStart + 3, currentPosition - 1), true));
				} else {
					if (matcher.pattern() == INDEX_PARAMETER_BINDING_PATTERN) {
						bindings
								.add(BindingContext.ParameterBinding.indexed(Integer.parseInt(matcher.group(1))));
					} else {
						bindings.add(BindingContext.ParameterBinding.named(matcher.group(1)));
					}

					currentPosition = matcher.end();
				}

				startIndex = currentPosition;
			}

			return result.append(input.subSequence(currentPosition, input.length())).toString();
		}

		@Nullable
		private static Matcher findNextBindingOrExpression(String input, int position) {

			List<Matcher> matchers = new ArrayList<>();

			matchers.add(INDEX_PARAMETER_BINDING_PATTERN.matcher(input));
			matchers.add(NAMED_PARAMETER_BINDING_PATTERN.matcher(input));
			matchers.add(INDEX_BASED_EXPRESSION_PATTERN.matcher(input));
			matchers.add(NAME_BASED_EXPRESSION_PATTERN.matcher(input));

			TreeMap<Integer, Matcher> matcherMap = new TreeMap<>();

			for (Matcher matcher : matchers) {
				if (matcher.find(position)) {
					matcherMap.put(matcher.start(), matcher);
				}
			}

			return (matcherMap.isEmpty() ? null : matcherMap.values().iterator().next());
		}
	}
}
