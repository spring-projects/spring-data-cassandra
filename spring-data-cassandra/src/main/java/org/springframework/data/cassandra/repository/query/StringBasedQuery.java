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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.data.cassandra.repository.query.ExpressionEvaluatingParameterBinder.BindingContext;
import org.springframework.data.cassandra.repository.query.ExpressionEvaluatingParameterBinder.ParameterBinding;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.querybuilder.BindMarker;

/**
 * String-based Query abstracting a CQL query with parameter bindings.
 * 
 * @author Mark Paluch
 * @since 2.0
 */
class StringBasedQuery {

	private final CodecRegistry codecRegistry;
	private final ExpressionEvaluatingParameterBinder parameterBinder;
	private final List<ParameterBinding> queryParameterBindings = new ArrayList<>();
	private final String query;

	/**
	 * Creates a new {@link StringBasedQuery} given {@code query}, {@link ExpressionEvaluatingParameterBinder} and
	 * {@link CodecRegistry}.
	 * 
	 * @param query must not be empty.
	 * @param parameterBinder must not be {@literal null}.
	 * @param codecRegistry must not be {@literal null}.
	 */
	public StringBasedQuery(String query, ExpressionEvaluatingParameterBinder parameterBinder,
			CodecRegistry codecRegistry) {

		Assert.hasText(query, "Query must not be empty");
		Assert.notNull(parameterBinder, "ExpressionEvaluatingParameterBinder must not be null");
		Assert.notNull(codecRegistry, "CodecRegistry must not be null");

		this.codecRegistry = codecRegistry;
		this.parameterBinder = parameterBinder;

		this.query = ParameterBindingParser.INSTANCE.parseAndCollectParameterBindingsFromQueryIntoBindings(query,
				this.queryParameterBindings);

	}

	/**
	 * Bind the query to actual parameters using {@link CassandraParameterAccessor},
	 * 
	 * @param parameterAccessor must not be {@literal null}.
	 * @param queryMethod must not be {@literal null}.
	 * @return the bound String query containing formatted parameters.
	 */
	public String bindQuery(CassandraParameterAccessor parameterAccessor, CassandraQueryMethod queryMethod) {

		Assert.notNull(parameterAccessor, "CassandraParameterAccessor must not be null");
		Assert.notNull(queryMethod, "CassandraQueryMethod must not be null");
		
		List<Object> arguments = parameterBinder.bind(parameterAccessor,
				new BindingContext(queryMethod, queryParameterBindings));
		
		return ParameterBinder.INSTANCE.bind(query, codecRegistry, arguments);
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

		public String bind(String input, CodecRegistry codecRegistry, List<Object> parameters) {

			if (parameters.isEmpty()) {
				return input;
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

				result.append(input.subSequence(startIndex, exprStart));
				result = appendValue(parameters.get(parameterIndex++), codecRegistry, result);

				currentPosition = matcher.end();
				startIndex = currentPosition;
			}

			return result.append(input.subSequence(currentPosition, input.length())).toString();
		}

		static StringBuilder appendValue(Object value, CodecRegistry codecRegistry, StringBuilder builder) {

			if (value == null) {
				builder.append("null");
			} else if (value instanceof BindMarker) {
				builder.append(value);
			} else if (value instanceof List && isSerializable(value)) {
				// bind variables are not supported inside collection literals
				appendList((List<?>) value, codecRegistry, builder);
			} else if (value instanceof Set && isSerializable(value)) {
				// bind variables are not supported inside collection literals
				appendSet((Set<?>) value, codecRegistry, builder);
			} else if (value instanceof Map && isSerializable(value)) {
				// bind variables are not supported inside collection literals
				appendMap((Map<?, ?>) value, codecRegistry, builder);
			} else if (isSerializable(value)) {
				TypeCodec<Object> codec = codecRegistry.codecFor(value);
				builder.append(codec.format(value));
			} else {
				throw new IllegalArgumentException(String.format("Argument value [%s] is not serializable", value.toString()));
			}

			return builder;
		}

		private static StringBuilder appendList(List<?> list, CodecRegistry codecRegistry, StringBuilder builder) {

			for (int index = 0, size = list.size(); index < size; index++) {
				builder.append(index > 0 ? "," : "");
				appendValue(list.get(index), codecRegistry, builder);
			}

			return builder;
		}

		private static StringBuilder appendSet(Set<?> set, CodecRegistry codecRegistry, StringBuilder builder) {

			boolean first = true;

			for (Object element : set) {
				builder.append(first ? "" : ",");
				appendValue(element, codecRegistry, builder);
				first = false;
			}

			return builder;
		}

		private static StringBuilder appendMap(Map<?, ?> map, CodecRegistry codecRegistry, StringBuilder builder) {

			builder.append('{');

			boolean first = true;

			for (Map.Entry<?, ?> entry : map.entrySet()) {
				builder.append(first ? "" : ",");
				appendValue(entry.getKey(), codecRegistry, builder);
				builder.append(':');
				appendValue(entry.getValue(), codecRegistry, builder);
				first = false;
			}

			builder.append('}');

			return builder;
		}

		/**
		 * Return true if the given value is likely to find a suitable codec to be serialized as a query parameter. If the
		 * value is not serializable, it must be included in the query string. Non serializable values include special
		 * values such as function calls, column names and bind markers, and collections thereof. We also don't serialize
		 * fixed size number types. The reason is that if we do it, we will force a particular size (4 bytes for ints, ...)
		 * and for the query builder, we don't want users to have to bother with that.
		 *
		 * @param value the value to inspect.
		 * @return true if the value is serializable, false otherwise.
		 */
		static boolean isSerializable(Object value) {

			if (containsSpecialValue(value)) {
				return false;
			}

			if (value instanceof Collection) {
				for (Object element : (Collection) value) {
					if (!isSerializable(element)) {
						return false;
					}
				}
			}

			if (value instanceof Map) {
				for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
					if (!isSerializable(entry.getKey()) || !isSerializable(entry.getValue())) {
						return false;
					}
				}
			}

			return true;
		}

		static boolean containsSpecialValue(Object value) {

			if (value instanceof BindMarker) {
				return true;
			}

			if (value instanceof Collection) {
				for (Object element : (Collection) value) {
					if (containsSpecialValue(element)) {
						return true;
					}
				}
			}

			if (value instanceof Map) {
				for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
					if (containsSpecialValue(entry.getKey()) || containsSpecialValue(entry.getValue())) {
						return true;
					}
				}
			}

			return false;
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
					bindings.add(ExpressionEvaluatingParameterBinder.ParameterBinding
							.expression(input.substring(exprStart + 3, currentPosition - 1), true));
				} else {
					if (matcher.pattern() == INDEX_PARAMETER_BINDING_PATTERN) {
						bindings
								.add(ExpressionEvaluatingParameterBinder.ParameterBinding.indexed(Integer.parseInt(matcher.group(1))));
					} else {
						bindings.add(ExpressionEvaluatingParameterBinder.ParameterBinding.named(matcher.group(1)));
					}

					currentPosition = matcher.end();
				}

				startIndex = currentPosition;
			}

			return result.append(input.subSequence(currentPosition, input.length())).toString();
		}

		private static Matcher findNextBindingOrExpression(String input, int position) {

			List<Matcher> matchers = new ArrayList<Matcher>();

			matchers.add(INDEX_PARAMETER_BINDING_PATTERN.matcher(input));
			matchers.add(NAMED_PARAMETER_BINDING_PATTERN.matcher(input));
			matchers.add(INDEX_BASED_EXPRESSION_PATTERN.matcher(input));
			matchers.add(NAME_BASED_EXPRESSION_PATTERN.matcher(input));

			TreeMap<Integer, Matcher> matcherMap = new TreeMap<Integer, Matcher>();

			for (Matcher matcher : matchers) {
				if (matcher.find(position)) {
					matcherMap.put(matcher.start(), matcher);
				}
			}

			return (matcherMap.isEmpty() ? null : matcherMap.values().iterator().next());
		}
	}
}
