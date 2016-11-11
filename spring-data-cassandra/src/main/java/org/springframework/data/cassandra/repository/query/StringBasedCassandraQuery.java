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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.repository.query.ExpressionEvaluatingParameterBinder.BindingContext;
import org.springframework.data.repository.query.EvaluationContextProvider;
import org.springframework.data.repository.query.QueryCreationException;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.querybuilder.BindMarker;

/**
 * String-based {@link AbstractCassandraQuery} implementation.
 * <p>
 * A {@link StringBasedCassandraQuery} expects a query method to be annotated with
 * {@link org.springframework.data.cassandra.repository.Query} with a CQL query. String-based queries support named,
 * index-based and expression parameters that are resolved during query execution.
 *
 * @author Matthew Adams
 * @author Mark Paluch
 * @see org.springframework.data.cassandra.repository.Query
 */
public class StringBasedCassandraQuery extends AbstractCassandraQuery {

	private static final Logger LOG = LoggerFactory.getLogger(StringBasedCassandraQuery.class);
	private static final ParameterBindingParser BINDING_PARSER = ParameterBindingParser.INSTANCE;

	private final CodecRegistry codecRegistry;

	private final ExpressionEvaluatingParameterBinder parameterBinder;

	private final List<ParameterBinding> queryParameterBindings;

	private final String query;

	/**
	 * Creates a new {@link StringBasedCassandraQuery} for the given {@link CassandraQueryMethod},
	 * {@link CassandraOperations}, {@link SpelExpressionParser}, and {@link EvaluationContextProvider}.
	 *
	 * @param queryMethod {@link CassandraQueryMethod} on which this query is based.
	 * @param operations {@link CassandraOperations} used to perform data access in Cassandra.
	 * @param expressionParser {@link SpelExpressionParser} used to parse expressions in the query.
	 * @param evaluationContextProvider {@link EvaluationContextProvider} used to access the potentially shared
	 * {@link org.springframework.expression.spel.support.StandardEvaluationContext}.
	 */
	public StringBasedCassandraQuery(CassandraQueryMethod queryMethod, CassandraOperations operations,
			SpelExpressionParser expressionParser, EvaluationContextProvider evaluationContextProvider) {

		this(queryMethod.getAnnotatedQuery(), queryMethod, operations, expressionParser, evaluationContextProvider);
	}

	/**
	 * Creates a new {@link StringBasedCassandraQuery} for the given {@code query}, {@link CassandraQueryMethod},
	 * {@link CassandraOperations}, {@link SpelExpressionParser}, and {@link EvaluationContextProvider}.
	 *
	 * @param query
	 * @param queryMethod
	 * @param operations
	 * @param expressionParser
	 * @param evaluationContextProvider
	 */
	public StringBasedCassandraQuery(String query, CassandraQueryMethod queryMethod, CassandraOperations operations,
			SpelExpressionParser expressionParser, EvaluationContextProvider evaluationContextProvider) {

		super(queryMethod, operations);

		this.queryParameterBindings = new ArrayList<ParameterBinding>();
		this.query = BINDING_PARSER.parseAndCollectParameterBindingsFromQueryIntoBindings(query,
				this.queryParameterBindings);
		this.parameterBinder = new ExpressionEvaluatingParameterBinder(expressionParser, evaluationContextProvider);
		this.codecRegistry = operations.getSession().getCluster().getConfiguration().getCodecRegistry();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.repository.query.AbstractCassandraQuery#createQuery(org.springframework.data.cassandra.repository.query.CassandraParameterAccessor)
	 */
	@Override
	public String createQuery(CassandraParameterAccessor parameterAccessor) {

		try {
			List<Object> arguments = this.parameterBinder.bind(parameterAccessor,
					new BindingContext(getQueryMethod(), queryParameterBindings));

			String boundQuery = bind(query, arguments);

			if (LOG.isDebugEnabled()) {
				LOG.debug(String.format("Created query [%s].", boundQuery));
			}

			return boundQuery;
		} catch (RuntimeException e) {
			throw QueryCreationException.create(getQueryMethod(), e);
		}
	}

	private String bind(String query, List<Object> arguments) {
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
		public String parseAndCollectParameterBindingsFromQueryIntoBindings(String input,
				List<ParameterBinding> bindings) {

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
					bindings.add(ParameterBinding.expression(input.substring(exprStart + 3, currentPosition - 1), true));
				} else {
					if (matcher.pattern() == INDEX_PARAMETER_BINDING_PATTERN) {
						bindings.add(ParameterBinding.indexed(Integer.parseInt(matcher.group(1))));
					} else {
						bindings.add(ParameterBinding.named(matcher.group(1)));
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
