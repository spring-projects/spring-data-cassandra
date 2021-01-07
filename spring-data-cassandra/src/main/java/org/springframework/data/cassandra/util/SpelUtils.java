/*
 * Copyright 2017-2021 the original author or authors.
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
package org.springframework.data.cassandra.util;

import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.ParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.lang.Nullable;

/**
 * Evaluates a SpEL expression.
 *
 * @author Mark Paluch
 */
public class SpelUtils {

	public static final SpelExpressionParser DEFAULT_PARSER = new SpelExpressionParser();

	/**
	 * Evaluates the given value against the given context as a string.
	 */
	@Nullable
	public static String evaluate(CharSequence value, EvaluationContext context) {
		return evaluate(value, context, DEFAULT_PARSER);
	}

	/**
	 * Evaluates the given value against the given context as a string using the given parser.
	 */
	@Nullable
	public static String evaluate(CharSequence value, EvaluationContext context, ExpressionParser parser) {
		return evaluate(value, context, String.class, parser);
	}

	/**
	 * Evaluates the given value against the given context as an object of the given class.
	 */
	@Nullable
	public static <T> T evaluate(CharSequence value, EvaluationContext context, Class<T> clazz) {
		return evaluate(value, context, clazz, DEFAULT_PARSER);
	}

	/**
	 * Evaluates the given value against the given context as an object of the given class using the given parser.
	 */
	@Nullable
	public static <T> T evaluate(CharSequence value, EvaluationContext context, Class<T> clazz, ExpressionParser parser) {

		Expression expression = parser.parseExpression(value.toString(), ParserContext.TEMPLATE_EXPRESSION);
		return expression.getValue(context, clazz);
	}
}
