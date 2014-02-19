package org.springframework.data.cassandra.util;

import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.ParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;

public class SpelUtils {

	public static final SpelExpressionParser DEFAULT_PARSER = new SpelExpressionParser();

	/**
	 * Evaluates the given value against the given context as a string.
	 */
	public static String evaluate(CharSequence value, EvaluationContext context) {
		return evaluate(value, context, DEFAULT_PARSER);
	}

	/**
	 * Evaluates the given value against the given context as a string using the given parser.
	 */
	public static String evaluate(CharSequence value, EvaluationContext context, ExpressionParser parser) {
		return evaluate(value, context, String.class, parser);
	}

	/**
	 * Evaluates the given value against the given context as an object of the given class.
	 */
	public static <T> T evaluate(CharSequence value, EvaluationContext context, Class<T> clazz) {
		return evaluate(value, context, clazz, DEFAULT_PARSER);
	}

	/**
	 * Evaluates the given value against the given context as an object of the given class using the given parser.
	 */
	public static <T> T evaluate(CharSequence value, EvaluationContext context, Class<T> clazz, ExpressionParser parser) {
		Expression expression = parser.parseExpression(value.toString(), ParserContext.TEMPLATE_EXPRESSION);
		return expression.getValue(context, clazz);
	}
}
