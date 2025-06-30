/*
 * Copyright 2024-2025 the original author or authors.
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

import org.jspecify.annotations.Nullable;

import org.springframework.data.expression.ValueEvaluationContext;
import org.springframework.data.expression.ValueExpression;
import org.springframework.data.expression.ValueExpressionParser;
import org.springframework.data.mapping.model.ValueExpressionEvaluator;

/**
 * @author Marcin Grzejszczak
 * @author Mark Paluch
 */
class ContextualValueExpressionEvaluator implements ValueExpressionEvaluator {

	private final ValueExpressionParser parser;

	public ContextualValueExpressionEvaluator(ValueExpressionParser parser, ValueEvaluationContext evaluationContext) {
		this.parser = parser;
		this.evaluationContext = evaluationContext;
	}

	private final ValueEvaluationContext evaluationContext;

	@SuppressWarnings("unchecked")
	@Override
	public <T> @Nullable T evaluate(String rawExpressionString) {

		String expressionString = rawExpressionString.contains("#{") || rawExpressionString.contains("${")
				? rawExpressionString
				: "#{" + rawExpressionString + "}";

		ValueExpression expression = parser.parse(expressionString);
		return (T) expression.evaluate(evaluationContext);
	}

}
