/*
 * Copyright 2017 the original author or authors.
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
package org.springframework.data.cassandra.core.query;

import lombok.EqualsAndHashCode;

import org.springframework.util.Assert;

/**
 * Criteria definition for a {@link ColumnName} exposing a {@link Predicate}.
 *
 * @author Mark Paluch
 * @since 2.0
 */
public interface CriteriaDefinition {

	/**
	 * Get the identifying {@literal key}.
	 *
	 * @return the {@link ColumnName}.
	 */
	ColumnName getColumnName();

	/**
	 * Get {@link Predicate}.
	 *
	 * @return the {@link Predicate}
	 */
	Predicate getPredicate();

	/**
	 * Represents an operator associated with its value.
	 *
	 * @author Mark Paluch
	 */
	@EqualsAndHashCode
	class Predicate {

		private final String operator;

		private final Object value;

		/**
		 * Create a new {@link Predicate} given {@code operator} and {@code value}.
		 *
		 * @param operator must not be {@literal null} or empty.
		 * @param value the match value.
		 */
		public Predicate(String operator, Object value) {

			Assert.hasText(operator, "Operator must not be null or empty");

			this.operator = operator;
			this.value = value;
		}

		/**
		 * @return the operator, such as {@literal =}, {@literal >=}, {@literal LIKE}.
		 */
		public String getOperator() {
			return operator;
		}

		/**
		 * @return the match value.
		 */
		public Object getValue() {
			return value;
		}
	}
}
