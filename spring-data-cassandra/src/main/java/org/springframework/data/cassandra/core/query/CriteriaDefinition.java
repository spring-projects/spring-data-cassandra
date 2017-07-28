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

import org.springframework.lang.Nullable;
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

		private final Operator operator;

		private final @Nullable Object value;

		/**
		 * Create a new {@link Predicate} given {@code operator} and {@code value}.
		 *
		 * @param operator must not be {@literal null}.
		 * @param value the match value.
		 */
		public Predicate(Operator operator, @Nullable Object value) {

			Assert.notNull(operator, "Operator must not be null");

			this.operator = operator;
			this.value = value;
		}

		/**
		 * @return the operator, such as {@literal =}, {@literal >=}, {@literal LIKE}.
		 */
		public Operator getOperator() {
			return operator;
		}

		/**
		 * @return the match value.
		 */
		@Nullable
		public Object getValue() {
			return value;
		}
	}

	/**
	 * Strategy interface to represent a CQL predicate operator.
	 */
	interface Operator {

		/**
		 * @return the String representation of the operator.
		 */
		String toString();
	}

	/**
	 * Commonly used CQL operators.
	 */
	enum Operators implements Operator {

		EQ("="), GT(">"), GTE(">="), LT("<"), LTE("<="), CONTAINS("CONTAINS"), CONTAINS_KEY("CONTAINS KEY"), IN("IN"), LIKE(
				"LIKE");

		private final String operator;

		Operators(String operator) {
			this.operator = operator;
		}

		@Override
		public String toString() {
			return operator;
		}
	}
}
