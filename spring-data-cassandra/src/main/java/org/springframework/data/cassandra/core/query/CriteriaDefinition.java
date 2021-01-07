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
package org.springframework.data.cassandra.core.query;


import java.util.Optional;
import java.util.function.Function;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

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
			return this.operator;
		}

		/**
		 * @return the match value.
		 */
		@Nullable
		public Object getValue() {
			return this.value;
		}

		/**
		 * This method allows the application of a function to this {@link Predicate} value. The function should expect a
		 * single {@link Object} argument and produce an {@code R} result. Any exception thrown by f() will be propagated to
		 * the caller.
		 *
		 * @param <R>
		 * @return the result of the {@link Function mappingFunction}.
		 */
		public <R> R as(Function<Object, ? extends R> mappingFunction) {
			return mappingFunction.apply(this.value);
		}

		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#equals(java.lang.Object)
		 */
		@Override
		public boolean equals(Object o) {

			if (this == o) {
				return true;
			}

			if (!(o instanceof Predicate)) {
				return false;
			}

			Predicate predicate = (Predicate) o;

			if (!ObjectUtils.nullSafeEquals(operator, predicate.operator)) {
				return false;
			}

			return ObjectUtils.nullSafeEquals(value, predicate.value);
		}

		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		@Override
		public int hashCode() {
			int result = ObjectUtils.nullSafeHashCode(operator);
			result = 31 * result + ObjectUtils.nullSafeHashCode(value);
			return result;
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

		/**
		 * Render to a CQL-like representation. Rendering does not apply conversion via
		 * {@link com.datastax.driver.core.CodecRegistry} therefore this output is an approximation towards CQL and not
		 * necessarily valid CQL.
		 *
		 * @param value optional predicate value, can be {@literal null}.
		 * @return A CQL-like representation.
		 * @since 2.1
		 */
		default String toCql(@Nullable Object value) {
			return String.format("%s %s", toString(), value);
		}
	}

	/**
	 * Commonly used CQL operators.
	 */
	enum Operators implements Operator {

		CONTAINS("CONTAINS"), CONTAINS_KEY("CONTAINS KEY"), EQ("="),

		/**
		 * @since 2.1
		 */
		NE("!="),

		/**
		 * @since 2.1
		 */
		IS_NOT_NULL("IS NOT NULL") {
			@Override
			public String toCql(@Nullable Object value) {
				return toString();
			}
		},

		GT(">"), GTE(">="), LT("<"), LTE("<="), IN("IN"), LIKE("LIKE");

		public static Optional<Operators> from(String operator) {

			for (Operators operatorsValue : Operators.values()) {
				if (operatorsValue.toString().equals(operator)) {
					return Optional.of(operatorsValue);
				}
			}

			return Optional.empty();
		}

		private final String operator;

		Operators(String operator) {
			this.operator = operator;
		}

		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return this.operator;
		}
	}
}
