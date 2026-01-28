/*
 * Copyright 2017-present the original author or authors.
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

import org.jspecify.annotations.Nullable;

import org.springframework.data.core.PropertyPath;
import org.springframework.data.core.TypedPropertyPath;
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * Value object representing a column name. Column names can be expressed either through {@link CqlIdentifier} or a
 * {@link String} literal. Using a String literal preserves case and is suitable to reference properties.
 * <p>
 * Equality and hash code are based on {@link #toCql()} representation.
 * <p>
 * Implementing classes must provide either {@link #getColumnName()} or {@link #getCqlIdentifier()}.
 *
 * @author Mark Paluch
 * @see com.datastax.oss.driver.api.core.CqlIdentifier
 * @since 2.0
 */
public abstract class ColumnName {

	/**
	 * Create a {@link ColumnName} given a {@link TypedPropertyPath property}.
	 *
	 * @param property must not be {@literal null}.
	 * @return the {@link ColumnName} for {@link PropertyPath}
	 * @since 5.1
	 */
	public static <T, P> ColumnName from(TypedPropertyPath<T, P> property) {
		// lambdas/method references do not provide equality semantics, so we need to obtain the resolved PropertyPath
		// variant
		return from((PropertyPath) (TypedPropertyPath.of(property)));
	}

	/**
	 * Create a {@link ColumnName} given a {@link PropertyPath property}.
	 *
	 * @param propertyPath must not be {@literal null}.
	 * @return the {@link ColumnName} for {@link PropertyPath}
	 * @since 5.1
	 */
	public static ColumnName from(PropertyPath propertyPath) {
		return new PropertyPathColumnName(propertyPath);
	}

	/**
	 * Create a {@link ColumnName} given {@link CqlIdentifier}. The resulting instance uses CQL identifier rules to
	 * identify column names (quoting, case-sensitivity).
	 *
	 * @param cqlIdentifier must not be {@literal null}.
	 * @return the {@link ColumnName} for {@link CqlIdentifier}
	 * @see CqlIdentifier
	 */
	public static ColumnName from(CqlIdentifier cqlIdentifier) {
		return new CqlIdentifierColumnName(cqlIdentifier);
	}

	/**
	 * Create a {@link ColumnName} given a string {@code columnName}. The resulting instance uses String rules to identify
	 * column names (case-sensitivity).
	 *
	 * @param columnName must not be {@literal null} or empty.
	 * @return the {@link ColumnName} for {@code columnName}
	 */
	public static ColumnName from(String columnName) {
		return new StringColumnName(columnName);
	}

	/**
	 * @return the optional column name.
	 */
	public Optional<String> getColumnName() {
		return Optional.empty();
	}

	/**
	 * Indicates whether a column name is available.
	 *
	 * @return {@literal true} if a (string or {@link CqlIdentifier}) column name is available; {@literal false}
	 *         otherwise.
	 * @since 5.1
	 */
	public boolean hasColumnName() {
		return getColumnName().isPresent();
	}

	/**
	 * @return the optional {@link PropertyPath}.
	 */
	public @Nullable PropertyPath getPropertyPath() {
		return null;
	}

	/**
	 * Indicates whether a {@link PropertyPath} is available.
	 *
	 * @return {@literal true} if a {@link PropertyPath} is available; {@literal false} otherwise.
	 * @since 5.1
	 */
	public boolean hasPropertyPath() {
		return getPropertyPath() != null;
	}

	/**
	 * Returns the required {@link PropertyPath} or throws an {@link IllegalStateException} if not available.
	 *
	 * @return the required {@link PropertyPath}.
	 * @throws IllegalStateException if no {@link PropertyPath} is available.
	 * @since 5.1
	 */
	public PropertyPath getRequiredPropertyPath() {

		PropertyPath propertyPath = getPropertyPath();

		if (propertyPath == null) {
			throw new IllegalStateException("No PropertyPath available");
		}

		return propertyPath;
	}

	/**
	 * @return the optional {@link CqlIdentifier}.
	 */
	public Optional<CqlIdentifier> getCqlIdentifier() {
		return Optional.empty();
	}

	/**
	 * Returns the required {@link CqlIdentifier} or constructs one from available information.
	 *
	 * @return the required {@link CqlIdentifier}.
	 * @since 5.1
	 */
	public CqlIdentifier getRequiredCqlIdentifier() {
		return getCqlIdentifier().or(() -> getColumnName().map(CqlIdentifier::fromCql))
				.orElseGet(() -> CqlIdentifier.fromCql(toCql()));
	}

	/**
	 * Represent the column name as CQL.
	 *
	 * @return CQL representation of the column name.
	 */
	public abstract String toCql();

	@Override
	public boolean equals(@Nullable Object obj) {

		if (this == obj) {
			return true;
		}

		if (!(obj instanceof ColumnName)) {
			return false;
		}

		ColumnName that = (ColumnName) obj;

		return toCql().equals(that.toCql());
	}

	@Override
	public int hashCode() {
		int hashValue = 17;
		hashValue = 37 * hashValue + toCql().hashCode();
		return hashValue;
	}

	/**
	 * {@link PropertyPath}-based column name representation.
	 *
	 * @author Mark Paluch
	 */
	static class PropertyPathColumnName extends ColumnName {

		private final PropertyPath propertyPath;

		PropertyPathColumnName(PropertyPath propertyPath) {

			Assert.notNull(propertyPath, "Property path must not be null");

			this.propertyPath = propertyPath;
		}

		@Override
		public Optional<String> getColumnName() {
			return Optional.of(toCql());
		}

		@Override
		public PropertyPath getPropertyPath() {
			return propertyPath;
		}

		@Override
		public boolean equals(@Nullable Object obj) {

			if (obj instanceof PropertyPathColumnName that) {
				if (!this.propertyPath.equals(that.propertyPath)) {
					return false;
				}
			}

			return super.equals(obj);
		}

		@Override
		public String toCql() {
			return this.propertyPath.toDotPath();
		}

		@Override
		public String toString() {

			if (this.propertyPath.hasNext()) {
				return this.propertyPath.toString();
			}

			return this.propertyPath.toDotPath();
		}
	}

	/**
	 * {@link String}-based column name representation. Preserves letter casing.
	 *
	 * @author Mark Paluch
	 */
	static class StringColumnName extends ColumnName {

		private final String columnName;

		StringColumnName(String columnName) {

			Assert.hasText(columnName, "Column name must not be null or empty");

			this.columnName = columnName;
		}

		@Override
		public Optional<String> getColumnName() {
			return Optional.of(columnName);
		}

		@Override
		public String toCql() {
			return this.columnName;
		}

		@Override
		public String toString() {
			return this.columnName;
		}
	}

	/**
	 * {@link CqlIdentifier}-based column name representation. Follows {@link CqlIdentifier} comparison rules.
	 *
	 * @author Mark Paluch
	 */
	static class CqlIdentifierColumnName extends ColumnName {

		private final CqlIdentifier cqlIdentifier;

		CqlIdentifierColumnName(CqlIdentifier cqlIdentifier) {

			Assert.notNull(cqlIdentifier, "Column name must not be null");

			this.cqlIdentifier = cqlIdentifier;
		}

		@Override
		public Optional<CqlIdentifier> getCqlIdentifier() {
			return Optional.of(this.cqlIdentifier);
		}

		@Override
		public String toCql() {
			return this.cqlIdentifier.asInternal();
		}

		@Override
		public String toString() {
			return this.cqlIdentifier.toString();
		}
	}
}
