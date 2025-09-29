/*
 * Copyright 2017-2025 the original author or authors.
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

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Function;

import org.springframework.data.cassandra.core.convert.CassandraVector;
import org.springframework.data.cassandra.core.mapping.SimilarityFunction;
import org.springframework.data.domain.Vector;
import org.springframework.lang.CheckReturnValue;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.CqlVector;

/**
 * Value object to abstract column names involved in a CQL query. Columns can be constructed from an array of names and
 * included using a {@link Selector}.
 *
 * @author Mark Paluch
 * @see com.datastax.oss.driver.api.core.CqlIdentifier
 * @see org.springframework.data.cassandra.core.query.ColumnName
 * @see Selector
 * @see FunctionCall
 * @see ColumnSelector
 * @since 2.0
 */
public class Columns implements Iterable<ColumnName> {

	private final Map<ColumnName, Selector> columns;

	private Columns(Map<ColumnName, Selector> columns) {
		this.columns = Collections.unmodifiableMap(columns);
	}

	/**
	 * Create an empty {@link Columns} instance without any columns.
	 *
	 * @return an empty {@link Columns} instance.
	 */
	public static Columns empty() {
		return new Columns(Collections.emptyMap());
	}

	/**
	 * Create a {@link Columns} given {@code columnNames}. Individual column names can be either quoted or unquoted.
	 *
	 * @param columnNames must not be {@literal null}.
	 * @return the {@link Columns} object for {@code columnNames}.
	 */
	public static Columns from(String... columnNames) {

		Assert.notNull(columnNames, "Column names must not be null");

		Map<ColumnName, Selector> columns = new LinkedHashMap<>(columnNames.length, 1);

		for (String columnName : columnNames) {
			columns.put(ColumnName.from(columnName), ColumnSelector.from(columnName));
		}

		return new Columns(columns);
	}

	/**
	 * Create a {@link Columns} given {@code columnNames}.
	 *
	 * @param columnNames must not be {@literal null}.
	 * @return the {@link Columns} object for {@code columnNames}.
	 */
	public static Columns from(CqlIdentifier... columnNames) {

		Assert.notNull(columnNames, "Column names must not be null");

		Map<ColumnName, Selector> columns = new LinkedHashMap<>(columnNames.length, 1);

		for (CqlIdentifier cqlId : columnNames) {
			columns.put(ColumnName.from(cqlId), ColumnSelector.from(cqlId));
		}

		return new Columns(columns);
	}

	/**
	 * Include column {@code columnName} to the selection. Column inclusion overrides an existing selection for the column
	 * name.
	 *
	 * @param columnName must not be {@literal null}.
	 * @return a new {@link Columns} object containing all column definitions and the included {@code columnName}.
	 */
	@CheckReturnValue
	public Columns include(String columnName) {
		return select(columnName, ColumnSelector.from(columnName));
	}

	/**
	 * Include column {@code columnName} to the selection. Column inclusion overrides an existing selection for the column
	 * name.
	 *
	 * @param columnName must not be {@literal null}.
	 * @return a new {@link Columns} object containing all column definitions and the included {@code columnName}.
	 */
	@CheckReturnValue
	public Columns include(CqlIdentifier columnName) {
		return select(columnName, ColumnSelector.from(columnName));
	}

	/**
	 * Include column {@code columnName} as TTL value in the selection. This column selection overrides an existing
	 * selection for the column name.
	 *
	 * @param columnName must not be {@literal null}.
	 * @return a new {@link Columns} object containing all column definitions and the TTL for {@code columnName}.
	 */
	@CheckReturnValue
	public Columns ttl(String columnName) {
		return select(columnName, FunctionCall.from("TTL", ColumnSelector.from(columnName)));
	}

	/**
	 * Include column {@code columnName} as TTL value in the selection. This column selection overrides an existing
	 * selection for the column name.
	 *
	 * @param columnName must not be {@literal null}.
	 * @return a new {@link Columns} object containing all column definitions and the TTL for {@code columnName}.
	 */
	@CheckReturnValue
	public Columns ttl(CqlIdentifier columnName) {
		return select(columnName, FunctionCall.from("TTL", ColumnSelector.from(columnName)));
	}

	/**
	 * Include column {@code columnName} with {@link Selector}. This column selection overrides an existing selection for
	 * the column name.
	 *
	 * @param columnName must not be {@literal null}.
	 * @return a new {@link Columns} object containing all column definitions and the selected {@code columnName}.
	 */
	@CheckReturnValue
	public Columns select(String columnName, Selector selector) {
		return select(ColumnName.from(columnName), selector);
	}

	/**
	 * Include column {@code columnName} with a built {@link Selector}. This column selection overrides an existing
	 * selection for the column name. {@link SelectorBuilder} uses the given {@code columnName} as column alias to
	 * represent the selection in the result.
	 *
	 * @param columnName must not be {@literal null}.
	 * @return a new {@link Columns} object containing all column definitions and the selected {@code columnName}.
	 * @since 4.5
	 */
	@CheckReturnValue
	public Columns select(CqlIdentifier columnName, Function<SelectorBuilder, Selector> builder) {

		ColumnName from = ColumnName.from(columnName);
		return select(from, builder.apply(new DefaultSelectorBuilder(from)));
	}

	/**
	 * Include column {@code columnName} with a built {@link Selector}. This column selection overrides an existing
	 * selection for the column name. {@link SelectorBuilder} uses the given {@code columnName} as column alias to
	 * represent the selection in the result.
	 *
	 * @param columnName must not be {@literal null}.
	 * @return a new {@link Columns} object containing all column definitions and the selected {@code columnName}.
	 * @since 4.5
	 */
	@CheckReturnValue
	public Columns select(String columnName, Function<SelectorBuilder, Selector> builder) {

		ColumnName from = ColumnName.from(columnName);
		return select(from, builder.apply(new DefaultSelectorBuilder(from)));
	}

	/**
	 * Include column {@code columnName} with {@link Selector}. This column selection overrides an existing selection for
	 * the column name.
	 *
	 * @param columnName must not be {@literal null}.
	 * @return a new {@link Columns} object containing all column definitions and the selected {@code columnName}.
	 */
	@CheckReturnValue
	public Columns select(CqlIdentifier columnName, Selector selector) {
		return select(ColumnName.from(columnName), selector);
	}

	/**
	 * Include column {@code columnName} with {@link Selector}. This column selection overrides an existing selection for
	 * the column name.
	 *
	 * @param columnName must not be {@literal null}.
	 * @return a new {@link Columns} object containing all column definitions and the selected {@code columnName}.
	 */
	private Columns select(ColumnName columnName, Selector selector) {

		Map<ColumnName, Selector> result = new LinkedHashMap<>(this.columns);
		result.put(columnName, selector);

		return new Columns(result);
	}

	/**
	 * @return {@literal true} if no columns were specified and this {@link Columns} object is empty.
	 */
	public boolean isEmpty() {
		return this.columns.isEmpty();
	}

	/**
	 * Returns a new {@link Columns} consisting of the {@link ColumnName}s of the current {@link Columns} combined with
	 * the given ones. Existing {@link ColumnName}s are overwritten if specified within {@code columns}.
	 *
	 * @param columns must not be {@literal null}.
	 * @return a new {@link Columns} with the merged result of the configured and given {@link Columns}.
	 */
	@CheckReturnValue
	public Columns and(Columns columns) {

		Map<ColumnName, Selector> result = new LinkedHashMap<>(this.columns);

		result.putAll(columns.columns);

		return new Columns(result);
	}

	@Override
	public Iterator<ColumnName> iterator() {
		return this.columns.keySet().iterator();
	}

	/**
	 * @param columnName must not be {@literal null}.
	 * @return the {@link Optional} {@link Selector} for {@link ColumnName}.
	 */
	public Optional<Selector> getSelector(ColumnName columnName) {

		Assert.notNull(columnName, "ColumnName must not be null");

		return Optional.ofNullable(this.columns.get(columnName));
	}

	@Override
	public boolean equals(@Nullable Object object) {

		if (this == object) {
			return true;
		}

		if (!(object instanceof Columns)) {
			return false;
		}

		Columns that = (Columns) object;

		return this.columns.equals(that.columns);
	}

	@Override
	public int hashCode() {
		int result = 17;
		result += 31 * ObjectUtils.nullSafeHashCode(this.columns);
		return result;
	}

	@Override
	public String toString() {

		Iterator<Entry<ColumnName, Selector>> iterator = this.columns.entrySet().iterator();
		StringBuilder builder = toString(iterator);

		if (builder.isEmpty()) {
			return "*";
		}

		return builder.toString();
	}

	private StringBuilder toString(Iterator<Entry<ColumnName, Selector>> iterator) {

		StringBuilder builder = new StringBuilder();
		boolean first = true;

		while (iterator.hasNext()) {

			Entry<ColumnName, Selector> entry = iterator.next();

			Selector expression = entry.getValue();

			if (first) {
				first = false;
			} else {
				builder.append(", ");
			}

			builder.append(expression.toString());
		}

		return builder;
	}

	/**
	 * Strategy interface to render a column or function selection.
	 *
	 * @author Mark Paluch
	 */
	public interface Selector {

		/**
		 * Apply the given {@code alias} to the current {@link Selector} expression.
		 *
		 * @param alias
		 * @return the aliased {@code Selector} expression.
		 * @since 4.5
		 */
		default Selector as(String alias) {
			return as(CqlIdentifier.fromCql(alias));
		}

		/**
		 * Apply the given {@code alias} to the current {@link Selector} expression.
		 *
		 * @param alias
		 * @return the aliased {@code Selector} expression.
		 * @since 4.5
		 */
		Selector as(CqlIdentifier alias);

		String getExpression();

		Optional<CqlIdentifier> getAlias();

	}

	/**
	 * Column selection.
	 *
	 * @author Mark Paluch
	 */
	public static class ColumnSelector implements Selector {

		private final ColumnName columnName;
		private final Optional<CqlIdentifier> alias;

		ColumnSelector(ColumnName columnName) {

			Assert.notNull(columnName, "ColumnName must not be null");

			this.columnName = columnName;
			this.alias = Optional.empty();
		}

		ColumnSelector(ColumnName columnName, CqlIdentifier alias) {

			Assert.notNull(columnName, "ColumnName must not be null");
			Assert.notNull(alias, "Alias must not be null");

			this.columnName = columnName;
			this.alias = Optional.of(alias);
		}

		/**
		 * Create a {@link ColumnSelector} given {@link ColumnName}.
		 */
		public static ColumnSelector from(ColumnName columnName) {
			return new ColumnSelector(columnName);
		}

		/**
		 * Create a {@link ColumnSelector} given {@link CqlIdentifier}.
		 */
		public static ColumnSelector from(CqlIdentifier columnName) {
			return from(ColumnName.from(columnName));
		}

		/**
		 * Create a {@link ColumnSelector} given a plain {@code columnName}.
		 */
		public static ColumnSelector from(String columnName) {
			return from(ColumnName.from(columnName));
		}

		/**
		 * Create a {@link ColumnSelector} for the current {@link #getExpression() expression} aliased as {@code alias}.
		 *
		 * @param alias must not be {@literal null}.
		 * @return the aliased {@link ColumnSelector}.
		 */
		@Override
		public ColumnSelector as(CqlIdentifier alias) {
			return new ColumnSelector(columnName, alias);
		}

		@Override
		public Optional<CqlIdentifier> getAlias() {
			return alias;
		}

		@Override
		public String getExpression() {
			return columnName.toCql();
		}

		@Override
		public boolean equals(@Nullable Object o) {
			if (this == o) {
				return true;
			}
			if (!(o instanceof ColumnSelector)) {
				return false;
			}
			ColumnSelector that = (ColumnSelector) o;
			if (!ObjectUtils.nullSafeEquals(columnName, that.columnName)) {
				return false;
			}
			return ObjectUtils.nullSafeEquals(alias, that.alias);
		}

		@Override
		public int hashCode() {
			int result = ObjectUtils.nullSafeHashCode(columnName);
			result = 31 * result + ObjectUtils.nullSafeHashCode(alias);
			return result;
		}

		@Override
		public String toString() {
			return getAlias().map(cqlIdentifier -> String.format("%s AS %s", getExpression(), cqlIdentifier))
					.orElseGet(this::getExpression);
		}
	}

	/**
	 * Function call selector with alias support.
	 */
	public static class FunctionCall implements Selector {

		private final String expression;
		private final List<Object> params;
		private final Optional<CqlIdentifier> alias;

		FunctionCall(String expression, List<Object> params) {

			this.expression = expression;
			this.params = params;
			this.alias = Optional.empty();
		}

		private FunctionCall(String expression, List<Object> params, CqlIdentifier alias) {

			this.expression = expression;
			this.params = params;
			this.alias = Optional.of(alias);
		}

		public static FunctionCall from(String expression, Object... params) {
			return new FunctionCall(expression, Arrays.asList(params));
		}

		/**
		 * Create a {@link FunctionCall} for the current {@link #getExpression() expression} aliased as {@code alias}.
		 *
		 * @param alias must not be {@literal null}.
		 * @return the aliased {@link ColumnSelector}.
		 */
		@Override
		public FunctionCall as(CqlIdentifier alias) {
			return new FunctionCall(expression, params, alias);
		}

		FunctionCall as(ColumnName alias) {
			return new FunctionCall(expression, params, alias.getRequiredCqlIdentifier());
		}

		@Override
		public String getExpression() {
			return expression;
		}

		@Override
		public Optional<CqlIdentifier> getAlias() {
			return alias;
		}

		public List<Object> getParameters() {
			return params;
		}

		@Override
		public boolean equals(@Nullable Object o) {
			if (this == o) {
				return true;
			}
			if (!(o instanceof FunctionCall)) {
				return false;
			}
			FunctionCall that = (FunctionCall) o;
			if (!ObjectUtils.nullSafeEquals(expression, that.expression)) {
				return false;
			}
			if (!ObjectUtils.nullSafeEquals(params, that.params)) {
				return false;
			}
			return ObjectUtils.nullSafeEquals(alias, that.alias);
		}

		@Override
		public int hashCode() {
			int result = ObjectUtils.nullSafeHashCode(expression);
			result = 31 * result + ObjectUtils.nullSafeHashCode(params);
			result = 31 * result + ObjectUtils.nullSafeHashCode(alias);
			return result;
		}

		@Override
		public String toString() {

			String parameters = StringUtils.collectionToDelimitedString(getParameters(), ", ");

			return getAlias().map(cqlIdentifier -> String.format("%s(%s) AS %s", getExpression(), parameters, cqlIdentifier))
					.orElseGet(() -> String.format("%s(%s)", getExpression(), parameters));
		}

	}

	/**
	 * Entrypoint to build a {@link Selector}.
	 *
	 * @since 4.5
	 */
	public interface SelectorBuilder {

		/**
		 * Include the column in the selection.
		 *
		 * @return column selector for the used column name.
		 */
		Selector column();

		/**
		 * Return the time to live for the column in the selection.
		 *
		 * @return TTL function selector for the used column name.
		 */
		Selector ttl();

		/**
		 * Return a builder for a similarity function using the given {@link CqlVector}.
		 *
		 * @param vector
		 * @return builder to build a similarity function.
		 */
		SimilarityBuilder similarity(CqlVector<?> vector);

		/**
		 * Return a builder for a similarity function using the given {@link Vector}.
		 *
		 * @param vector
		 * @return builder to build a similarity function.
		 */
		SimilarityBuilder similarity(Vector vector);

	}

	/**
	 * Builder for similarity functions.
	 *
	 * @since 4.5
	 */
	public interface SimilarityBuilder {

		/**
		 * Return the Cosine similarity function for the column in the selection based on the previously defined vector.
		 *
		 * @return cosine similarity function selector.
		 */
		Selector cosine();

		/**
		 * Return the Euclidean similarity function for the column in the selection based on the previously defined vector.
		 *
		 * @return euclidean similarity function selector.
		 */
		Selector euclidean();

		/**
		 * Return the Dot-Product similarity function for the column in the selection based on the previously defined
		 * vector.
		 *
		 * @return dot-product similarity function selector.
		 */
		Selector dotProduct();

		/**
		 * Return a similarity function using {@link SimilarityFunction} for the column in the selection based on the
		 * previously defined vector.
		 *
		 * @param similarityFunction must not be {@literal null}.
		 * @return similarity function selector.
		 */
		Selector using(SimilarityFunction similarityFunction);

	}

	static class DefaultSelectorBuilder implements SelectorBuilder {

		private final ColumnName columnName;

		DefaultSelectorBuilder(ColumnName columnName) {
			this.columnName = columnName;
		}

		@Override
		public Selector column() {
			return new ColumnSelector(columnName);
		}

		@Override
		public Selector ttl() {
			return FunctionCall.from("TTL", ColumnSelector.from(columnName)).as(columnName);
		}

		@Override
		public SimilarityBuilder similarity(CqlVector<?> vector) {
			return similarity(CassandraVector.of(vector));
		}

		@Override
		public SimilarityBuilder similarity(Vector vector) {

			Assert.notNull(vector, "Vector must not be null");

			return new SimilarityBuilder() {
				@Override
				public Selector cosine() {
					return using(SimilarityFunction.COSINE);
				}

				@Override
				public Selector euclidean() {
					return using(SimilarityFunction.EUCLIDEAN);
				}

				@Override
				public Selector dotProduct() {
					return using(SimilarityFunction.DOT_PRODUCT);
				}

				@Override
				public Selector using(SimilarityFunction similarityFunction) {
					return FunctionCall.from("similarity_" + similarityFunction.name().toLowerCase(Locale.ROOT), columnName,
							vector).as(columnName);
				}
			};
		}

	}

}
