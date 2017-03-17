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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import lombok.EqualsAndHashCode;
import org.springframework.cassandra.core.cql.CqlIdentifier;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * Value object to abstract column names involved in a CQL query. Columns can be constructed from an array of names and
 * included/excluded.
 *
 * @author Mark Paluch
 * @since 2.0
 * @see CqlIdentifier
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

		Map<ColumnName, Selector> columns = new HashMap<>(columnNames.length, 1);

		Arrays.stream(columnNames).forEach(columnName -> columns.put(ColumnName.from(columnName), Selectors.Include));

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

		Map<ColumnName, Selector> columns = new HashMap<>(columnNames.length, 1);

		Arrays.stream(columnNames).forEach(cqlId -> columns.put(ColumnName.from(cqlId), Selectors.Include));

		return new Columns(columns);
	}

	/**
	 * Include a {@code columnName} in the selection. Including a column name overrides the exclusion if the column was
	 * already excluded.
	 *
	 * @param columnName must not be {@literal null}.
	 * @return a new {@link Columns} object containing all column definitions and the included {@code columnName}.
	 */
	public Columns include(String columnName) {
		return withColumn(columnName, Selectors.Include);
	}

	/**
	 * Include a {@code columnName} in the selection. Including a column name overrides the exclusion if the column was
	 * already excluded.
	 *
	 * @param columnName must not be {@literal null}.
	 * @return a new {@link Columns} object containing all column definitions and the included {@code columnName}.
	 */
	public Columns include(CqlIdentifier columnName) {
		return withColumn(columnName, Selectors.Include);
	}

	/**
	 * Include the {@code columnName} as TTL value in the selection. Including a column name overrides the exclusion if
	 * the column was already excluded.
	 *
	 * @param columnName must not be {@literal null}.
	 * @return a new {@link Columns} object containing all column definitions and the TTL for {@code columnName}.
	 */
	public Columns ttl(String columnName) {
		return withColumn(columnName, Selectors.TTL);
	}

	/**
	 * Include the {@code columnName} as TTL value in the selection. Including a column name overrides the exclusion if
	 * the column was already excluded.
	 *
	 * @param columnName must not be {@literal null}.
	 * @return a new {@link Columns} object containing all column definitions and the TTL for {@code columnName}.
	 */
	public Columns ttl(CqlIdentifier columnName) {
		return withColumn(columnName, Selectors.TTL);
	}

	/**
	 * Include the {@code columnName} as {@link Selector}. Including a column name overrides the exclusion if the column
	 * was already excluded.
	 *
	 * @param columnName must not be {@literal null}.
	 * @return a new {@link Columns} object containing all column definitions and the selected {@code columnName}.
	 */
	public Columns select(String columnName, Selector selector) {
		return withColumn(columnName, selector);
	}

	/**
	 * Include the {@code columnName} as {@link Selector}. Including a column name overrides the exclusion if the column
	 * was already excluded.
	 *
	 * @param columnName must not be {@literal null}.
	 * @return a new {@link Columns} object containing all column definitions and the selected {@code columnName}.
	 */
	public Columns select(CqlIdentifier columnName, Selector selector) {
		return withColumn(columnName, selector);
	}

	/**
	 * Exclude a {@code columnName} from the selection. Excluding a column name overrides the inclusion if the column was
	 * already included.
	 *
	 * @param columnName must not be {@literal null}.
	 * @return a new {@link Columns} object containing all column definitions and the excluded {@code columnName}.
	 */
	public Columns exclude(String columnName) {
		return withColumn(columnName, InternalSelectors.Exclude);
	}

	/**
	 * Exclude a {@code columnName} from the selection. Excluding a column name overrides the inclusion if the column was
	 * already included.
	 *
	 * @param columnName must not be {@literal null}.
	 * @return a new {@link Columns} object containing all column definitions and the excluded {@code columnName}.
	 */
	public Columns exclude(CqlIdentifier columnName) {
		return withColumn(columnName, InternalSelectors.Exclude);
	}

	private Columns withColumn(String columnName, Selector selector) {

		Assert.notNull(columnName, "Column name must not be null");

		Map<ColumnName, Selector> result = new LinkedHashMap<>(this.columns);
		result.put(ColumnName.from(columnName), selector);

		return new Columns(result);
	}

	private Columns withColumn(CqlIdentifier columnName, Selector selector) {

		Assert.notNull(columnName, "Column name must not be null");

		Map<ColumnName, Selector> result = new LinkedHashMap<>(this.columns);
		result.put(ColumnName.from(columnName), selector);

		return new Columns(result);
	}

	/**
	 * @return {@literal true} if no columns were specified and this {@link Columns} object is empty.
	 */
	public boolean isEmpty() {
		return this.columns.isEmpty();
	}

	/**
	 * @return {@literal true} if this {@link Columns} object contains excluded columns.
	 */
	public boolean hasExclusions() {
		return columns.values().stream().anyMatch(InternalSelectors.Exclude::equals);
	}

	/**
	 * Returns a new {@link Columns} consisting of the {@link ColumnName}s of the current {@link Columns} combined with
	 * the given ones. Existing {@link ColumnName}s are overwritten if specified within {@code columns}.
	 *
	 * @param columns can be {@literal null}.
	 * @return
	 */
	public Columns and(Columns columns) {

		Map<ColumnName, Selector> result = new LinkedHashMap<>(this.columns);

		result.putAll(columns.columns);

		return new Columns(result);
	}

	/* (non-Javadoc)
	 * @see java.lang.Iterable#iterator()
	 */
	@Override
	public Iterator<ColumnName> iterator() {
		return this.columns.keySet().iterator();
	}

	/**
	 * @param columnName must not be {@literal null}.
	 * @return {@literal true} if the {@link ColumnName} is excluded.
	 */
	public boolean isExcluded(ColumnName columnName) {

		Assert.notNull(columnName, "ColumnName must not be null");

		return getColumnExpression(columnName).filter(InternalSelectors.Exclude::equals).isPresent();
	}

	/**
	 * @param columnName must not be {@literal null}.
	 * @return the {@link Optional} {@link Selector} for {@link ColumnName}.
	 */
	public Optional<Selector> getColumnExpression(ColumnName columnName) {

		Assert.notNull(columnName, "ColumnName must not be null");

		return Optional.ofNullable(this.columns.get(columnName));
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object object) {

		if (this == object) {
			return true;
		}

		if (!(object instanceof Columns)) {
			return false;
		}

		Columns that = (Columns) object;

		return this.columns.equals(that.columns);
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {

		int result = 17;

		result += 31 * ObjectUtils.nullSafeHashCode(this.columns);

		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {

		Iterator<Entry<ColumnName, Selector>> iterator = this.columns.entrySet().iterator();
		StringBuilder builder = toString(iterator);

		if (builder.length() == 0) {
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

			if (expression == InternalSelectors.Exclude) {
				continue;
			}

			if (first) {
				first = false;
			} else {
				builder.append(", ");
			}

			Optional<CqlIdentifier> columnName = getCqlIdentifier(entry.getKey());

			columnName.map(expression::evaluate).ifPresent(builder::append);
		}

		return builder;
	}

	private Optional<CqlIdentifier> getCqlIdentifier(ColumnName columnName) {

		if (columnName.getCqlIdentifier().isPresent()) {
			return columnName.getCqlIdentifier();
		}

		return columnName.getColumnName().map(CqlIdentifier::cqlId);
	}

	/**
	 * Strategy interface to render a column selector.
	 *
	 * @author Mark Paluch
	 */
	public interface Selector {

		/**
		 * Render a column selector given {@link CqlIdentifier}.
		 *
		 * @param columnName must not be {@literal null}.
		 * @return the column selector to be used with CQL.
		 */
		Column evaluate(CqlIdentifier columnName);
	}

	/**
	 * Internal set of {@link Selector}.
	 *
	 * @author Mark Paluch
	 */
	enum InternalSelectors implements Selector {

		/**
		 * Exclude column from selection.
		 */
		Exclude {
			@Override
			public Column evaluate(CqlIdentifier columnName) {
				throw new UnsupportedOperationException();
			}
		},
	}

	/**
	 * Commonly used {@link Selector}s.
	 *
	 * @author Mark Paluch
	 */
	public enum Selectors implements Selector {

		/**
		 * Plain inclusion.
		 */
		Include {
			@Override
			public Column evaluate(CqlIdentifier columnName) {
				return Column.of(columnName.toCql());
			}
		},

		/**
		 * Select the TTL for a column.
		 */
		TTL {
			@Override
			public FunctionCall evaluate(CqlIdentifier columnName) {
				return FunctionCall.of("TTL", Column.of(columnName.toCql()));
			}
		},

		/**
		 * Select the WRITETIME for a column.
		 */
		WRITETIME {
			@Override
			public FunctionCall evaluate(CqlIdentifier columnName) {
				return FunctionCall.of("WRITETIME", Column.of(columnName.toCql()));
			}
		}
	}

	/**
	 * Column selector with alias support.
	 */
	@EqualsAndHashCode
	public static class Column {

		private final String expression;
		private final Optional<CqlIdentifier> alias;

		Column(String expression) {

			this.expression = expression;
			this.alias = Optional.empty();
		}

		Column(String expression, CqlIdentifier alias) {
			this.expression = expression;
			this.alias = Optional.of(alias);
		}

		public static Column of(String expression) {
			return new Column(expression);
		}

		public static Column of(String expression, CqlIdentifier alias) {
			return new Column(expression, alias);
		}

		public Column as(String alias) {
			return new Column(getExpression(), CqlIdentifier.cqlId(alias));
		}

		public String getExpression() {
			return expression;
		}

		public Optional<CqlIdentifier> getAlias() {
			return alias;
		}

		/* (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return getAlias().map(cqlIdentifier -> String.format("%s AS %s", expression, cqlIdentifier.toCql()))
					.orElse(expression);
		}
	}

	/**
	 * Function call selector with alias support.
	 */
	@EqualsAndHashCode
	public static class FunctionCall extends Column {

		private final List<Object> params;

		FunctionCall(String expression, List<Object> params) {

			super(expression);

			this.params = params;
		}

		private FunctionCall(String expression, CqlIdentifier alias, List<Object> params) {
			super(expression, alias);
			this.params = params;
		}

		public static FunctionCall of(String expression, Object... params) {
			return new FunctionCall(expression, Arrays.asList(params));
		}

		public static FunctionCall of(String expression, CqlIdentifier alias, List<Object> params) {
			return new FunctionCall(expression, alias, params);
		}

		public FunctionCall as(String alias) {
			return new FunctionCall(getExpression(), CqlIdentifier.cqlId(alias), params);
		}

		public List<Object> getParameters() {
			return params;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.query.Columns.Column#toString()
		 */
		@Override
		public String toString() {

			String params = StringUtils.collectionToDelimitedString(getParameters(), ", ");

			return getAlias().map(cqlIdentifier -> {
				return String.format("%s(%s) AS %s", getExpression(), params, cqlIdentifier.toCql());
			}).orElseGet(() -> {
				return String.format("%s(%s)", getExpression(), params);
			});
		}
	}
}
