/*
 * Copyright 2017-2020 the original author or authors.
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import com.datastax.oss.driver.api.core.CqlIdentifier;

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

		Map<ColumnName, Selector> columns = new HashMap<>(columnNames.length, 1);

		Arrays.stream(columnNames)
				.forEach(columnName -> columns.put(ColumnName.from(columnName), ColumnSelector.from(columnName)));

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

		Arrays.stream(columnNames).forEach(cqlId -> columns.put(ColumnName.from(cqlId), ColumnSelector.from(cqlId)));

		return new Columns(columns);
	}

	/**
	 * Include column {@code columnName} to the selection. Column inclusion overrides an existing selection for the column
	 * name.
	 *
	 * @param columnName must not be {@literal null}.
	 * @return a new {@link Columns} object containing all column definitions and the included {@code columnName}.
	 */
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
	public Columns select(String columnName, Selector selector) {

		Assert.notNull(columnName, "Column name must not be null");

		Map<ColumnName, Selector> result = new LinkedHashMap<>(this.columns);
		result.put(ColumnName.from(columnName), selector);

		return new Columns(result);
	}

	/**
	 * Include column {@code columnName} with {@link Selector}. This column selection overrides an existing selection for
	 * the column name.
	 *
	 * @param columnName must not be {@literal null}.
	 * @return a new {@link Columns} object containing all column definitions and the selected {@code columnName}.
	 */
	public Columns select(CqlIdentifier columnName, Selector selector) {

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
	 * Returns a new {@link Columns} consisting of the {@link ColumnName}s of the current {@link Columns} combined with
	 * the given ones. Existing {@link ColumnName}s are overwritten if specified within {@code columns}.
	 *
	 * @param columns must not be {@literal null}.
	 * @return a new {@link Columns} with the merged result of the configured and given {@link Columns}.
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
	 * @return the {@link Optional} {@link Selector} for {@link ColumnName}.
	 */
	public Optional<Selector> getSelector(ColumnName columnName) {

		Assert.notNull(columnName, "ColumnName must not be null");

		return Optional.ofNullable(this.columns.get(columnName));
	}

	/*
	 * (non-Javadoc)
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

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public int hashCode() {
		int result = 17;
		result += 31 * ObjectUtils.nullSafeHashCode(this.columns);
		return result;
	}

	/*
	 * (non-Javadoc)
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
	 * Strategy interface to render a column selection.
	 *
	 * @author Mark Paluch
	 */
	public interface Selector {

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
		 * @param alias must not be {@literal null} or empty.
		 * @return the aliased {@link ColumnSelector}.
		 */
		public ColumnSelector as(String alias) {
			return as(CqlIdentifier.fromCql(alias));
		}

		/**
		 * Create a {@link ColumnSelector} for the current {@link #getExpression() expression} aliased as {@code alias}.
		 *
		 * @param alias must not be {@literal null}.
		 * @return the aliased {@link ColumnSelector}.
		 */
		public ColumnSelector as(CqlIdentifier alias) {
			return new ColumnSelector(columnName, alias);
		}

		public Optional<CqlIdentifier> getAlias() {
			return alias;
		}

		public String getExpression() {
			return columnName.toCql();
		}

		/* (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return getAlias().map(cqlIdentifier -> String.format("%s AS %s", getExpression(), cqlIdentifier))
					.orElseGet(this::getExpression);
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
			if (!(o instanceof ColumnSelector)) {
				return false;
			}
			ColumnSelector that = (ColumnSelector) o;
			if (!ObjectUtils.nullSafeEquals(columnName, that.columnName)) {
				return false;
			}
			return ObjectUtils.nullSafeEquals(alias, that.alias);
		}

		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		@Override
		public int hashCode() {
			int result = ObjectUtils.nullSafeHashCode(columnName);
			result = 31 * result + ObjectUtils.nullSafeHashCode(alias);
			return result;
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
		 * @param alias must not be {@literal null} or empty.
		 * @return the aliased {@link ColumnSelector}.
		 */
		public FunctionCall as(String alias) {
			return as(CqlIdentifier.fromCql(alias));
		}

		/**
		 * Create a {@link FunctionCall} for the current {@link #getExpression() expression} aliased as {@code alias}.
		 *
		 * @param alias must not be {@literal null}.
		 * @return the aliased {@link ColumnSelector}.
		 */
		public FunctionCall as(CqlIdentifier alias) {
			return new FunctionCall(expression, params, alias);
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

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.query.Columns.Column#toString()
		 */
		@Override
		public String toString() {

			String parameters = StringUtils.collectionToDelimitedString(getParameters(), ", ");

			return getAlias()
					.map(cqlIdentifier -> String.format("%s(%s) AS %s", getExpression(), parameters, cqlIdentifier))
					.orElseGet(() -> String.format("%s(%s)", getExpression(), parameters));
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

		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		@Override
		public int hashCode() {
			int result = ObjectUtils.nullSafeHashCode(expression);
			result = 31 * result + ObjectUtils.nullSafeHashCode(params);
			result = 31 * result + ObjectUtils.nullSafeHashCode(alias);
			return result;
		}
	}
}
