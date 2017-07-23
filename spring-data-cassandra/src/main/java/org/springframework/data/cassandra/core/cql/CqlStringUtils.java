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
package org.springframework.data.cassandra.core.cql;

import com.datastax.driver.core.DataType;

public class CqlStringUtils {

	protected static final String DOUBLE_QUOTE = "\"";
	protected static final String DOUBLE_DOUBLE_QUOTE = "\"\"";
	protected static final String DOUBLE_SINGLE_QUOTE = "\'\'";
	protected static final String SINGLE_QUOTE = "\'";
	protected static final String EMPTY_STRING = "";
	protected static final String TYPE_PARAMETER_PREFIX = "<";
	protected static final String TYPE_PARAMETER_SUFFIX = ">";

	public static StringBuilder noNull(StringBuilder builder) {
		return (builder == null ? new StringBuilder() : builder);
	}

	/**
	 * Renders the given string as a legal Cassandra string column or table option value, by escaping single quotes and
	 * encasing the result in single quotes. Given {@code null}, returns <code>null</code>.
	 */
	public static String valuize(String candidate) {
		return (candidate != null ? singleQuote(escapeSingle(candidate)) : null);
	}

	/**
	 * Doubles single quote characters (' -&gt; ''). Given {@code null}, returns <code>null</code>.
	 */
	public static String escapeSingle(Object thing) {
		return (thing == null ? null : thing.toString().replace(SINGLE_QUOTE, DOUBLE_SINGLE_QUOTE));
	}

	/**
	 * Doubles double quote characters (" -&gt; ""). Given {@code null}, returns <code>null</code>.
	 */
	public static String escapeDouble(Object thing) {
		return (thing == null ? null : thing.toString().replace(DOUBLE_QUOTE, DOUBLE_DOUBLE_QUOTE));
	}

	/**
	 * Surrounds given object's {@link Object#toString()} with single quotes. Given {@code null}, returns {@code null}.
	 */
	public static String singleQuote(Object thing) {
		return (thing == null ? null : SINGLE_QUOTE.concat(thing.toString()).concat(SINGLE_QUOTE));
	}

	/**
	 * Surrounds given object's {@link Object#toString()} with double quotes. Given {@code null}, returns {@code null}.
	 */
	public static String doubleQuote(Object thing) {
		return (thing == null ? null : DOUBLE_QUOTE.concat(thing.toString()).concat(DOUBLE_QUOTE));
	}

	/**
	 * Removed single quotes from quoted String option values
	 */
	public static String removeSingleQuotes(Object thing) {
		return (thing == null ? null : thing.toString().replaceAll(SINGLE_QUOTE, EMPTY_STRING));
	}

	/**
	 * Renders the given {@link DataType} as a CQL string.
	 *
	 * @param dataType The {@link DataType} to render; must not be null.
	 */
	public static String toCql(DataType dataType) {

		if (dataType.getTypeArguments().isEmpty()) {
			return dataType.getName().name();
		}

		StringBuilder builder = new StringBuilder();

		builder.append(dataType.getName().name()).append(TYPE_PARAMETER_PREFIX);

		boolean first = true;

		for (DataType argDataType : dataType.getTypeArguments()) {

			if (first) {
				first = false;
			} else {
				builder.append(',');
			}

			builder.append(argDataType.getName().name());
		}

		return builder.append(TYPE_PARAMETER_SUFFIX).toString();
	}

	public static String unquote(String value) {
		return unquote(value, "\"");
	}

	public static String unquote(String value, String quoteChar) {

		if (value == null) {
			return null;
		}

		if (!value.startsWith(quoteChar) || !value.endsWith(quoteChar)) {
			return value;
		}

		if (value.length() <= 2) {
			return value;
		}

		return value.substring(1, value.length() - 1);
	}
}
