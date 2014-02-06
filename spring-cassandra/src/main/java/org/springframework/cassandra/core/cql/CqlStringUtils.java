/*
 * Copyright 2011-2013 the original author or authors.
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
package org.springframework.cassandra.core.cql;

import java.util.regex.Pattern;

import com.datastax.driver.core.DataType;

public class CqlStringUtils {

	protected static final String SINGLE_QUOTE = "\'";
	protected static final String DOUBLE_SINGLE_QUOTE = "\'\'";
	protected static final String DOUBLE_QUOTE = "\"";
	protected static final String DOUBLE_DOUBLE_QUOTE = "\"\"";
	protected static final String EMPTY_STRING = "";
	protected static final String TYPE_PARAMETER_PREFIX = "<";
	protected static final String TYPE_PARAMETER_SUFFIX = ">";

	public static StringBuilder noNull(StringBuilder sb) {
		return sb == null ? new StringBuilder() : sb;
	}

	public static final String UNESCAPED_DOUBLE_QUOTE_REGEX = "TODO";
	public static final Pattern UNESCAPED_DOUBLE_QUOTE_PATTERN = Pattern.compile(UNESCAPED_DOUBLE_QUOTE_REGEX);

	public static final String UNQUOTED_IDENTIFIER_REGEX = "[a-zA-Z_][a-zA-Z0-9_]*";
	public static final Pattern UNQUOTED_IDENTIFIER_PATTERN = Pattern.compile(UNQUOTED_IDENTIFIER_REGEX);

	public static boolean isUnquotedIdentifier(CharSequence chars) {
		return UNQUOTED_IDENTIFIER_PATTERN.matcher(chars).matches();
	}

	public static void checkUnquotedIdentifier(CharSequence chars) {
		if (!CqlStringUtils.isUnquotedIdentifier(chars)) {
			throw new IllegalArgumentException("[" + chars + "] is not a valid CQL identifier");
		}
	}

	public static final String QUOTED_IDENTIFIER_REGEX = "[a-zA-Z_]([a-zA-Z0-9_]|\"{2}+)*";
	public static final Pattern QUOTED_IDENTIFIER_PATTERN = Pattern.compile(QUOTED_IDENTIFIER_REGEX);

	public static boolean isQuotedIdentifier(CharSequence chars) {
		return QUOTED_IDENTIFIER_PATTERN.matcher(chars).matches();
	}

	public static void checkQuotedIdentifier(CharSequence chars) {
		if (!CqlStringUtils.isQuotedIdentifier(chars)) {
			throw new IllegalArgumentException("[" + chars + "] is not a valid CQL quoted identifier");
		}
	}

	public static boolean isIdentifier(CharSequence chars) {
		return isUnquotedIdentifier(chars) || isQuotedIdentifier(chars);
	}

	public static void checkIdentifier(CharSequence chars) {
		if (!CqlStringUtils.isIdentifier(chars)) {
			throw new IllegalArgumentException("[" + chars + "] is not a valid CQL quoted or unquoted identifier");
		}
	}

	/**
	 * Renders the given string as a legal Cassandra identifier.
	 * <ul>
	 * <li>If the given identifier is a legal unquoted identifier, it is returned unchanged.</li>
	 * <li>If the given identifier is a legal quoted identifier, it is returned encased in double quotes.</li>
	 * <li>If the given identifier is illegal, an {@link IllegalArgumentException} is thrown.</li>
	 * </ul>
	 */
	public static String identifize(String candidate) {

		checkIdentifier(candidate);

		if (isUnquotedIdentifier(candidate)) {
			return candidate;
		}
		// else it must be quoted
		return doubleQuote(candidate);
	}

	/**
	 * Renders the given string as a legal Cassandra string column or table option value, by escaping single quotes and
	 * encasing the result in single quotes. Given <code>null</code>, returns <code>null</code>.
	 */
	public static String valuize(String candidate) {

		if (candidate == null) {
			return null;
		}
		return singleQuote(escapeSingle(candidate));
	}

	/**
	 * Doubles single quote characters (' -&gt; ''). Given <code>null</code>, returns <code>null</code>.
	 */
	public static String escapeSingle(Object things) {
		return things == null ? (String) null : things.toString().replace(SINGLE_QUOTE, DOUBLE_SINGLE_QUOTE);
	}

	/**
	 * Doubles double quote characters (" -&gt; ""). Given <code>null</code>, returns <code>null</code>.
	 */
	public static String escapeDouble(Object things) {
		return things == null ? (String) null : things.toString().replace(DOUBLE_QUOTE, DOUBLE_DOUBLE_QUOTE);
	}

	/**
	 * Surrounds given object's {@link Object#toString()} with single quotes. Given <code>null</code>, returns
	 * <code>null</code>.
	 */
	public static String singleQuote(Object thing) {
		return thing == null ? (String) null : new StringBuilder().append(SINGLE_QUOTE).append(thing).append(SINGLE_QUOTE)
				.toString();
	}

	/**
	 * Surrounds given object's {@link Object#toString()} with double quotes. Given <code>null</code>, returns
	 * <code>null</code>.
	 */
	public static String doubleQuote(Object thing) {
		return thing == null ? (String) null : new StringBuilder().append(DOUBLE_QUOTE).append(thing).append(DOUBLE_QUOTE)
				.toString();
	}

	/**
	 * Removed single quotes from quoted String option values
	 */
	public static String removeSingleQuotes(Object thing) {
		return thing == null ? (String) null : ((String) thing).replaceAll(SINGLE_QUOTE, EMPTY_STRING);
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

		StringBuilder s = new StringBuilder();
		s.append(dataType.getName().name()).append(TYPE_PARAMETER_PREFIX);

		boolean first = true;

		for (DataType argDataType : dataType.getTypeArguments()) {

			if (first) {
				first = false;
			} else {
				s.append(',');
			}

			s.append(argDataType.getName().name());
		}

		return s.append(TYPE_PARAMETER_SUFFIX).toString();
	}
}
