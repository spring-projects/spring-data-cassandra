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
package org.springframework.data.cassandra.core.cql.keyspace;

import org.jspecify.annotations.Nullable;

import org.springframework.lang.Contract;

import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;

/**
 * Utility class for CQL string manipulation.
 *
 * @author Mark Paluch
 */
public class CqlStringUtils {

	private static final String DOUBLE_SINGLE_QUOTE = "\'\'";
	private static final String SINGLE_QUOTE = "\'";

	/**
	 * Render the given {@code value} as a CQL-safe string literal potentially quoting and escaping the value.
	 * <p>
	 * A {@code toString(value, true, true)} is equivalent to {@link #toCqlLiteral(Object)}.
	 *
	 * @param value the value to render.
	 * @param escape {@literal true} to escape single quotes. {@literal false} to render the value as-is.
	 * @param quote {@literal true} to quote the value. {@literal false} to render the value as-is.
	 * @return the rendered CQL string literal, or {@literal null} if {@code value} is {@literal null}.
	 * @since 5.1.2
	 */
	@Contract("null, _, _ -> null; !null, _, _ -> !null")
	public static @Nullable String toString(@Nullable Object value, boolean escape, boolean quote) {

		if (value == null) {
			return null;
		}

		if (escape && quote) {
			return toCqlLiteral(value);
		} else if (escape) {
			return escapeSingle(value);
		} else if (quote) {
			return singleQuote(value);
		}

		return value.toString();
	}

	/**
	 * Convert the given value into a single-quoted CQL string literal.
	 * <p>
	 * Embedded single quotes are escaped and a {@literal null} value is preserved to distinguish it from an empty string
	 * literal.
	 *
	 * @param value the value to quote.
	 * @return the quoted CQL string literal, or {@literal null} if {@code value} is {@literal null}.
	 * @since 5.1.2
	 */
	@Contract("null -> null; !null -> !null")
	public @Nullable static String toCqlLiteral(@Nullable Object value) {
		return value == null ? null : TypeCodecs.TEXT.format(value.toString());
	}

	/**
	 * Doubles single quote characters (' -&gt; '').
	 *
	 * @param value the value to escape.
	 * @return the escaped CQL string literal, or {@literal null} if {@code value} is {@literal null}.
	 */
	@Contract("null -> null; !null -> !null")
	public @Nullable static String escapeSingle(@Nullable Object value) {
		return (value == null ? null : value.toString().replace(SINGLE_QUOTE, DOUBLE_SINGLE_QUOTE));
	}

	/**
	 * Surrounds given object's {@link Object#toString()} with single quotes.
	 *
	 * @param value the value to quote.
	 * @return the quoted CQL string literal, or {@literal null} if {@code value} is {@literal null}.
	 */
	@Contract("null -> null; !null -> !null")
	public @Nullable static String singleQuote(@Nullable Object value) {
		return (value == null ? null : SINGLE_QUOTE.concat(value.toString()).concat(SINGLE_QUOTE));
	}

}
