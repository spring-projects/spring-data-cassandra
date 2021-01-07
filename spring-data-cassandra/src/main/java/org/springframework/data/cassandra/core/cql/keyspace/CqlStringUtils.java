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
package org.springframework.data.cassandra.core.cql.keyspace;

import org.springframework.lang.Nullable;

public class CqlStringUtils {

	private static final String DOUBLE_SINGLE_QUOTE = "\'\'";
	private static final String SINGLE_QUOTE = "\'";

	/**
	 * Doubles single quote characters (' -&gt; ''). Given {@literal null}, returns <code>null</code>.
	 */
	@Nullable
	public static String escapeSingle(@Nullable Object thing) {
		return (thing == null ? null : thing.toString().replace(SINGLE_QUOTE, DOUBLE_SINGLE_QUOTE));
	}

	/**
	 * Surrounds given object's {@link Object#toString()} with single quotes. Given {@literal null}, returns
	 * {@literal null}.
	 */
	@Nullable
	public static String singleQuote(@Nullable Object thing) {
		return (thing == null ? null : SINGLE_QUOTE.concat(thing.toString()).concat(SINGLE_QUOTE));
	}
}
