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
package org.springframework.data.cassandra.core.cql;

import java.util.regex.Pattern;

import static org.springframework.data.cassandra.core.cql.CqlConstantType.Regex.*;

public enum CqlConstantType {

	STRING(STRING_PATTERN), INTEGER(INTEGER_PATTERN), FLOAT(FLOAT_PATTERN), BOOLEAN(BOOLEAN_PATTERN), UUID(
			UUID_PATTERN), BLOB(BLOB_PATTERN);

	private Pattern pattern;

	CqlConstantType(Pattern pattern) {
		this.pattern = pattern;
	}

	public boolean matches(CharSequence candidate) {
		return pattern.matcher(candidate).matches();
	}

	public static class Regex {

		public static final String STRING_REGEX = "'((?:[^']+|'')*)'";
		public static final Pattern STRING_PATTERN = Pattern.compile(STRING_REGEX);

		public static final String INTEGER_REGEX = "\\-?[0-9]+";
		public static final Pattern INTEGER_PATTERN = Pattern.compile(INTEGER_REGEX);

		public static final String FLOAT_REGEX = "(\\-?[0-9]+(\\.[0-9]*)?([eE][+-]?[0-9+])?)|NaN|Infinity";
		public static final Pattern FLOAT_PATTERN = Pattern.compile(FLOAT_REGEX);

		public static final String BOOLEAN_REGEX = "(?i)true|false";
		public static final Pattern BOOLEAN_PATTERN = Pattern.compile(BOOLEAN_REGEX);

		public static final String UUID_REGEX = "(?i)[0-9a-f]{8}+\\-[0-9a-f]{4}+\\-[0-9a-f]{4}+\\-[0-9a-f]{4}+\\-[0-9a-f]{12}+";
		public static final Pattern UUID_PATTERN = Pattern.compile(UUID_REGEX);

		public static final String BLOB_REGEX = "(?i)0[x](0-9a-f)+";
		public static final Pattern BLOB_PATTERN = Pattern.compile(BLOB_REGEX);
	}
}
