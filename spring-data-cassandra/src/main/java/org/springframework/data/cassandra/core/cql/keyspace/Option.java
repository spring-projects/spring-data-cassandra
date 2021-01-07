/*
 * Copyright 2013-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core.cql.keyspace;

import org.springframework.lang.Nullable;

/**
 * Interface to represent option types.
 *
 * @author Matthew T. Adams
 */
public interface Option {

	/**
	 * The type that values must be able to be coerced into for this option.
	 */
	Class<?> getType();

	/**
	 * The (usually lower-cased, underscore-separated) name of this table option.
	 */
	String getName();

	/**
	 * Whether this option takes a value.
	 */
	boolean takesValue();

	/**
	 * Whether this option should escape single quotes in its value.
	 */
	boolean escapesValue();

	/**
	 * Whether this option's value should be single-quoted.
	 */
	boolean quotesValue();

	/**
	 * Whether this option requires a value.
	 */
	boolean requiresValue();

	/**
	 * Checks that the given value can be coerced into the type given by {@link #getType()}.
	 */
	void checkValue(Object value);

	/**
	 * Tests whether the given value can be coerced into the type given by {@link #getType()}.
	 */
	boolean isCoerceable(Object value);

	/**
	 * First ensures that the given value is coerceable into the type expected by this option, then returns the result of
	 * {@link Object#toString()} called on the given value. If this option is escaping quotes ({@link #escapesValue()} is
	 * {@code true}), then single quotes will be escaped, and if this option is quoting values ( {@link #quotesValue()} is
	 * {@code true}), then the value will be surrounded by single quotes. Given {@literal null}, returns an empty string.
	 *
	 * @see #escapesValue()
	 * @see #quotesValue()
	 */
	String toString(@Nullable Object value);
}
