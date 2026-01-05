/*
 * Copyright 2023-present the original author or authors.
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
package org.springframework.data.cassandra.core.cql.util;

import org.jspecify.annotations.Nullable;

import com.datastax.oss.driver.api.querybuilder.BindMarker;

/**
 * Factory for {@link BindMarker} capturing binding {@code value}s.
 * <p>
 * A {@link Bindings} object is typically used with {@link StatementBuilder}.
 *
 * @author Mark Paluch
 * @since 4.2
 */
@FunctionalInterface
public interface Bindings {

	/**
	 * Create a {@link BindMarker} for the given {@code value}. Using bindings with positional bind markers must consider
	 * the usage order within a statement.
	 *
	 * @param value the value to bind, can be {@literal null}.
	 * @return the {@link BindMarker} for the given {@code value}.
	 */
	BindMarker bind(@Nullable Object value);

}
