/*
 * Copyright 2019-present the original author or authors.
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

import java.util.function.Function;
import java.util.function.Supplier;

import org.jspecify.annotations.Nullable;

import com.datastax.oss.driver.api.querybuilder.term.Term;

/**
 * Factory for {@link Term} objects encapsulating a binding {@code value}. Classes implementing this factory interface
 * may return inline terms to render values as part of the query string, or bind markers to supply parameters on
 * statement creation/parameter binding.
 * <p>
 * A {@link TermFactory} is typically used with {@link StatementBuilder}.
 *
 * @author Mark Paluch
 * @since 3.0
 */
@FunctionalInterface
public interface TermFactory {

	/**
	 * Create a {@link Term} for the given {@code value}.
	 *
	 * @param value the value to bind, can be {@literal null}.
	 * @return the {@link Term} for the given {@code value}.
	 */
	Term create(@Nullable Object value);

	/**
	 * Check whether the term factory accepts {@link java.util.Collection} values to be created as {@link Term}.
	 *
	 * @return {@code true} whether the term factory can {@link java.util.Collection} values.
	 * @since 3.2.6
	 */
	default boolean canBindCollection() {
		return true;
	}

	/**
	 * Apply functions depending on whether the term factory uses bind markers or inline values.
	 *
	 * @param bindingFunction the binding functions to apply with {@link Bindings} for bind marker capturing.
	 * @param inlineFunction the function to run otherwise, if the term factory uses inline values.
	 * @return the outcome of the binding operation.
	 * @param <T>
	 * @since 4.2
	 */
	default <T> T ifBoundOrInline(Function<Bindings, T> bindingFunction, Supplier<T> inlineFunction) {
		return inlineFunction.get();
	}

}
