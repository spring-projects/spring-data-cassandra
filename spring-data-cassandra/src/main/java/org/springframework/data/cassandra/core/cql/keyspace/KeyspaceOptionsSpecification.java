/*
 * Copyright 2013-2020 the original author or authors.
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

import static org.springframework.data.cassandra.core.cql.keyspace.CqlStringUtils.*;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.lang.Nullable;
import org.springframework.util.ObjectUtils;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * Abstract builder class to support the construction of table specifications that have table options, that is, those
 * options normally specified by {@code WITH ... AND ...}.
 * <p/>
 * It is important to note that although this class depends on {@link KeyspaceOption} for convenient and typesafe use,
 * it ultimately stores its options in a <code>Map<String,Object></code> for flexibility. This means that
 * {@link #with(KeyspaceOption)} and {@link #with(KeyspaceOption, Object)} delegate to
 * {@link #with(String, Object, boolean, boolean)}. This design allows the API to support new Cassandra options as they
 * are introduced without having to update the code immediately.
 *
 * @author John McPeek
 * @param <T> The subtype of the {@link KeyspaceOptionsSpecification}.
 */
public abstract class KeyspaceOptionsSpecification<T extends KeyspaceOptionsSpecification<T>>
		extends KeyspaceActionSpecification {

	protected Map<String, Object> options = new LinkedHashMap<>();

	protected KeyspaceOptionsSpecification(CqlIdentifier name) {
		super(name);
	}

	/**
	 * Convenience method that calls {@code with(option, null)}.
	 *
	 * @return this
	 */
	public T with(KeyspaceOption option) {
		return with(option.getName(), null, option.escapesValue(), option.quotesValue());
	}

	/**
	 * Sets the given table option. This is a convenience method that calls
	 * {@link #with(String, Object, boolean, boolean)} appropriately from the given {@link KeyspaceOption} and value for
	 * that option.
	 *
	 * @param option The option to set.
	 * @param value The value of the option. Must be type-compatible with the {@link KeyspaceOption}.
	 * @return this
	 * @see #with(String, Object, boolean, boolean)
	 */
	public T with(KeyspaceOption option, Object value) {
		option.checkValue(value);
		return with(option.getName(), value, option.escapesValue(), option.quotesValue());
	}

	/**
	 * Adds the given option by name to this keyspaces's options.
	 * <p/>
	 * Options that have {@literal null} values are considered single string options where the name of the option is the
	 * string to be used. Otherwise, the result of {@link Object#toString()} is considered to be the value of the option
	 * with the given name. The value, after conversion to string, may have embedded single quotes escaped according to
	 * parameter {@code escape} and may be single-quoted according to parameter <code>quote</code>.
	 *
	 * @param name The name of the option
	 * @param value The value of the option. If {@literal null}, the value is ignored and the option is considered to be
	 *          composed of only the name, otherwise the value's {@link Object#toString()} value is used.
	 * @param escape Whether to escape the value via {@link CqlStringUtils#escapeSingle(Object)}. Ignored if given value
	 *          is an instance of a {@link Map}.
	 * @param quote Whether to quote the value via {@link CqlStringUtils#singleQuote(Object)}. Ignored if given value is
	 *          an instance of a {@link Map}.
	 * @return this
	 */
	@SuppressWarnings("unchecked")
	public T with(String name, @Nullable Object value, boolean escape, boolean quote) {

		if (!(value instanceof Map)) {
			if (escape) {
				value = escapeSingle(value);
			}
			if (quote) {
				value = singleQuote(value);
			}
		}
		options.put(name, value);
		return (T) this;
	}

	public Map<String, Object> getOptions() {
		return Collections.unmodifiableMap(options);
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

		if (!(o instanceof KeyspaceOptionsSpecification)) {
			return false;
		}

		if (!super.equals(o)) {
			return false;
		}

		KeyspaceOptionsSpecification<?> that = (KeyspaceOptionsSpecification<?>) o;
		return ObjectUtils.nullSafeEquals(options, that.options);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public int hashCode() {
		int result = super.hashCode();
		result = 31 * result + ObjectUtils.nullSafeHashCode(options);
		return result;
	}
}
