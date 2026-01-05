/*
 * Copyright 2013-present the original author or authors.
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

import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jspecify.annotations.Nullable;
import org.springframework.util.Assert;

/**
 * A default implementation of {@link Option}.
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
public class DefaultOption implements Option {

	protected final Log log = LogFactory.getLog(getClass());

	private final String name;

	private final Class<?> type;

	private final boolean requiresValue;

	private final boolean escapesValue;

	private final boolean quotesValue;

	public DefaultOption(String name, Class<?> type, boolean requiresValue, boolean escapesValue, boolean quotesValue) {

		Assert.hasText(name, "Name must not be null or empty");
		Assert.notNull(type, "Type must not be null");

		if (type.isInterface() && !(Map.class.isAssignableFrom(type) || Collection.class.isAssignableFrom(type))) {
			throw new IllegalArgumentException("given type [" + type.getName() + "] must be a class, Map or Collection");
		}

		this.name = name;
		this.type = type;

		this.requiresValue = requiresValue;
		this.escapesValue = escapesValue;
		this.quotesValue = quotesValue;

	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public boolean isCoerceable(Object value) {

		if (getType().equals(Void.class) || getType().equals(String.class)) {
			return true;
		}

		// check map
		if (Map.class.isAssignableFrom(type)) {
			return Map.class.isAssignableFrom(value.getClass());
		}
		// check collection
		if (Collection.class.isAssignableFrom(type)) {
			return Collection.class.isAssignableFrom(value.getClass());
		}
		// check enum
		if (type.isEnum()) {
			try {
				String name = value instanceof Enum ? name = ((Enum) value).name() : value.toString();
				Enum.valueOf((Class<? extends Enum>) type, name);
				return true;
			} catch (IllegalArgumentException | NullPointerException x) {
				return false;
			}
		}

		if (type == Long.class) {
			return tryParse(value, Long::parseLong);
		}
		if (type == Integer.class) {
			return tryParse(value, Integer::parseInt);
		}
		if (type == Double.class) {
			return tryParse(value, Double::parseDouble);
		}
		if (type == Float.class) {
			return tryParse(value, Float::parseFloat);
		}
		if (type == Boolean.class) {
			return tryParse(value, Boolean::valueOf);
		}

		// check class via String constructor
		try {
			Constructor<?> ctor = type.getConstructor(String.class);
			if (!ctor.canAccess(this)) {
				ctor.setAccessible(true);
			}
			ctor.newInstance(value.toString());
			return true;
		} catch (Exception e) {
			if (log.isDebugEnabled()) {
				log.debug("Cannot parse option %s into %s".formatted(getName(), getType()), e);
			}

		}
		return false;
	}

	private boolean tryParse(Object value, Consumer<String> parseFunction) {
		try {
			parseFunction.accept(value.toString());
			return true;
		} catch (RuntimeException e) {
			if (log.isDebugEnabled()) {
				log.debug("Cannot parse option %s into %s".formatted(getName(), getType()), e);
			}
			return false;
		}
	}

	public Class<?> getType() {
		return type;
	}

	public String getName() {
		return name;
	}

	public boolean takesValue() {
		return type != Void.class;
	}

	public boolean requiresValue() {
		return this.requiresValue;
	}

	public boolean escapesValue() {
		return this.escapesValue;
	}

	public boolean quotesValue() {
		return this.quotesValue;
	}

	public void checkValue(@Nullable Object value) {
		if (takesValue()) {
			if (value == null) {
				if (requiresValue) {
					throw new IllegalArgumentException("Option [" + getName() + "] requires a value");
				}
				return; // doesn't require a value, so null is ok
			}
			// else value is not null
			if (isCoerceable(value)) {
				return;
			}
			// else value is not coerceable into the expected type
			throw new IllegalArgumentException(
					"Option [" + getName() + "] takes value coerceable to type [" + getType() + "]");
		}
		// else this option doesn't take a value
		if (value != null) {
			throw new IllegalArgumentException("Option [" + getName() + "] takes no value");
		}
	}

	public String toString(@Nullable Object value) {

		if (value == null) {
			return "";
		}

		checkValue(value);

		String string = value.toString();
		string = escapesValue ? escapeSingle(string) : string;
		string = quotesValue ? singleQuote(string) : string;
		return string;
	}

	@Override
	public String toString() {
		return "[name=" + name + ", type=" + type.getName() + ", requiresValue=" + requiresValue + ", escapesValue="
				+ escapesValue + ", quotesValue=" + quotesValue + "]";
	}
}
