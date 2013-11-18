package org.springframework.data.cassandra.cql.builder;

import static org.springframework.data.cassandra.cql.CqlStringUtils.escapeSingle;
import static org.springframework.data.cassandra.cql.CqlStringUtils.singleQuote;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

/**
 * A default implementation of {@link Option} to which {@link Enum} types can delegate, since they can't extend
 * anything.
 * 
 * @author Matthew T. Adams
 */
public class DefaultOption implements Option {

	private String name;
	private Class<?> type;
	private boolean requiresValue;
	private boolean escapesValue;
	private boolean quotesValue;
	private HashSet<Object> enumConstants; // HACK for enums only

	public DefaultOption(String name, Class<?> type, boolean requiresValue, boolean escapesValue, boolean quotesValue) {
		this.name = name;
		if (type != null) {
			if (type.isInterface() && !(Map.class.isAssignableFrom(type) || Collection.class.isAssignableFrom(type))) {
				throw new IllegalArgumentException("given type [" + type.getName() + "] must be a class, Map or Collection");
			}
			// HACK for enums only
			if (type.isEnum()) {
				enumConstants = new HashSet<Object>(Arrays.asList(type.getEnumConstants()));
			}
		}
		this.type = type;
		this.requiresValue = requiresValue;
		this.escapesValue = escapesValue;
		this.quotesValue = quotesValue;
	}

	public boolean isCoerceable(Object value) {
		if (value == null) {
			return true;
		}

		// check collections
		if (Map.class.isAssignableFrom(type)) {
			return Map.class.isAssignableFrom(value.getClass());
		}
		// check map
		if (Collection.class.isAssignableFrom(type)) {
			return Collection.class.isAssignableFrom(value.getClass());
		}
		// check enum
		if (type.isEnum()) {
			// HACK -- prefer to use Enum.valueOf(type, stringValue), but can't
			return enumConstants.contains(value.toString());
		}

		// check class via String constructor
		try {
			Constructor<?> ctor = type.getConstructor(String.class);
			if (!ctor.isAccessible()) {
				ctor.setAccessible(true);
			}
			ctor.newInstance(value.toString());
			return true;
		} catch (InstantiationException e) {
		} catch (IllegalAccessException e) {
		} catch (IllegalArgumentException e) {
		} catch (InvocationTargetException e) {
		} catch (NoSuchMethodException e) {
		} catch (SecurityException e) {
		}
		return false;
	}

	public Class<?> getType() {
		return type;
	}

	public String getName() {
		return name;
	}

	public boolean takesValue() {
		return type != null;
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

	public void checkValue(Object value) {
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
			throw new IllegalArgumentException("Option [" + getName() + "] takes value coerceable to type ["
					+ getType().getName() + "]");
		}
		// else this option doesn't take a value
		if (value != null) {
			throw new IllegalArgumentException("Option [" + getName() + "] takes no value");
		}
	}

	public String toString(Object value) {
		if (value == null) {
			return null;
		}
		String string = value.toString();
		string = escapesValue ? escapeSingle(string) : string;
		string = quotesValue ? singleQuote(string) : string;
		return string;
	}

	@Override
	public String toString() {
		return getName();
	}
}
