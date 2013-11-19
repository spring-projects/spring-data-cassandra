package org.springframework.data.cassandra.cql.builder;

import static org.springframework.data.cassandra.cql.CqlStringUtils.escapeSingle;
import static org.springframework.data.cassandra.cql.CqlStringUtils.singleQuote;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Map;

import org.springframework.util.Assert;

/**
 * A default implementation of {@link Option}.
 * 
 * @author Matthew T. Adams
 */
public class DefaultOption implements Option {

	private String name;
	private Class<?> type;
	private boolean requiresValue;
	private boolean escapesValue;
	private boolean quotesValue;

	public DefaultOption(String name, Class<?> type, boolean requiresValue, boolean escapesValue, boolean quotesValue) {
		setName(name);
		setType(type);
		this.requiresValue = requiresValue;
		this.escapesValue = escapesValue;
		this.quotesValue = quotesValue;

	}

	protected void setName(String name) {
		Assert.hasLength(name);
		this.name = name;
	}

	protected void setType(Class<?> type) {
		if (type != null) {
			if (type.isInterface() && !(Map.class.isAssignableFrom(type) || Collection.class.isAssignableFrom(type))) {
				throw new IllegalArgumentException("given type [" + type.getName() + "] must be a class, Map or Collection");
			}
		}
		this.type = type;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public boolean isCoerceable(Object value) {
		if (value == null || type == null) {
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
			try {
				String name = value instanceof Enum ? name = ((Enum) value).name() : value.toString();
				Enum.valueOf((Class<? extends Enum>) type, name);
				return true;
			} catch (NullPointerException x) {
				return false;
			} catch (IllegalArgumentException x) {
				return false;
			}
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
