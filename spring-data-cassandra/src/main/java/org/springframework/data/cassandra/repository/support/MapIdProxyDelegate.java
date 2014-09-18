package org.springframework.data.cassandra.repository.support;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.springframework.data.cassandra.repository.MapId;
import org.springframework.util.StringUtils;

class MapIdProxyDelegate implements InvocationHandler {

	static class Signature {
		String name;
		Class<?>[] argTypes;
		Class<?> returnType;

		Signature(Method method, boolean includeReturnType) {
			this(method.getName(), method.getParameterTypes(), includeReturnType ? method.getReturnType() : null);
		}

		Signature(String name, Class<?>[] argTypes, Class<?> returnType) {
			this.name = name;
			this.argTypes = argTypes;
			this.returnType = returnType;
		}

		@Override
		public String toString() {
			return String.format("%s %s(%s)", returnType, name, Arrays.toString(argTypes));
		}

		@Override
		public boolean equals(Object that) {
			if (that == null) {
				return false;
			}
			if (this == that) {
				return true;
			}
			if (!(that instanceof Signature)) {
				return false;
			}
			Signature that_ = (Signature) that;
			if (!this.name.equals(that_.name)) {
				return false;
			}
			if ((this.argTypes == null && that_.argTypes != null) || (this.argTypes != null && that_.argTypes == null)) {
				return false;
			}
			if (this.argTypes != null) {
				if (this.argTypes.length != that_.argTypes.length) {
					return false;
				}
				for (int i = 0; i < this.argTypes.length; i++) {
					if (!this.argTypes[i].equals(that_.argTypes[i])) {
						return false;
					}
				}
			}
			if (this.returnType == null) {
				return that_.returnType == null;
			}
			return this.returnType.equals(that_.returnType);
		}

		@Override
		public int hashCode() {
			int hash = 37 ^ name.hashCode();
			if (argTypes != null) {
				for (Class<?> c : argTypes) {
					hash ^= c.hashCode();
				}
			}
			if (returnType != null) {
				hash ^= returnType.hashCode();
			}
			return hash;
		}
	}

	private static final Map<Signature, Signature> MAP_ID_SIGNATURES;

	static {
		Method[] mapIdMethods = MapId.class.getMethods();
		MAP_ID_SIGNATURES = new HashMap<Signature, Signature>(mapIdMethods.length);
		for (Method m : mapIdMethods) {
			Signature s = new Signature(m, true);
			MAP_ID_SIGNATURES.put(s, s);
		}
	}

	MapId delegate = new BasicMapId();
	Class<?> idInterface;

	MapIdProxyDelegate(Class<?> idInterface) {
		this.idInterface = idInterface;
	}

	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		if (isMapIdMethod(method)) {
			return method.invoke(delegate, args);
		}

		if (args != null && args.length > 1) {
			throw new IllegalArgumentException(String.format("Method [%s] on interface [%s] must take zero or one argument",
					method, idInterface));
		}
		boolean isSetter = args != null && args.length == 1;

		if (isSetter) {
			invokeSetter(method, args[0]);
			return void.class.equals(method.getReturnType()) ? null : proxy;
		}

		return invokeGetter(method);
	}

	private boolean isMapIdMethod(Method method) {
		return MAP_ID_SIGNATURES.containsKey(new Signature(method, true));
	}

	private Serializable invokeGetter(Method method) {
		String name = method.getName();
		if (name.startsWith("get")) {
			if (name.length() == 3) {
				throw new IllegalArgumentException(String.format("Method [%s] on interface [%s] must be of form "
						+ "'<PropertyType> get<PropertyName>()' or " + "'<PropertyType> <propertyName>()'", name, idInterface));
			}
			name = StringUtils.uncapitalize(name.substring(3));
		}

		return delegate.get(name);
	}

	private void invokeSetter(Method method, Object value) {
		String name = method.getName();
		int minLength = 1;
		boolean isSet = name.startsWith("set");
		boolean isWith = name.startsWith("with");
		minLength += isSet ? 3 : isWith ? 4 : 0;
		int length = name.length();
		if (isSet || isWith) {
			if (length < minLength) {
				throw new IllegalArgumentException(String.format("Method [%s] on interface [%s] must be of form "
						+ "'<IdType|void> set<PropertyName>(<PropertyType>)', "
						+ "'<IdType|void> with<PropertyName>(<PropertyType>)' or "
						+ "'<IdType|void> <propertyName>(<PropertyType>)'", name, idInterface));
			}
			name = StringUtils.uncapitalize(name.substring(minLength - 1));
		}

		if (value == null) {
			delegate.put(name, null);
			return;
		}
		if (!(value instanceof Serializable)) {
			throw new IllegalArgumentException(String.format("Given object [%s] must implement %s", value,
					Serializable.class.getName()));
		}
		delegate.put(name, (Serializable) value);
	}
}
