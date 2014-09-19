package org.springframework.data.cassandra.repository.support;

import java.io.Serializable;
import java.lang.reflect.Method;

import org.springframework.data.cassandra.repository.MapId;

public class IdInterfaceValidator {

	/**
	 * Validates the form of the given id interface candidate type. If the interface violates the following restrictions,
	 * then an {@link IdInterfaceExceptions} is thrown containing all of the violations encountered, which can be obtained
	 * from {@link IdInterfaceExceptions#getExceptions()} or, as a convenience,
	 * {@link IdInterfaceExceptions#getMessages()}.
	 * <p/>
	 * Id interfaces are intended to have methods representing setters and getters. Getter methods take the form
	 * <ul>
	 * <li><code>PropertyType getPropertyName()</code> or</li>
	 * <li><code>PropertyType propertyName()</code>.</li>
	 * </ul>
	 * Setter methods take the form
	 * <ul>
	 * <li><code>void|IdType setPropertyName(PropertyType)</code>,</li>
	 * <li><code>void|IdType withPropertyName(PropertyType)</code>, or</li>
	 * <li><code>void|IdType propertyName(PropertyType)</code>;</li>
	 * </ul>
	 * setter methods may also declare that they return their id interface type to support method chaining.
	 * <p/>
	 * Id interfaces
	 * <ul>
	 * <li>must be an <code>interface</code>, not a <code>class</code>,</li>
	 * <li>may extend {@link Serializable},</li>
	 * <li>may extend {@link MapId},</li>
	 * <li>must have getter methods that only return a {@link Serializable} type,</li>
	 * <li>must have setter methods that only take a single {@link Serializable} type,</li>
	 * <li>must have setter methods that only return <code>void</code> or the id interface's type,</li>
	 * <li>must not define any getter methods with the literal name "get", and</li>
	 * <li>must not define any setter methods with the literal names "set" or "with".</li>
	 * </ul>
	 * 
	 * @param id The candidate interface type
	 * @throws IdInterfaceExceptions
	 * @see IdInterfaceExceptions#getExceptions()
	 * @see {@link IdInterfaceException}
	 */
	public static void validate(Class<?> id) {

		IdInterfaceExceptions x = new IdInterfaceExceptions(id);

		if (!id.isInterface()) {
			x.add(new IdInterfaceException(id, null, "id type must be an interface"));
		}

		Class<?>[] interfaces = id.getInterfaces();
		if (interfaces.length > 2
				|| ((interfaces.length == 1 && !(interfaces[0].equals(Serializable.class) || interfaces[0].equals(MapId.class))))) {
			x.add(new IdInterfaceException(id, null, "id type may only extend Serializable and/or MapId"));
		}

		for (Method m : id.getDeclaredMethods()) {
			Class<?>[] args = m.getParameterTypes();
			String name = m.getName();
			Class<?> ret = m.getReturnType();
			switch (args.length) {
				case 0: // then getter
					if (name.startsWith("get") && name.length() == 3) {
						x.add(new IdInterfaceException(id, m, "getter methods must have a property name following 'get' prefix"));
					}
					if (!Serializable.class.isAssignableFrom(ret)) {
						x.add(new IdInterfaceException(id, m, "getter methods must return Serializable types"));
					}
					break;
				case 1: // then setter
					if (name.startsWith("set") && name.length() == 3) {
						x.add(new IdInterfaceException(id, m, "setter methods must have a property name following 'set' prefix"));
					}
					if (name.startsWith("with") && name.length() == 4) {
						x.add(new IdInterfaceException(id, m, "setter methods must have a property name following 'with' prefix"));
					}
					if (!void.class.equals(ret) && !id.equals(ret)) {
						x.add(new IdInterfaceException(id, m,
								"setter methods not returning void may only return the same type as their id interface"));
					}
					Class<?> arg = args[0];
					if (!Serializable.class.isAssignableFrom(arg)) {
						x.add(new IdInterfaceException(id, m, "setter methods must take exactly one Serializable type"));
					}
					break;
				default:
					x.add(new IdInterfaceException(id, m,
							"id interface methods may only take zero parameters for a getter or one parameter for a setter; found "
									+ args.length));
			}
		}

		if (x.getCount() > 0) {
			throw x;
		}
	}
}
