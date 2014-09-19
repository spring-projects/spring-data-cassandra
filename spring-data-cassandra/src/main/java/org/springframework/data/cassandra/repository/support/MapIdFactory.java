package org.springframework.data.cassandra.repository.support;

import java.io.Serializable;
import java.lang.reflect.Proxy;

import org.springframework.data.cassandra.repository.MapId;
import org.springframework.util.Assert;

/**
 * Factory class for producing implementations of given id interfaces. For restrictions on id interfaces definitions,
 * see {@link IdInterfaceValidator#validate(Class)}.
 * 
 * @see IdInterfaceValidator#validate(Class)
 * @author Matthew T. Adams
 */
@SuppressWarnings("unchecked")
public class MapIdFactory {

	/**
	 * Produces an implementation of the given id interface type using the type's class loader. For restrictions on id
	 * interfaces definitions, see {@link IdInterfaceValidator#validate(Class)}. Returns an implementation of the given
	 * interface that also implements {@link MapId} and {@link Serializable}, so it can be cast as such if necessary.
	 * 
	 * @param idInterface The type of the id interface.
	 * @return An implementation of the given interface that also implements {@link MapId} and {@link Serializable}.
	 * @see IdInterfaceValidator#validate(Class)
	 */
	public static <T> T id(Class<T> idInterface) {
		Assert.notNull(idInterface);
		return id(idInterface, idInterface.getClassLoader());
	}

	/**
	 * Produces an implementation of the given class loader. For restrictions on id interfaces definitions, see
	 * {@link IdInterfaceValidator#validate(Class)}. Returns an implementation of the given interface that also implements
	 * {@link MapId} and {@link Serializable}, so it can be cast as such if necessary.
	 * 
	 * @param idInterface The type of the id interface.
	 * @return An implementation of the given interface that also implements {@link MapId} and {@link Serializable}.
	 * @see IdInterfaceValidator#validate(Class)
	 */
	public static <T> T id(Class<T> idInterface, ClassLoader loader) {
		if (MapId.class.equals(idInterface)) {
			return (T) new BasicMapId();
		}

		IdInterfaceValidator.validate(idInterface);

		Class<?>[] interfaces = idInterface.getInterfaces();
		switch (interfaces.length) {
			case 0:
				interfaces = new Class<?>[] { idInterface, MapId.class, Serializable.class };
				break;
			case 1:
				Class<?> other = interfaces[0].equals(Serializable.class) ? MapId.class : Serializable.class;
				interfaces = new Class<?>[] { idInterface, interfaces[0], other };
				break;
			default:
				interfaces = new Class<?>[] { idInterface, interfaces[0], interfaces[1] };
		}
		return (T) Proxy.newProxyInstance(loader, interfaces, new MapIdProxyDelegate(idInterface));
	}
}
