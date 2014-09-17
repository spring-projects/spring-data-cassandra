package org.springframework.data.cassandra.repository.support;

import java.lang.reflect.Proxy;

import org.springframework.data.cassandra.repository.MapId;
import org.springframework.util.Assert;

@SuppressWarnings("unchecked")
public class MapIdFactory {

	public static <T extends MapId> T id(Class<T> idInterface) {
		Assert.notNull(idInterface);
		return id(idInterface, idInterface.getClassLoader());
	}

	public static <T extends MapId> T id(Class<T> idInterface, ClassLoader loader) {
		return (T) Proxy.newProxyInstance(loader, new Class<?>[] { idInterface }, new MapIdProxyDelegate(idInterface));
	}
}
