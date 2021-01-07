/*
 * Copyright 2017-2021 the original author or authors.
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
package org.springframework.data.cassandra.core.mapping;

import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Factory class for producing implementations of given id interfaces. For restrictions on id interfaces definitions,
 * see {@link IdInterfaceValidator#validate(Class)}.
 *
 * @see IdInterfaceValidator#validate(Class)
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
@SuppressWarnings("unchecked")
public class MapIdFactory {

	/**
	 * Produces an implementation of the given id interface type using the type's class loader. For restrictions on id
	 * interfaces definitions, see {@link IdInterfaceValidator#validate(Class)}. Returns an implementation of the given
	 * interface that also implements {@link MapId}, so it can be cast as such if necessary.
	 *
	 * @param idInterface The type of the id interface.
	 * @return An implementation of the given interface that also implements {@link MapId}.
	 * @see IdInterfaceValidator#validate(Class)
	 */
	public static <T> T id(Class<T> idInterface) {

		Assert.notNull(idInterface, "Interface class must not be null");
		return id(idInterface, idInterface.getClassLoader());
	}

	/**
	 * Produces an implementation of the given class loader. For restrictions on id interfaces definitions, see
	 * {@link IdInterfaceValidator#validate(Class)}. Returns an implementation of the given interface that also implements
	 * {@link MapId}, so it can be cast as such if necessary.
	 *
	 * @param idInterface The type of the id interface.
	 * @return An implementation of the given interface that also implements {@link MapId}.
	 * @see IdInterfaceValidator#validate(Class)
	 */
	public static <T> T id(Class<T> idInterface, ClassLoader loader) {

		if (MapId.class.equals(idInterface)) {
			return (T) new BasicMapId();
		}

		IdInterfaceValidator.validate(idInterface);

		Class<?>[] idInterfaces = ClassUtils.getAllInterfacesForClass(idInterface);
		Set<Class<?>> proxyInterfaces = new HashSet<>(idInterfaces.length + 1, 1);

		proxyInterfaces.add(MapId.class);
		proxyInterfaces.addAll(Arrays.asList(idInterfaces));

		return (T) Proxy.newProxyInstance(loader, proxyInterfaces.toArray(new Class[proxyInterfaces.size()]),
				new MapIdProxyDelegate(idInterface));
	}
}
