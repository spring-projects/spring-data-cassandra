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

import java.lang.reflect.Method;

import org.springframework.data.mapping.MappingException;
import org.springframework.lang.Nullable;

/**
 * Exception thrown on incorrect mapping of an Id interface.
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
public class IdInterfaceException extends MappingException {

	private static final long serialVersionUID = -1635695314254522703L;

	private final String idInterfaceName;

	private final @Nullable String method;

	public IdInterfaceException(Class<?> idInterface, @Nullable Method method, String message) {
		this(idInterface.getClass().getName(), method == null ? null : method.toString(), message);
	}

	public IdInterfaceException(String idInterfaceName, @Nullable String method, String message) {
		super(message);
		this.idInterfaceName = idInterfaceName;
		this.method = method;
	}

	public String getIdInterfaceName() {
		return idInterfaceName;
	}

	@Nullable
	public String getMethod() {
		return method;
	}
}
