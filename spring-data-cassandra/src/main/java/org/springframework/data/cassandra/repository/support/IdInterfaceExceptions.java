/*
 * Copyright 2013-2017 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.repository.support;

import java.util.Collection;
import java.util.LinkedList;
import java.util.stream.Collectors;

/**
 * Aggregator of multiple violations for convenience when verifying id interfaces. This allows the framework to
 * communicate all errors at once, rather than one at a time.
 *
 * @author Matthew T. Adams
 */
@SuppressWarnings("serial")
public class IdInterfaceExceptions extends RuntimeException {

	Collection<IdInterfaceException> exceptions = new LinkedList<>();
	String idInterfaceName;

	public IdInterfaceExceptions(Class<?> idInterface) {
		this.idInterfaceName = idInterface.getClass().getName();
	}

	public void add(IdInterfaceException e) {
		exceptions.add(e);
	}

	/**
	 * Returns a list of the {@link IdInterfaceException}s aggregated within.
	 */
	public Collection<IdInterfaceException> getExceptions() {
		return exceptions;
	}

	/**
	 * Returns a list of the {@link IdInterfaceException} messages aggregated within.
	 */
	public Collection<String> getMessages() {
		return exceptions.stream().map(Throwable::getMessage).collect(Collectors.toList());
	}

	/**
	 * Returns the number of exceptions aggregated in this exception.
	 */
	public int getCount() {
		return exceptions.size();
	}

	@Override
	public String getMessage() {
		StringBuilder builder = new StringBuilder(idInterfaceName).append(":\n");
		for (IdInterfaceException e : exceptions) {
			builder.append(e.getMessage()).append("\n");
		}
		return builder.toString();
	}

	public String getIdInterfaceName() {
		return idInterfaceName;
	}
}
