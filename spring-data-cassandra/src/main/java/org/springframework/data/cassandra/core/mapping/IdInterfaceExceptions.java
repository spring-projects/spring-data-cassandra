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

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.stream.Collectors;

import org.springframework.data.mapping.MappingException;
import org.springframework.util.Assert;

/**
 * Aggregator of multiple violations for convenience when verifying id interfaces. This allows the framework to
 * communicate all errors at once, rather than one at a time.
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
@SuppressWarnings("serial")
public class IdInterfaceExceptions extends MappingException {

	private final Collection<MappingException> exceptions;
	private final String className;

	/**
	 * Create a new {@link IdInterfaceExceptions} for the given {@code idInterfaceClass} and exceptions.
	 *
	 * @param idInterfaceClass must not be {@literal null}.
	 * @param exceptions must not be {@literal null}.
	 * @since 2.0
	 */
	public IdInterfaceExceptions(Class<?> idInterfaceClass, Collection<MappingException> exceptions) {

		super(String.format("Mapping Exceptions for %s", idInterfaceClass.getName()));

		Assert.notNull(idInterfaceClass, "CassandraPersistentEntity must not be null");

		this.exceptions = Collections.unmodifiableCollection(new LinkedList<>(exceptions));
		this.className = idInterfaceClass.getName();

		this.exceptions.forEach(this::addSuppressed);
	}

	public void add(IdInterfaceException e) {
		exceptions.add(e);
	}

	/**
	 * Returns a list of the {@link IdInterfaceException}s aggregated within.
	 */
	public Collection<MappingException> getExceptions() {
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
		StringBuilder builder = new StringBuilder(className).append(":\n");
		for (MappingException e : exceptions) {
			builder.append(e.getMessage()).append("\n");
		}
		return builder.toString();
	}

	public String getIdInterfaceName() {
		return className;
	}
}
