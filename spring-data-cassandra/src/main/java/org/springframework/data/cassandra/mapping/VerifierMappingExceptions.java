/*
 * Copyright 2013-2016 the original author or authors
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
package org.springframework.data.cassandra.mapping;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;

import org.springframework.data.mapping.model.MappingException;
import org.springframework.util.Assert;

/**
 * Aggregator of multiple {@link MappingException} for convenience when verifying persistent entities. This allows the
 * framework to communicate all verification errors to the user of the framework, rather than one at a time.
 *
 * @author David Webb
 * @author Mark Paluch
 */
@SuppressWarnings("serial")
public class VerifierMappingExceptions extends MappingException {

	final Collection<MappingException> exceptions;

	private final String className;

	/**
	 * Creates a new {@link VerifierMappingExceptions} for the given {@code entity} and message.
	 *
	 * @param entity must not be {@literal null}.
	 * @param exceptions must not be {@literal null}.
	 * @since 1.5
	 */
	public VerifierMappingExceptions(CassandraPersistentEntity<?> entity, Collection<MappingException> exceptions) {

		super(String.format("Mapping Exceptions for %s", entity.getName()));

		Assert.notNull(entity, "CassandraPersistentEntity must not be null");

		this.exceptions = Collections.unmodifiableCollection(new LinkedList<MappingException>(exceptions));
		this.className = entity.getType().getName();
	}

	/**
	 * Creates a new {@link VerifierMappingExceptions} for the given {@code entity} and message.
	 *
	 * @param entity must not be {@literal null}.
	 * @param message
	 */
	public VerifierMappingExceptions(CassandraPersistentEntity<?> entity, String message) {

		super(message);

		Assert.notNull(entity, "CassandraPersistentEntity must not be null");

		this.exceptions = new LinkedList<MappingException>();
		this.className = entity.getType().getName();
	}

	/**
	 * @param mappingException must not be {@literal null}.
	 * @deprecated Exceptions should be immutable so this method is subject to be removed in future versions
	 */
	@Deprecated
	public void add(MappingException mappingException) {

		Assert.notNull(mappingException, "MappingException must not be null");

		exceptions.add(mappingException);
	}

	/**
	 * Returns a list of the MappingExceptions aggregated within.
	 *
	 * @return The Collection of MappingException
	 */
	public Collection<MappingException> getMappingExceptions() {
		return Collections.unmodifiableCollection(exceptions);
	}

	/**
	 * Returns a list of the MappingException messages aggregated within.
	 *
	 * @return The Collection of Messages
	 */
	public Collection<String> getMessages() {
		Collection<String> messages = new LinkedList<String>();

		for (MappingException e : exceptions) {
			messages.add(e.getMessage());
		}

		return messages;
	}

	/**
	 * Returns the number of errors that have been added to this Exception Class.
	 *
	 * @return Number of Errors present
	 */
	public int getCount() {
		return exceptions.size();
	}

	@Override
	public String getMessage() {
		StringBuilder builder = new StringBuilder(className).append(":\n");

		for (MappingException e : exceptions) {
			builder.append(" - ").append(e.getMessage()).append("\n");
		}

		return builder.toString();
	}
}
