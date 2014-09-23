/*
 * Copyright 2013-2014 the original author or authors
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
import java.util.LinkedList;

import org.springframework.data.mapping.model.MappingException;

/**
 * Aggregator of multiple {@link MappingException} for convenience when verifying persistent entities. This allows the
 * framework to communicate all verification errors to the user of the framework, rather than one at a time.
 * 
 * @author David Webb
 */
@SuppressWarnings("serial")
public class VerifierMappingExceptions extends MappingException {

	Collection<MappingException> exceptions = new LinkedList<MappingException>();
	private String className;

	/**
	 * @param s
	 */
	public VerifierMappingExceptions(CassandraPersistentEntity<?> entity, String s) {
		super(s);
		this.className = entity.getType().getName();
	}

	/**
	 * @param s
	 */
	public void add(MappingException e) {
		exceptions.add(e);
	}

	/**
	 * Returns a list of the MappingExceptions aggregated within.
	 * 
	 * @return The Collection of MappingException
	 */
	public Collection<MappingException> getMappingExceptions() {
		return exceptions;
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
			builder.append(e.getMessage()).append("\n");
		}
		return builder.toString();
	}

}
