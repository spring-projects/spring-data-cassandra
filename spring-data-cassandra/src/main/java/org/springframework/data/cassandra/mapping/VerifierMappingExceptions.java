/*
 * Copyright 2011-2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
 * 
 */
public class VerifierMappingExceptions extends MappingException {

	Collection<String> messages = new LinkedList<String>();
	Collection<MappingException> errors = new LinkedList<MappingException>();

	/**
	 * @param s
	 */
	public VerifierMappingExceptions(String s) {
		super(s);
	}

	/**
	 * @param s
	 */
	public void add(MappingException e) {
		messages.add(e.getMessage());
		errors.add(e);
	}

	/**
	 * @param s
	 */
	public void add(String s, MappingException e) {
		messages.add(s);
		errors.add(e);
	}

	/**
	 * Returns a list of the MappingExceptions aggregated within.
	 * 
	 * @return The Collection of MappingException
	 */
	public Collection<MappingException> getMappingExceptions() {
		return errors;
	}

	/**
	 * Returns a list of the MappingException messages aggregated within.
	 * 
	 * @return The Collection of Messages
	 */
	public Collection<String> getMappingExceptionMessages() {
		return messages;
	}

	/**
	 * Returns the number of errors that have been added to this Exception Class.
	 * 
	 * @return Number of Errors present
	 */
	public int getErrorCount() {
		return errors.size();
	}

	@Override
	public String getMessage() {
		StringBuilder builder = new StringBuilder();
		for (String s : messages) {
			builder.append(s).append("\n");
		}
		return builder.toString();
	}

}
