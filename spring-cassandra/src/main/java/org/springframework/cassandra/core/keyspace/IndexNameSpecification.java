/*
 * Copyright 2011-2013 the original author or authors.
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
package org.springframework.cassandra.core.keyspace;

import org.springframework.cassandra.core.CqlIdentifier;

/**
 * Abstract builder class to support the construction of table specifications.
 * 
 * @author David Webb
 * @param <T> The subtype of the {@link IndexNameSpecification}
 */
public abstract class IndexNameSpecification<T extends IndexNameSpecification<T>> {

	/**
	 * The name of the index.
	 */
	private CqlIdentifier identifier;

	/**
	 * Sets the index name.
	 * 
	 * @return this
	 */
	@SuppressWarnings("unchecked")
	public T name(String name) {
		identifier = new CqlIdentifier(name);
		return (T) this;
	}

	public String getName() {
		return identifier.getName();
	}

	public String getNameAsIdentifier() {
		return identifier.toCql();
	}

}
