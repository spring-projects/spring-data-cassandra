/*
 * Copyright 2013-2014 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.cassandra.core.keyspace;

import static org.springframework.cassandra.core.cql.CqlIdentifier.cqlId;

import org.springframework.cassandra.core.cql.CqlIdentifier;
import org.springframework.util.Assert;

/**
 * Abstract builder class to support the construction of an index.
 * 
 * @param <T> The subtype of the {@link IndexNameSpecification}
 * @author David Webb
 * @author Matthew T. Adams
 */
public abstract class IndexNameSpecification<T extends IndexNameSpecification<T>> {

	/**
	 * The name of the index.
	 */
	private CqlIdentifier name;

	/**
	 * Sets the index name.
	 * 
	 * @return this
	 */
	public T name(String name) {
		return name(cqlId(name));
	}

	@SuppressWarnings("unchecked")
	public T name(CqlIdentifier name) {
		Assert.notNull(name);
		this.name = name;
		return (T) this;
	}

	public CqlIdentifier getName() {
		return name;
	}
}
