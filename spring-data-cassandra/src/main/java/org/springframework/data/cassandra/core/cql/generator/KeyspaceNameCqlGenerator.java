/*
 * Copyright 2013-2025 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core.cql.generator;

import org.springframework.data.cassandra.core.cql.keyspace.KeyspaceActionSpecification;
import org.springframework.util.Assert;

/**
 * Base class for Keyspace CQL generators.
 *
 * @param <T> subtype of {@link KeyspaceActionSpecification}.
 * @author Mark Paluch
 */
public abstract class KeyspaceNameCqlGenerator<T extends KeyspaceActionSpecification> {

	private final KeyspaceActionSpecification specification;

	public KeyspaceNameCqlGenerator(KeyspaceActionSpecification specification) {

		Assert.notNull(specification, "KeyspaceActionSpecification must not be null");
		this.specification = specification;
	}

	@SuppressWarnings("unchecked")
	public T getSpecification() {
		return (T) specification;
	}

	/**
	 * Convenient synonymous method of {@link #getSpecification()}.
	 */
	protected T spec() {
		return getSpecification();
	}

	public String toCql() {
		return toCql(new StringBuilder()).toString();
	}

	public abstract StringBuilder toCql(StringBuilder cql);
}
