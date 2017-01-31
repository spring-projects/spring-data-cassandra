/*
 * Copyright 2013-2017 the original author or authors.
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

import static org.springframework.cassandra.core.cql.KeyspaceIdentifier.*;

import org.springframework.cassandra.core.cql.KeyspaceIdentifier;
import org.springframework.util.Assert;

/**
 * Abstract builder class to support the construction of keyspace specifications.
 *
 * @author John McPeek
 * @author David Webb
 * @param <T> The subtype of the {@link KeyspaceActionSpecification}
 */
public abstract class KeyspaceActionSpecification<T extends KeyspaceActionSpecification<T>> {

	/**
	 * The name of the keyspace.
	 */
	private KeyspaceIdentifier name;

	/**
	 * Sets the keyspace name.
	 *
	 * @return this
	 */
	public T name(String name) {
		return name(ksId(name));
	}

	/**
	 * Sets the keyspace name.
	 *
	 * @return this
	 */
	@SuppressWarnings("unchecked")
	public T name(KeyspaceIdentifier name) {

		Assert.notNull(name, "KeyspaceIdentifier must not be null");

		this.name = name;
		return (T) this;
	}

	public KeyspaceIdentifier getName() {
		return name;
	}

	/**
	 * Determine the KeyspaceActionSpecifications are the same if they have the same "name" and same class.
	 *
	 * @param that The object to compare this to.
	 * @return Are this and that the same?
	 */
	@Override
	public boolean equals(Object that) {
		if (this == that) {
			return true;
		}
		if (that == null) {
			return false;
		}
		if (!(that instanceof KeyspaceActionSpecification)) {
			return false;
		}
		KeyspaceActionSpecification<?> other = (KeyspaceActionSpecification<?>) that;
		return this.name.equals(other.name) && this.getClass().equals(that.getClass());
	}

	@Override
	public int hashCode() {
		return name.hashCode() ^ getClass().hashCode();
	}
}
