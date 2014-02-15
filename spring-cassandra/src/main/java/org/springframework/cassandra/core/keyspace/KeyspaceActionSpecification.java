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

import org.springframework.cassandra.core.CqlIdentifier;

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
	private CqlIdentifier identifier;

	/**
	 * Sets the keyspace name.
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

	/**
	 * For debugging KeyspaceActionSprcifications
	 */
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Keyspace Action Specification {name: " + identifier + ", class: " + this.getClass() + "}");
		return sb.toString();
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
		KeyspaceActionSpecification<?> thatSpec = (KeyspaceActionSpecification<?>) that;
		return this.identifier.equals(thatSpec.identifier) && this.getClass().equals(that.getClass());
	}

	@Override
	public int hashCode() {
		return this.identifier.hashCode() ^ this.getClass().hashCode();
	}

}
